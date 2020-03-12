package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"log/syslog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/go-redis/redis/v7"
)

// RedisConfig is a struct representing Redis server configuration.
type RedisConfig struct {
	Server   string `json:"server"`
	Port     uint16 `json:"port"`
	DB       int    `json:"db"`
	Password string `json:"password"`
}

// Addr emits a host:port value passed as Addr parameter to redis.Options.
func (rc *RedisConfig) Addr() string {
	return fmt.Sprintf("%s:%d", rc.Server, rc.Port)
}

// ToRedisOptions builds a *redis.Options struct for convenient interaction
// with redis client.
func (rc *RedisConfig) ToRedisOptions() *redis.Options {
	return &redis.Options{
		Addr:     rc.Addr(),
		Password: rc.Password,
		DB:       rc.DB,
	}
}

// NewRedisConfig reads a JSON configuration file and un-marshals contents of
// this file into a *RedisConfig struct.
func NewRedisConfig(name string) *RedisConfig {
	c := &RedisConfig{}
	var file *os.File
	var err error
	if file, err = os.Open(name); err != nil {
		return c
	}
	defer file.Close()
	d := json.NewDecoder(file)
	d.Decode(c)
	return c
}

type args struct {
	redisConfigFile string
	debug           bool
	level           string // not implemented yet
	priority        string
	tag             string
}

var cliArgs args

func setupCliFlags() {
	flag.BoolVar(&cliArgs.debug, "debug", false, "Enable debugging")
	flag.StringVar(&cliArgs.tag, "t", "demotag", "Tag with which to publish messages")
	flag.StringVar(&cliArgs.priority, "p", "daemon.notice", "Priority as 'facility.level' to use when message does not have one already")
	flag.StringVar(&cliArgs.redisConfigFile, "redis-cfgfile", "redis.json", "Configuration file location with Redis db info")
	flag.Parse()

	cliArgs.level = "notice" // FIXME: should derive from CLI args
}

func strToFacility(s string, defaultFacility syslog.Priority) syslog.Priority {
	var mapStrToFacility = map[string]syslog.Priority{
		"kern":   syslog.LOG_KERN,
		"kernel": syslog.LOG_KERN,
		"user":   syslog.LOG_USER,
		"daemon": syslog.LOG_DAEMON,
		"syslog": syslog.LOG_SYSLOG,
	}
	var facilityStr string
	if strings.Contains(s, ".") {
		tokens := strings.Split(strings.ToLower(s), ".")
		facilityStr = tokens[0]
		log.Printf("facilityStr is: %s | tokens is %v", facilityStr, tokens)
	} else {
		facilityStr = s
	}

	if facility, ok := mapStrToFacility[facilityStr]; ok {
		return facility
	}
	return defaultFacility
}

func strToLevel(s string, defaultLevel syslog.Priority) syslog.Priority {
	var mapStrToLevel = map[string]syslog.Priority{
		"fatal":   syslog.LOG_CRIT,
		"err":     syslog.LOG_ERR,
		"error":   syslog.LOG_ERR,
		"warn":    syslog.LOG_WARNING,
		"warning": syslog.LOG_WARNING,
		"notice":  syslog.LOG_NOTICE,
		"info":    syslog.LOG_INFO,
		"dbug":    syslog.LOG_DEBUG,
	}
	var levelStr string
	if strings.Contains(s, ".") {
		tokens := strings.Split(strings.ToLower(s), ".")
		levelStr = tokens[1]
	} else {
		levelStr = s
	}
	if level, ok := mapStrToLevel[levelStr]; ok {
		return level
	}
	return defaultLevel
}

func strToPriority(
	s string,
	defaultLevel, defaultFacility syslog.Priority,
) syslog.Priority {
	var facility, level syslog.Priority
	tokens := strings.Split(s, ".")

	switch len(tokens) {
	// We are only extracting level in this case
	case 1:
		return strToLevel(tokens[0], defaultLevel)
	// We are expecting facility and level in this case
	case 2:
		facility = strToFacility(tokens[0], defaultFacility)
		level = strToLevel(tokens[1], defaultLevel)
		// log.Printf("facility: %v | level: %v", facility, level)
		return facility | level
	default:
		return defaultFacility | defaultLevel
	}
}

func levelToStr(level syslog.Priority, defaultLevelStr string) string {
	var mapLevelToStr = map[syslog.Priority]string{
		syslog.LOG_CRIT:    "fatal",
		syslog.LOG_ERR:     "err",
		syslog.LOG_WARNING: "warning",
		syslog.LOG_NOTICE:  "notice",
		syslog.LOG_INFO:    "info",
		syslog.LOG_DEBUG:   "dbug",
	}
	if levelStr, ok := mapLevelToStr[level]; ok {
		return levelStr
	}
	return defaultLevelStr
}

type SyslogFunc func(string) error

func loggerfn(level syslog.Priority, w *syslog.Writer) SyslogFunc {
	var mapLevelToFunc = map[syslog.Priority]SyslogFunc{
		syslog.LOG_CRIT:    w.Crit,
		syslog.LOG_ERR:     w.Err,
		syslog.LOG_WARNING: w.Warning,
		syslog.LOG_NOTICE:  w.Notice,
		syslog.LOG_INFO:    w.Info,
		syslog.LOG_DEBUG:   w.Debug,
	}
	return mapLevelToFunc[level]
}

type stat uint16

const (
	Plain stat = iota
	Json
	Crit
	Err
	Warning
	Notice
	Info
	Debug
	Unknown
)

// stats tracks number of unique messages received. Levels are only extracted
// from JSON messages where object contains field called "level", and that
// field contains a name known to this utility.
func (p *Publish) stats() {
	const interval = 10
	// Counters is meant to be private, but it is "exported" in order to be
	// JSON-marshal(able). In reality, it is not visible outside of this method.
	type Counters struct {
		Plain   int64 `json:"plaintext"`
		Json    int64 `json:"json_encoded"`
		Crit    int64 `json:"critical"`
		Err     int64 `json:"error"`
		Warning int64 `json:"warning"`
		Notice  int64 `json:"notice"`
		Info    int64 `json:"info"`
		Debug   int64 `json:"debug"`
	}
	var counts [Unknown]int64
	for {
		select {
		case <-time.Tick(interval * time.Second):

			b, _ := json.Marshal(
				map[string]interface{}{
					"counts": Counters{
						Plain: counts[Plain],
						Json:  counts[Json],
					},
					"source": p.source,
				},
			)
			log.Printf("%s", string(b))
			// Reset the counts after proceessing and reporting stats.
			for i, _ := range counts {
				counts[i] = 0
			}
		case v := <-p.statsChan:
			counts[v]++
		}
	}
}

// suppressedToMap creates a map which contains the original message body,
// and if original was decoded JSON, then only contents of "msg" or "message"
// field, or entire message if original was not valid JSON, and includes number
// of times this message occurred over a given period in seconds.
func suppressedToMap(m *Message, td time.Duration) map[string]interface{} {
	var dup = make(map[string]interface{})
	if !json.Valid(m.data) {
		dup["message"] = string(m.data)
	} else {
		json.Unmarshal(m.data, &dup)
	}
	dup["level"] = "info"
	dup["period"] = td.String()
	dup["suppressed"] = m.count
	dup["time"] = time.Now().Format(time.RFC3339Nano)
	return dup
}

// reportSuppressed on a regular basis produces an informational entry
// for each message which was encountered more than once during previous
// interval.
func reportSuppressed(p *Publish, dupes *Messages) {
	const interval = 60
	var t = p.chans
	var ticker = time.NewTicker(interval * time.Second)
	var timestamp = time.Now()
	for {
		select {
		case <-ticker.C:
			delta := time.Now().Sub(timestamp)
			for v := range dupes.Iter() {
				if v.count > 0 {
					m := suppressedToMap(v, delta)
					m["source"] = p.source
					m["key"] = "SuppressedMessage"
					t.json <- m
				}
			}
			dupes.Reset()
			timestamp = time.Now() // update timestamp for next report
		}
	}
}

// dispatch is where data from stdin is read, digested, and dispatched to
// functions which send it to Redis or syslog.
func dispatch(p *Publish, dupes *Messages) {
	var t = p.chans
	var scnr = bufio.NewScanner(os.Stdin)
	var validJSON bool
	for scnr.Scan() {
		var content bytes.Buffer
		validJSON = json.Valid(scnr.Bytes())
		var m = make(map[string]interface{})
		// When message is a JSON object, we want to use just the contents of
		// "msg" or "message" field for the purposes of establishing uniqueness
		// of the message. Because it is a serialized object, which likely
		// contains continuously variable fields such as a timestamp, we want to
		// only focus on the actual log text and ignore any associated metadata,
		// otherwise we won't be able to detect duplicate messages.
		if validJSON {
			json.Unmarshal(scnr.Bytes(), &m)
			if v, ok := getValue("msg", m); ok {
				content.WriteString(v)
			} else if v, ok := getValue("message", m); ok {
				content.WriteString(v)
			} else {
				continue
			}
		} else {
			content.Write(scnr.Bytes())
		}

		if !dupes.Insert(content.Bytes()) {
			continue
		}

		// If this is valid JSON, publish it to a JSON messages channel, else
		// it goes to the plaintext channel.
		if !validJSON {
			t.syslogplaintext <- scnr.Bytes()
			m["key"] = "SyslogMessage"
			m["level"] = "notice"  // this should come from CLI args
			m["msg"] = scnr.Text() // may become "message" instead
			m["source"] = p.source
			m["time"] = time.Now().Format(time.RFC3339Nano)
			t.plaintext <- m
			if p.conf.debug {
				log.Println("(plaintext): %v", m)
			}
		} else {
			// When "source" key does not exist, we use the tag `-t` with which
			// this program was started.
			if _, ok := getValue("source", m); !ok {
				m["source"] = p.source
			}
			if _, ok := getValue("key", m); !ok {
				m["key"] = "SyslogMessage"
			}
			t.json <- m
			t.syslogjson <- m
			if p.conf.debug {
				log.Println("(json): %v", m)
			}
		}
	}
}

func getValue(k string, m map[string]interface{}) (string, bool) {
	if v, ok := (m[k]).(string); ok {
		return v, true
	}
	return "", false
}

type topics struct {
	json            chan map[string]interface{}
	plaintext       chan map[string]interface{}
	syslogjson      chan map[string]interface{}
	syslogplaintext chan []byte
}

// Publish implements message publishing part of the program. Methods on this
// struct do the required work in goroutines after having messages dispatched
// by the dispatch function, which itself sits in a loop and scanning data from
// stdin.
type Publish struct {
	conf      *args // arguments from command line
	chans     *topics
	statsChan chan stat
	source    string
}

func (p *Publish) Close() {
	close(p.chans.json)
	close(p.chans.plaintext)
	close(p.chans.syslogjson)
	close(p.chans.syslogplaintext)
	close(p.statsChan)
}

// NewPublish builds a ready-to-go Publish struct, mostly just sets-up
// channels and such.
func NewPublish(conf *args) *Publish {
	return &Publish{
		conf: conf,
		chans: &topics{
			json:            make(chan map[string]interface{}),
			plaintext:       make(chan map[string]interface{}),
			syslogjson:      make(chan map[string]interface{}),
			syslogplaintext: make(chan []byte),
		},
		statsChan: make(chan stat),
		source:    conf.tag,
	}
}

func (p *Publish) publishToSyslog(
	w io.Writer,
) {
	for {
		if _, err := fmt.Fprintf(w, "%s", <-p.chans.syslogplaintext); err != nil {
			panic(err)
		}
		p.statsChan <- Plain
	}
}

func (p *Publish) publishToSyslogJSON(w *syslog.Writer) {
	mapLevelToStat := map[string]stat{
		"crit":    Crit,
		"err":     Err,
		"error":   Err,
		"warn":    Warning,
		"warning": Warning,
		"notice":  Notice,
		"info":    Info,
		"debug":   Debug,
	}
	for {
		obj := <-p.chans.syslogjson
		var levelStr string
		var ok bool
		if levelStr, ok = getValue("level", obj); !ok {
			// levelStr = levelToStr(defaultLevel, "")
			levelStr = p.conf.level
			obj["level"] = levelStr
		}
		_ = mapLevelToStat
		p.statsChan <- mapLevelToStat[levelStr]
		p.statsChan <- Json // This is a JSON-serialized message
		// Expect that message key could be either "msg" or "message".
		// If neither is found, or not a string value, we abandon processing
		// this particular object.
		// var msg string
		for _, k := range [...]string{"msg", "message"} {
			if v, ok := getValue(k, obj); ok {
				level := strToLevel(levelStr, syslog.LOG_NOTICE)
				fn := loggerfn(level, w)
				if err := fn(v); err != nil {
					panic(err)
				}
				break
			}
		}
	}
}

func (p *Publish) publishToTopic(rdb *redis.Client) {
	pubsub1 := rdb.Subscribe("json_msgs")
	if _, err := pubsub1.Receive(); err != nil {
		panic(err)
	}
	pubsub2 := rdb.Subscribe("plain_msgs")
	if _, err := pubsub2.Receive(); err != nil {
		panic(err)
	}
	for {
		select {
		case msg := <-p.chans.json:
			msgEncoded, _ := json.Marshal(msg)
			rdb.Publish("json_msgs", msgEncoded)
		case msg := <-p.chans.plaintext:
			msgEncoded, _ := json.Marshal(msg)
			rdb.Publish("plain_msgs", msgEncoded)
		}
	}
}

func signalHandler(sig chan os.Signal, done chan bool) {
	v := <-sig
	log.Printf("Shutting down on %v signal", v)
	done <- true
}

func main() {
	setupCliFlags()
	sigChan := make(chan os.Signal, 1)
	doneChan := make(chan bool, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go signalHandler(sigChan, doneChan)

	redisConfig := NewRedisConfig(cliArgs.redisConfigFile).ToRedisOptions()
	rdb := redis.NewClient(redisConfig)

	sysLog, err := syslog.Dial(
		"unixgram",
		"/dev/log",
		strToPriority(
			cliArgs.priority,
			syslog.LOG_NOTICE,
			syslog.LOG_DAEMON,
		), cliArgs.tag)
	if err != nil {
		log.Fatal(err)
	}

	// Setup pipelines
	p := NewPublish(&cliArgs)

	// Initialize duplicate messages structure
	dupes := NewMap()

	// Start statistics processing goroutine
	go p.stats()
	// Start syslog writing goroutine for JSON messages
	go p.publishToSyslogJSON(sysLog)
	// Start syslog writing goroutine for plaintext messages
	go p.publishToSyslog(sysLog)
	// Start Redis publishing goroutine
	go p.publishToTopic(rdb)
	// Start dispatch goroutine (it feeds the publishing goroutines)
	go dispatch(p, dupes)
	// Start duplicate message processing goroutine
	go reportSuppressed(p, dupes)

	<-doneChan
	p.Close()
}
