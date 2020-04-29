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
	"regexp"
	"strings"
	"syscall"
	"time"

	"github.com/go-redis/redis/v7"
)

const (
	// ChanTimeout is the amount of time we allow to wait on channel writes
	// before dropping the message.
	ChanTimeout = 100 * time.Millisecond
	// ChanBufferLen is the number of messages which we can queue up in the
	// channels before the channel (queue) is full. The larger the buffer the
	// longer messages are retained in the situation where destination is not
	// reachable.
	ChanBufferLen = 10
)

var patterns = struct {
	debug *regexp.Regexp
	err   *regexp.Regexp
	info  *regexp.Regexp
	warn  *regexp.Regexp
}{
	debug: regexp.MustCompile("(?i:debug)"),
	err:   regexp.MustCompile("(?i:error|fail|fault|crit|panic)"),
	info:  regexp.MustCompile("(?i:info)"),
	warn:  regexp.MustCompile("(?i:warn|caution)"),
}

func detectLevel(b []byte) syslog.Priority {
	switch {
	case patterns.err.Match(b):
		return syslog.LOG_ERR
	case patterns.warn.Match(b):
		return syslog.LOG_WARNING
	case patterns.info.Match(b):
		return syslog.LOG_INFO
	case patterns.debug.Match(b):
		return syslog.LOG_DEBUG
	default:
		return syslog.LOG_NOTICE
	}
}

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
// with Redis client.
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
	parserPattern   string
	priority        string
	tag             string
}

var cliArgs args

func setupCliFlags() {
	flag.BoolVar(&cliArgs.debug, "debug", false, "Enable debugging")
	flag.StringVar(&cliArgs.tag, "t", "demotag", "Tag with which to publish messages")
	flag.StringVar(&cliArgs.parserPattern, "pattern", "", "Pattern containing minimally a <msg> capture group")
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
		"debug":   syslog.LOG_DEBUG,
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
	tokens := strings.Split(strings.ToLower(s), ".")

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
		syslog.LOG_ERR:     "error",
		syslog.LOG_WARNING: "warning",
		syslog.LOG_NOTICE:  "notice",
		syslog.LOG_INFO:    "info",
		syslog.LOG_DEBUG:   "debug",
	}
	if levelStr, ok := mapLevelToStr[level]; ok {
		return levelStr
	}
	return defaultLevelStr
}

// SyslogFunc is a function prototype for a function returned from loggerfn.
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
	Duplicate
	Crit
	Err
	Warning
	Notice
	Info
	Debug
	Unknown
)

func levelToStat(levelStr string) stat {
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
	if v, ok := mapLevelToStat[levelStr]; !ok {
		return Unknown
	} else {
		return v
	}
}

func computeRatio(n, d int64) float64 {
	if n == 0 || d == 0 {
		return 0.
	}
	return float64(n) / float64(d)
}

// stats tracks number of unique messages received. Levels are only extracted
// from JSON messages where object contains field called "level", and that
// field contains a name known to this utility.
func (p *Publish) stats() {
	const interval = 10
	// Counters is meant to be private, but it is "exported" in order to be
	// JSON-marshal(able). In reality, it is not visible outside of this method.
	type Counters struct {
		Plain     int64 `json:"plaintext"`
		Json      int64 `json:"json_encoded"`
		Duplicate int64 `json:"duplicate"`
		Crit      int64 `json:"critical"`
		Err       int64 `json:"error"`
		Warning   int64 `json:"warning"`
		Notice    int64 `json:"notice"`
		Info      int64 `json:"info"`
		Debug     int64 `json:"debug"`
	}
	var counts [Unknown]int64
	var t = time.NewTicker(interval * time.Second).C
	for {
		select {
		case <-t:
			b, _ := json.Marshal(
				map[string]interface{}{
					"counts": Counters{
						Plain:     counts[Plain],
						Json:      counts[Json],
						Duplicate: counts[Duplicate],
						Crit:      counts[Crit],
						Err:       counts[Err],
						Warning:   counts[Warning],
						Notice:    counts[Notice],
						Info:      counts[Info],
						Debug:     counts[Debug],
					},
					"source": p.source,
				},
			)
			if p.conf.debug {
				log.Printf("%s", string(b))
			}
			// Reset the counts after proceessing and reporting stats.
			for i, _ := range counts {
				counts[i] = 0
			}
		case v := <-p.statsChan:
			if v < Unknown {
				counts[v]++
			}
		case <-p.doneChan:
			if p.conf.debug {
				log.Println("Shutting down stats")
			}
			p.ackDoneChan <- struct{}{}
			return
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
	// Regardless of message content, we consider these to be informational.
	dup["level"] = "info"
	dup["period"] = td.String()
	dup["suppressed"] = m.count
	if (time.Duration(m.count) * time.Second / td) > 1 {
		dup["alert"] = true
	} else {
		dup["alert"] = false
	}
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
					m["suppressed_per_sec"] = float64(m["suppressed"].(uint64)) / delta.Seconds()
					m["source"] = p.source
					m["key"] = "SuppressedMessage"
					select {
					case t.json <- m:
					case <-time.After(ChanTimeout):
						log.Println("dropped suppressed report")
					}
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
	var content bytes.Buffer
	for scnr.Scan() {
		content.Reset() // Re-using the same buffer instead of re-allocating
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
			p.statsChan <- Duplicate
			continue
		}

		// If this is valid JSON, publish it to a JSON messages channel, else
		// it goes to the plaintext channel.
		if !validJSON { // Invalid JSON extracted from stdin
			select {
			// Write message as-is without any transformations to syslog.
			case t.syslogplaintext <- scnr.Bytes():
			case <-time.After(ChanTimeout):
				// dropping this message instead of blocking here
				if p.conf.debug {
					log.Printf("dropped plaintext->syslog message after %v timeout", ChanTimeout)
				}
			}
			m = p.transform.MsgToMap(scnr)
			select {
			case t.json <- m:
			case <-time.After(ChanTimeout):
				// dropping this message instead of blocking here
				if p.conf.debug {
					log.Printf("dropped plaintext->redis message after %v timeout", ChanTimeout)
				}
			}

			if p.conf.debug {
				log.Printf("(plaintext): %v", m)
			}
		} else { // Valid JSON extracted from stdin
			// When "source" key does not exist, we use the tag `-t` with which
			// this program was started.
			if _, ok := getValue("source", m); !ok {
				m["source"] = p.source
			}
			// Fallback to generic key, which we have been using historically
			// in this context.
			if _, ok := getValue("key", m); !ok {
				m["key"] = "SyslogMessage"
			}
			// We attempt to derive level from the data if we don't already
			// have it in this map.
			var level syslog.Priority
			if levelStr, ok := getValue("level", m); !ok {
				level = detectLevel(scnr.Bytes())
				m["level"] = levelToStr(level, p.conf.level)
			} else {
				level = strToPriority(
					levelStr, syslog.LOG_NOTICE, 0)
			}

			// If there is no ccs field, we add it, and set it to true if level
			// is above syslog.LOG_DEBUG. Historically our model has been to
			// send messages to CCS as long as they were not debug messages or
			// explicitly identified as being local-only.
			if _, ok := getValue("ccs", m); !ok {
				if level < syslog.LOG_DEBUG {
					m["ccs"] = true
				} else {
					m["ccs"] = false
				}
			}
			// If the map already has a time field, i.e. original JSON object
			// had a time field, we will add a new field called source_time,
			// and add a time field with value of time.Now().
			if v, ok := getValue("time", m); ok {
				m["source_time"] = v
			}
			m["time"] = time.Now().Format(time.RFC3339Nano)
			select {
			case t.json <- m:
				log.Printf("(1-1) %v", m)
			case <-time.After(ChanTimeout):
				if p.conf.debug {
					log.Println("dropped JSON->redis message after timeout")
				}
			}
			select {
			case t.syslogjson <- m:
				log.Printf("(1-2) %v", m)
			case <-time.After(ChanTimeout):
				if p.conf.debug {
					log.Println("dropped JSON->redis message after timeout")
				}
			}
			if p.conf.debug {
				log.Printf("(json): %v", m)
			}
		}
		// Bump stats, tracking count of messages by level.
		select {
		case p.statsChan <- levelToStat(m["level"].(string)):
		case <-time.After(ChanTimeout):
			if p.conf.debug {
				log.Println("dropped stat after timeout")
			}
		}
	}
}

type Transformer interface {
	MsgToMap(scnr *bufio.Scanner) map[string]interface{}
}

type Parser struct {
	conf         *args
	parserRegexp *regexp.Regexp
}

func (p *Parser) mustParse() bool {
	return p.parserRegexp != nil
}

func (p *Parser) MsgToMap(scnr *bufio.Scanner) map[string]interface{} {
	m := make(map[string]interface{})
	if p.mustParse() {
		results, ok := p.parse(scnr.Bytes())
		if ok {
			for k, v := range results {
				m[k] = v
			}
		}
	}
	// If after parsing the message we still do not have a `message` field, we
	// use the entire line as message instead. This is a fallback really.
	if _, ok := m["message"]; !ok {
		m["message"] = scnr.Text() // may become "message" instead
	}
	if _, ok := m["key"]; !ok {
		m["key"] = "SyslogMessage"
	}
	var level syslog.Priority
	if _, ok := m["level"]; !ok {
		level = detectLevel(scnr.Bytes())
		m["level"] = levelToStr(level, p.conf.level)
	}
	if level < syslog.LOG_DEBUG {
		m["ccs"] = true
	} else {
		m["ccs"] = false
	}
	m["source"] = p.conf.tag
	m["time"] = time.Now().Format(time.RFC3339Nano)
	m["plaintext"] = true
	return m
}

func (p *Parser) parse(b []byte) (map[string]string, bool) {
	results := p.parserRegexp.FindAllSubmatch(b, -1)
	// if p.conf.debug {
	// 	for i, level1 := range results[0] {
	// 		log.Printf("level1: (%d): %s\t=> %s",
	// 		i, p.parserRegexp.SubexpNames()[i], level1)
	// 	}
	// }
	if len(results) == 0 {
		return nil, false
	}
	m := make(map[string]string, len(results[0])-1)
	for i, value := range results[0] {
		if i == 0 { // 0th index contains full string
			continue
		}
		key := p.parserRegexp.SubexpNames()[i]
		m[key] = string(value)
	}
	return m, true
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
	conf        *args // arguments from command line
	chans       *topics
	ackDoneChan chan struct{}
	doneChan    chan struct{}
	statsChan   chan stat
	source      string
	transform   Transformer
}

// Close shuts down publishers, and should be called before terminating
// the program.
func (p *Publish) Close() {
	const numOfWorkers = 4
	close(p.doneChan)
	// This blocks until all publishers acknowledge and corresponding
	// goroutines return. We do not close any of the *topics channels to avoid
	// a write on closed channel in the dispatch(...) goroutine.
	for i := 0; i < numOfWorkers; i++ {
		<-p.ackDoneChan
	}
}

// NewPublish builds a ready-to-go Publish struct, mostly just sets-up
// channels and such.
func NewPublish(conf *args) *Publish {
	var compiled *regexp.Regexp
	if conf.parserPattern != "" {
		compiled = regexp.MustCompile(conf.parserPattern)
	}
	return &Publish{
		conf: conf,
		chans: &topics{
			json:            make(chan map[string]interface{}, ChanBufferLen),
			syslogjson:      make(chan map[string]interface{}, ChanBufferLen),
			plaintext:       make(chan map[string]interface{}, ChanBufferLen),
			syslogplaintext: make(chan []byte, ChanBufferLen),
		},
		ackDoneChan: make(chan struct{}),
		doneChan:    make(chan struct{}),
		statsChan:   make(chan stat),
		source:      conf.tag,
		transform: &Parser{
			conf:         conf,
			parserRegexp: compiled,
		},
	}
}

func (p *Publish) publishToSyslog(
	w io.Writer,
) {
	for {
		select {
		case msg := <-p.chans.syslogplaintext:
			if _, err := fmt.Fprintf(w, "%s", msg); err != nil {
				//panic(err)
				log.Printf("syslog failed: %v", err)
			}
			p.statsChan <- Plain
		case <-p.doneChan:
			if p.conf.debug {
				log.Println("Shutting down publishToSyslog")
			}
			p.ackDoneChan <- struct{}{}
			return
		}
	}
}

func (p *Publish) publishToSyslogJSON(w *syslog.Writer) {
	for {
		select {
		case obj := <-p.chans.syslogjson:
			var levelStr string
			var ok bool
			if levelStr, ok = getValue("level", obj); !ok {
				// levelStr = levelToStr(defaultLevel, "")
				levelStr = p.conf.level
				obj["level"] = levelStr
			}
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
		case <-p.doneChan:
			if p.conf.debug {
				log.Println("Shutting down publishToSyslogJSON")
			}
			p.ackDoneChan <- struct{}{}
			return
		}
	}
}

type PubSubInterface interface {
	Publish(channel string, message interface{}) *redis.IntCmd
	Subscribe(channels ...string) *redis.PubSub
}

func (p *Publish) publishToDatabase(ps PubSubInterface) {
	psValidate := ps.Subscribe("json_msgs")
	var connected bool
	var delay time.Duration = 1 * time.Second
	// Retry connecting to Redis indefinitely. Upon connection the library
	// will take over and help to reconnect and re-subscribe as necessary.
	for !connected {
		select {
		case <-p.doneChan:
			if p.conf.debug {
				log.Println("Shutting down publishToDatabase")
			}
			p.ackDoneChan <- struct{}{}
			return
		default:
			if _, err := psValidate.Receive(); err != nil {
				log.Println(err)
				time.Sleep(delay)
				if delay < 30 * time.Second {
					delay += (delay/2 + 1)
				}
				if p.conf.debug {
					log.Printf("Delaying reconnect by %v", delay)
				}
			} else {
				connected = true
			}
		}
	}

	for {
		select {
		case msg := <-p.chans.json:
			msgEncoded, _ := json.Marshal(msg)
			ps.Publish("json_msgs", msgEncoded)
		case <-p.doneChan:
			if p.conf.debug {
				log.Println("Shutting down publishToDatabase")
			}
			p.ackDoneChan <- struct{}{}
			return
		}
	}
}

func signalHandler(sig chan os.Signal, done chan struct{}) {
	v := <-sig
	log.Printf("Shutting down on %v signal", v)
	close(done)
}

func main() {
	setupCliFlags()
	sigChan := make(chan os.Signal, 1)
	doneChan := make(chan struct{})
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
	go p.publishToDatabase(rdb)
	// Start dispatch goroutine (it feeds the publishing goroutines)
	go dispatch(p, dupes)
	// Start duplicate message processing goroutine
	go reportSuppressed(p, dupes)

	<-doneChan
	p.Close()
}
