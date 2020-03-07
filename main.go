package main

import (
	"time"
	"bufio"
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
	// "regexp"

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

type topics struct {
	json            chan []byte
	plaintext       chan []byte
	syslogjson      chan map[string]interface{}
	syslogplaintext chan string
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

type stat int16
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

func stats(stats chan stat, c chan string) {
	const interval = 1000
	type Counters struct {
		Plain int64 `json:"plaintext"`
		Json int64 `json:"json_encoded"`
		Crit int64 `json:"critical"`
		Err int64 `json:"error"`
		Warning int64 `json:"warning"`
		Notice int64 `json:"notice"`
		Info int64 `json:"info"`
		Debug int64 `json:"debug"`
	}
	var counts [Unknown]int64
	for {
		select {
		case <-time.Tick(interval*time.Second):

			b, _:= json.Marshal(
				Counters{
					Plain: counts[Plain],
					Json: counts[Json],
				},
			)
			log.Printf("%s", string(b))
			for i, _ := range counts {
				counts[i] = 0
			}
		case v := <-stats:
			counts[v]++
		}
	}
}

func dupesNotification(m *Message) map[string]interface{} {
	if !json.Valid(m.data) {
		dup := make(map[string]interface{})
		dup["repeated"] = m.count
		dup["message"] = string(m.data)
		return dup
	}
	dup := make(map[string]interface{})
	json.Unmarshal(m.data, &dup)
	dup["repeated"] = m.count
	return dup
}

func dispatch(t *topics) {
	dupesMap := NewMap()
	scnr := bufio.NewScanner(os.Stdin)
	ticker := time.NewTicker(time.Second * 5)
	for {
		select {
		case <- ticker.C:
			for v := range dupesMap.Iter() {
				log.Printf(">> %v", dupesNotification(v))
				// j, _ := json.Marshal(dupesNotification(v))
				// t.json <- j
			}
		default:
			scnr.Scan()
			if ! dupesMap.Insert(scnr.Bytes()) {
				continue
			}
			txt := scnr.Text()
			// If this is valid JSON, publish it to a JSON messages channel, else
			// it goes to the plaintext channel.
			if !json.Valid(scnr.Bytes()) {
				log.Println("-> plaintext")
				t.syslogplaintext <- txt
				t.plaintext <- scnr.Bytes()
			} else {
				t.json <- scnr.Bytes()
				var m = make(map[string]interface{})
				json.Unmarshal(scnr.Bytes(), &m)
				t.syslogjson <- m
				log.Println("-> json")
			}
		}
	}
}

func publishToSyslog(c <-chan string, w io.Writer, stats chan<- stat) {
	for {
		if _, err := fmt.Fprintf(w, <-c); err != nil {
			panic(err)
		}
		stats <- Plain
	}
}

func getValue(k string, m map[string]interface{}) (string, bool) {
	if v, ok := (m[k]).(string); ok {
		return v, true
	}
	return "", false
}

func publishToSyslogJSON(
	c <-chan map[string]interface{},
	w *syslog.Writer,
	defaultLevel syslog.Priority,
	stats chan<- stat,
) {
	mapLevelToStat := map[string]stat{
		"crit": Crit,
		"err": Err,
		"error": Err,
		"warn": Warning,
		"warning": Warning,
		"notice": Notice,
		"info": Info,
		"debug": Debug,
	}
	for {
		obj := <-c
		var levelStr string
		var ok bool
		if levelStr, ok = getValue("level", obj); !ok {
			levelStr = levelToStr(defaultLevel, "")
			obj["level"] = levelStr
		}
		stats <- mapLevelToStat[levelStr]
		stats <- Json // This is a JSON-serialized message
		// else {
		// 	obj["level"] = levelStr
		// }
		// Expect that message key could be either "msg" or "message".
		// If neither is found, or not a string value, we abandon processing
		// this particular object.
		// var msg string
		for _, k := range [...]string{"msg", "message"} {
			if v, ok := getValue(k, obj); ok {
				level := strToLevel(levelStr, defaultLevel)
				fn := loggerfn(level, w)
				if err := fn(v); err != nil {
					panic(err)
				}
				break
			}
		}
	}
}

func publishToTopic(c chan []byte, topic string, rdb *redis.Client) {
	pubsub := rdb.Subscribe(topic)
	if _, err := pubsub.Receive(); err != nil {
		panic(err)
	}
	for {
		msg := <-c
		rdb.Publish(topic, msg)
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

	chans := &topics{
		json:            make(chan []byte),
		plaintext:       make(chan []byte),
		syslogjson:      make(chan map[string]interface{}),
		syslogplaintext: make(chan string),
	}

	statsChannel := make(chan stat)
	// go stats(statsChannel, chans.json)
	go publishToSyslogJSON(chans.syslogjson, sysLog, strToLevel(cliArgs.priority, syslog.LOG_NOTICE), statsChannel)
	// Publish plaintext messages to system log
	go publishToSyslog(chans.syslogplaintext, sysLog, statsChannel)
	// Publish json messages to json channel
	go publishToTopic(chans.json, "json_msgs", rdb)
	// Publish plaintext messages to plaintext channel
	go publishToTopic(chans.plaintext, "plain_msgs", rdb)
	go dispatch(chans)

	<-doneChan
	close(chans.json)
	close(chans.plaintext)
}
