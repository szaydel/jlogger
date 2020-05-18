package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
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

	// DefaultParserPattern should match any key=value sub-strings if there are
	// any in the tail of the message.
	// In other words if message is:
	// "read failed function=alpha key=NotReadable", then the matched pairs
	// will be function=alpha and key=NotReadable.
	DefaultParserPattern = `(?m)(?P<message>^.*)\s\s|(?P<labels>\w+=\w+)`

	DefaultKey = "SyslogMessage"

	// DefaultExpireDupesAfter is the amount of time after which a previously
	// seen message will not be considered a duplicate. For example, if the
	// if this value is N, and two identical messages are logged N + 1 units
	// of time apart, they will not be considered duplicates.
	DefaultExpireDupesAfter = 10 * time.Second
)

var errTimedOut = errors.New("Write failed with a timeout")
var errMissingMsgKey = errors.New("Mapping missing required message key")

var levelsRegexp = struct {
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

func detectLevel(scnr *bufio.Scanner) syslog.Priority {
	switch {
	case levelsRegexp.err.Match(scnr.Bytes()):
		return syslog.LOG_ERR
	case levelsRegexp.warn.Match(scnr.Bytes()):
		return syslog.LOG_WARNING
	case levelsRegexp.info.Match(scnr.Bytes()):
		return syslog.LOG_INFO
	case levelsRegexp.debug.Match(scnr.Bytes()):
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
	redisConfigFile   string
	debug             bool
	ignoreMissingMsg  bool
	chanBufLen        int
	chanTimeoutRedis  time.Duration
	chanTimeoutSyslog time.Duration
	expireDupesAfter  time.Duration
	key               string
	level             string // not implemented yet
	parserPattern     string
	priority          string
	tag               string
}

func (a *args) SyslogLevel() syslog.Priority {
	return strToLevel(a.level, syslog.LOG_NOTICE)
}

func (a *args) SyslogLevelString() string {
	return a.level
}

var cliArgs args

func setupCliFlags() {
	flag.IntVar(&cliArgs.chanBufLen, "channel.buffer.length", ChanBufferLen, "How many messages to allow in the buffer before discards may happen")
	flag.BoolVar(&cliArgs.debug, "debug", false, "Enable debugging")
	flag.BoolVar(&cliArgs.ignoreMissingMsg, "ignore.missing.msg", false, "Do not look for a message key after parsing lines")
	flag.DurationVar(&cliArgs.chanTimeoutRedis, "redis.timeout.ms", ChanTimeout, "Set timeout value for sending messages to Redis")
	flag.DurationVar(&cliArgs.chanTimeoutSyslog, "syslog.timeout.ms", ChanTimeout, "Set timeout value for sending messages to Syslog")
	flag.DurationVar(&cliArgs.expireDupesAfter, "expire.dupes.after.s", DefaultExpireDupesAfter, "Amount of time after which previously seen message is not a duplicate")
	flag.StringVar(&cliArgs.key, "key", DefaultKey, "Key with which to publish messages")
	flag.StringVar(&cliArgs.tag, "t", "demotag", "Tag with which to publish messages")
	flag.StringVar(&cliArgs.parserPattern, "pattern", DefaultParserPattern, "Pattern containing minimally a <msg> capture group")
	flag.StringVar(&cliArgs.priority, "p", "daemon.notice", "Priority as 'facility.level' to use when message does not have one already")
	flag.StringVar(&cliArgs.redisConfigFile, "redis.config.file", "redis.json", "Configuration file location with Redis db info")
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
		// log.Printf("facilityStr is: %s | tokens is %v", facilityStr, tokens)
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
	var tokens []string

	tokens = strings.Split(strings.ToLower(s), ".")
	switch len(tokens) {
	// We are only extracting level in this case
	case 1:
		levelStr = tokens[0]
	// We are expecting facility and level in this case
	case 2:
		levelStr = tokens[1]
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
	var tokens []string

	tokens = strings.Split(strings.ToLower(s), ".")
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

type stat uint16

const (
	Line = iota
	Plain
	Json
	JsonNoMsg
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
	const reportInterval = 10
	// Counters is meant to be private, but it is "exported" in order to be
	// JSON-marshal(able). In reality, it is not visible outside of this method.
	type Counters struct {
		Line      int64 `json:"line"`
		Plain     int64 `json:"plaintext"`
		Json      int64 `json:"json_encoded"`
		JsonNoMsg int64 `json:"json_encoded_missing_message"`
		Duplicate int64 `json:"duplicate"`
		Crit      int64 `json:"critical"`
		Err       int64 `json:"error"`
		Warning   int64 `json:"warning"`
		Notice    int64 `json:"notice"`
		Info      int64 `json:"info"`
		Debug     int64 `json:"debug"`
	}
	var counts [Unknown]int64
	var t = time.NewTicker(reportInterval * time.Second).C
	for {
		select {
		case <-t:
			b, _ := json.Marshal(
				map[string]interface{}{
					"counts": Counters{
						Line:      counts[Line],
						Plain:     counts[Plain],
						JsonNoMsg: counts[JsonNoMsg],
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
				log.Printf("STATS: %s", string(b))
			}
			// Reset the counts after proceessing and reporting stats. We may
			// want to re-enable this, but right now I want to keep these as
			// counters instead of gauges.
			// for i, _ := range counts {
			// 	counts[i] = 0
			// }
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
	dup["ts"] = time.Now().Format(time.RFC3339Nano)
	return dup
}

// reportSuppressed on a regular basis produces an informational entry
// for each message which was encountered more than once during previous
// interval.
func reportSuppressed(p *Publish, dupes *Messages) {
	const reportInterval = 10
	var t = p.chans
	var ticker = time.NewTicker(reportInterval * time.Second)
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
			dupes.Expire(p.conf.expireDupesAfter)
			timestamp = time.Now() // update timestamp for next report
		}
	}
}

// PutWithTimeout sends the message on the supplied channel and times out after
// interval `t` if the channel in argument `c` is blocked.
func PutWithTimeout(
	c chan map[string]interface{},
	m map[string]interface{},
	t time.Duration) error {
	select {
	case c <- m:
		return nil
	case <-time.After(t):
		return errTimedOut
	}
}

// dispatch is where data from stdin is read, digested, and dispatched to
// functions which send it to Redis or syslog.
func dispatch(p *Publish, dupes *Messages) {
	var t = p.chans
	var scnr = bufio.NewScanner(os.Stdin)
	var validJSON bool
	var msgBody []byte
	for scnr.Scan() {
		p.statsChan <- Line // Keep a count of all lines received

		var m = make(map[string]interface{})
		nbytes := len(scnr.Bytes())
		// When there is effectively just a `\n`, we just want to read the
		// following line.
		if nbytes == 0 {
			continue
		}
		// Do a simple check to see if there is any chance that this buffer
		// actually contains valid JSON.
		if scnr.Bytes()[0] == '{' &&
			scnr.Bytes()[nbytes-1] == '}' {
			if err := json.Unmarshal(scnr.Bytes(), &m); err == nil {
				validJSON = true
			}
		} else {
			validJSON = false
		}
		// When message is a JSON object, we want to use just the contents of
		// "msg" or "message" field for the purposes of establishing uniqueness
		// of the message. Because it is a serialized object, which likely
		// contains continuously variable fields such as a timestamp, we want to
		// only focus on the actual log text and ignore any associated metadata,
		// otherwise we won't be able to detect duplicate messages.
		if validJSON {
			p.statsChan <- Json
			var ok bool
			if msgBody, ok = getMessageBytes(m); !ok {
				log.Printf("Discarding, no 'message' key in: '%s'", scnr.Text())
				continue
			}
		} else {
			p.statsChan <- Plain
			m = p.transform.MsgToMap(scnr)
			msgBody = []byte(m["message"].(string))
		}
		if !dupes.Insert(msgBody) {
			p.statsChan <- Duplicate
			continue
		}

		// If this is valid JSON, publish it to a JSON messages channel, else
		// it goes to the plaintext channel.
		if !validJSON { // Invalid JSON extracted from stdin
			switch PutWithTimeout(t.json, m, p.conf.chanTimeoutRedis) {
			case errTimedOut:
				if p.conf.debug {
					log.Printf("dropped PLAIN->redis message after %v timeout", p.conf.chanTimeoutRedis)
				}
			case nil:
				if p.conf.debug {
					log.Printf("PLAIN->redis: %v", m)
				}
			}
			switch PutWithTimeout(
				t.syslogplaintext, m, p.conf.chanTimeoutSyslog) {
			case errTimedOut:
				if p.conf.debug {
					log.Printf("dropped PLAIN->syslog message after %v timeout", p.conf.chanTimeoutSyslog)
				}
			case nil:
				if p.conf.debug {
					log.Printf("PLAIN->syslog: %v", m)
				}
			}
		} else { // Valid JSON extracted from stdin
			// When "source" key does not exist, we use the tag `-t` with which
			// this program was started.
			if _, ok := getValue("source", m); !ok {
				m["source"] = p.source
			}
			// Fallback to default key or key supplied via command line
			// argument.
			if _, ok := getValue("key", m); !ok {
				m["key"] = p.conf.key
			}
			// We attempt to derive level from the data if we don't already
			// have it in this map.
			var level syslog.Priority
			if levelStr, ok := getValue("level", m); !ok {
				level = detectLevel(scnr)
				m["level"] = levelToStr(level, p.conf.level)
			} else {
				level = strToPriority(
					levelStr, p.conf.SyslogLevel(), 0)
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
				m["orig_ts"] = v
			}
			m["ts"] = time.Now().Format(time.RFC3339Nano)
			// Send map with message contents to Redis and Syslog publishing
			// goroutines. Timeout after specified interval by selecting on
			// the supplied channel and a timer channel.
			switch PutWithTimeout(t.json, m, p.conf.chanTimeoutRedis) {
			case errTimedOut:
				if p.conf.debug {
					log.Printf("dropped JSON->redis message after %v timeout", p.conf.chanTimeoutRedis)
				}
			case nil:
				if p.conf.debug {
					log.Printf("JSON->redis: %v", m)
				}
			}
			switch PutWithTimeout(t.syslogjson, m, p.conf.chanTimeoutSyslog) {
			case errTimedOut:
				if p.conf.debug {
					log.Printf("dropped JSON->syslog message after %v timeout", p.conf.chanTimeoutRedis)
				}
			case nil:
				if p.conf.debug {
					log.Printf("JSON->syslog: %v", m)
				}
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

// IsBoolType detects if a given string should be treated as a boolean value.
// It uses struct{}(s) as values in the map because they don't use any memory
// and we don't actually care for contents of the values in this case, just that
// we can perform Θ(1) lookups with an input string.
func IsBoolType(s string) bool {
	var m = map[string]struct{}{
		"true":  struct{}{},
		"True":  struct{}{},
		"TRUE":  struct{}{},
		"false": struct{}{},
		"False": struct{}{},
		"FALSE": struct{}{},
		"yes":   struct{}{},
		"Yes":   struct{}{},
		"YES":   struct{}{},
		"no":    struct{}{},
		"No":    struct{}{},
		"NO":    struct{}{},
	}
	_, ok := m[s]
	return ok
}

// StringToBool returns a boolean value for a given string. It assumes that a
// string which it received via argument `s` is going to exist in the map.
// There is no checking performed as an optimization. IsBoolType must be used
// first to make sure that a given string is in fact something we recognize
// semantically as a boolean value.
func StringToBool(s string) bool {
	var m = map[string]bool{
		"true":  true,
		"True":  true,
		"TRUE":  true,
		"false": false,
		"False": false,
		"FALSE": false,
		"yes":   true,
		"Yes":   true,
		"YES":   true,
		"no":    false,
		"No":    false,
		"NO":    false,
	}
	return m[s]
}

// MsgToMap takes a pointer to a buffer from the dispatch(...) function and
// attempts to extract a few details by parsing the message either using the
// default pattern or pattern supplied via command line argument.
func (p *Parser) MsgToMap(scnr *bufio.Scanner) map[string]interface{} {
	m := make(map[string]interface{})
	results, ok := p.parse(scnr.Bytes())
	if ok {
		for k, v := range results {
			if IsBoolType(v) {
				m[k] = StringToBool(v)
			} else {
				m[k] = v
			}
		}
	}

	setValueWhenMissing("key", p.conf.key, m)
	var level syslog.Priority
	if _, ok := m["level"]; !ok {
		level = detectLevel(scnr)
		m["level"] = levelToStr(level, p.conf.level)
	}
	if level < syslog.LOG_DEBUG {
		m["ccs"] = true
	} else {
		m["ccs"] = false
	}
	m["source"] = p.conf.tag
	m["ts"] = time.Now().Format(time.RFC3339Nano)
	m["plaintext"] = true

	if p.conf.ignoreMissingMsg {
		return m
	}

	// If after parsing the message we still do not have a `message` field, we
	// use the entire line as message instead. This is a fallback really.
	// getStringValueOrDefault("message", scnr.Text(), m)
	if _, ok := getMessage(m); !ok {
		m["message"] = scnr.Text()
	}

	return m
}

func (p *Parser) parse(b []byte) (map[string]string, bool) {
	results := p.parserRegexp.FindAllSubmatch(b, -1)
	if len(results) == 0 {
		if p.conf.debug {
			log.Printf("Regexp produced no matches for: '%s'", string(b))
		}
		return nil, false
	}

	// Our base case is to use the default pattern, which should make it
	// possible to extract any additional labels which follow the message
	// string. If there are no such labels, the entire string is treated as a
	// message instead.
	if p.conf.parserPattern == DefaultParserPattern {
		return p.defaultRegexpMatchesToMap(results)
	}

	// Here we are making the assumption that a regex pattern has been provided
	// as an argument to this program, and it contains one or more named capture
	// groups, one of which extracts the message and is called `message` and
	// others, if there are any, are going to be any additional labels, such as
	// the level of this message, or any other "tags" that we want to include
	// with this message.
	// Names of each capture group will become the keys, and correspond to
	// values which they are supposed to extract from the message.
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

// defaultRegexpMatchesToMap handles the default Regular Expression pattern,
// which assumes a plaintext message with one or more key=value pairs after the
// actual message. Notice, key=value pairs are only matched if there is a
// leading double-space `\s\s` after the actual message string, as in this
// example:
// `Important thingy failed  level=error function=thingyAlpha`.
func (p *Parser) defaultRegexpMatchesToMap(results [][][]byte) (map[string]string, bool) {
	if p.conf.debug {
		for i, outter := range results {
			for j, inner := range outter {
				log.Printf("results[%d][%d] => %s", i, j, string(inner))
			}
		}
	}

	m := make(map[string]string, len(results[0]))
	m["message"] = strings.TrimRight(string(results[0][0]), " ")
	var tuple *KeyValueTuple
	for i := 1; i < len(results); i++ {
		if tuple = BytesToKeyValue(results[i][0]); tuple != nil {
			m[tuple.Key] = tuple.Value
		}
	}
	return m, len(m) > 0
}

type KeyValueTuple struct {
	Key   string
	Value string
}

// BytesToKeyValue converts a slice of bytes to a KeyValueTuple struct, and
// returns a pointer to KeyValueTuple on success or a nil on failure.
func BytesToKeyValue(b []byte) *KeyValueTuple {
	split := strings.Split(string(b), "=")
	if len(split) != 2 {
		return nil
	}
	return &KeyValueTuple{
		Key:   split[0],
		Value: split[1],
	}
}

func getMessage(m map[string]interface{}) (string, bool) {
	for _, key := range []string{"message", "msg"} {
		if v, ok := (m[key]).(string); ok {
			return v, true
		}
	}
	return "", false
}

func getMessageBytes(m map[string]interface{}) ([]byte, bool) {
	for _, key := range []string{"message", "msg"} {
		if v, ok := (m[key]).(string); ok {
			return []byte(v), true
		}
	}
	return []byte{}, false
}

func getValue(k string, m map[string]interface{}) (string, bool) {
	if v, ok := (m[k]).(string); ok {
		return v, true
	}
	return "", false
}

func setValueWhenMissing(k, v string, m map[string]interface{}) {
	if _, ok := m[k]; !ok {
		m[k] = v
	}
	return
}

type topics struct {
	json            chan map[string]interface{}
	syslogjson      chan map[string]interface{}
	syslogplaintext chan map[string]interface{}
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
	const numOfWorkers = 3
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
	var compiled = regexp.MustCompile(conf.parserPattern)

	return &Publish{
		conf: conf,
		chans: &topics{
			json:            make(chan map[string]interface{}, conf.chanBufLen),
			syslogjson:      make(chan map[string]interface{}, conf.chanBufLen),
			syslogplaintext: make(chan map[string]interface{}, conf.chanBufLen),
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

// SyslogFunc is a function prototype for a function returned from loggerfn.
type SyslogFunc func(string) error

func logWriterForLevel(levelStr string, w SyslogWriter) SyslogFunc {
	var mapLevelToFunc = map[syslog.Priority]SyslogFunc{
		syslog.LOG_CRIT:    w.Crit,
		syslog.LOG_ERR:     w.Err,
		syslog.LOG_WARNING: w.Warning,
		syslog.LOG_NOTICE:  w.Notice,
		syslog.LOG_INFO:    w.Info,
		syslog.LOG_DEBUG:   w.Debug,
	}

	var level = strToLevel(levelStr, syslog.LOG_NOTICE)
	return mapLevelToFunc[level]
}

// SyslogWriter is an interface which for some reason does not exist in the go
// standard library's syslog package. This was added mainly to make testing
// easier in the future.
type SyslogWriter interface {
	Close() error
	Write(b []byte) (int, error)
	Emerg(m string) error
	Alert(m string) error
	Crit(m string) error
	Err(m string) error
	Warning(m string) error
	Notice(m string) error
	Info(m string) error
	Debug(m string) error
}

func mapToSyslogWriter(
	m map[string]interface{},
	levelStr string,
	w SyslogWriter) error {
	// If the mapping does not already have a level key, use the fallback
	// value passed in via levelStr.
	if _, ok := getValue("level", m); !ok {
		m["level"] = levelStr
	}
	// Expect that message key could be either "msg" or "message".
	// If neither is found, or not a string value, we abandon processing
	// this particular object.
	// var msg string
	for _, k := range [...]string{"message", "msg"} {
		if v, ok := getValue(k, m); ok {
			fn := logWriterForLevel(m["level"].(string), w)
			return fn(v)
		}
	}
	return errMissingMsgKey
}

func (p *Publish) publishToSyslog(w SyslogWriter) {
	for {
		select {
		case m := <-p.chans.syslogplaintext:
			mapToSyslogWriter(m, p.conf.SyslogLevelString(), w)
		case m := <-p.chans.syslogjson:
			mapToSyslogWriter(m, p.conf.SyslogLevelString(), w)
		case <-p.doneChan:
			if p.conf.debug {
				log.Println("Shutting down publishToSyslog")
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
				log.Printf("Redis error %v", err)
				time.Sleep(delay)
				if delay < 30*time.Second {
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
		// "tcp",
		// "localhost:5514",
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
	// Start syslog writing goroutine for Plaintext and JSON messages to syslog
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
