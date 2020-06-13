package main

import (
	"bufio"
	"encoding/json"
	"log/syslog"
	"strings"
)

func detectLevel(scnr *bufio.Scanner, levelsRegexp LevelsRegexp) syslog.Priority {
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

// SyslogFunc is a function prototype for a function returned from loggerfn.
type SyslogFunc func(string) error

// logWriterForLevel returns correct syslog function for the given level.
// This aids in writing messages to syslog with correct severity level.
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

// SyslogWriter is an interface which for some reason does not exist in the Go
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

// mapToSyslogWriter writes the given map to syslog as JSON-serialized message.
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
	fn := logWriterForLevel(m["level"].(string), w)
	if b, err := json.Marshal(m); err != nil {
		return err
	} else {
		return fn(string(b))
	}

	// Originally, I was extracting the message out of the map, but instead
	// decided to simply marshal the map into JSON and convert the byte slice
	// into a string, which then gets written to syslog. This preserves much
	// more context. It also makes syslog and redis a little more equal to
	// each other.
	// for _, k := range [...]string{"message", "msg"} {
	// 	if v, ok := getValue(k, m); ok {
	// 		fn := logWriterForLevel(m["level"].(string), w)
	// 		return fn(v)
	// 	}
	// }
	// return errMissingMsgKey
}

type SyslogConn int

const (
	Unixgram SyslogConn = iota
	TCP
	UDP
)

// String returns a string value for a given SyslogConn enum.
// This method is required to implement the flag.Value interface.
func (nt SyslogConn) String() string {
	ntToStrMap := map[SyslogConn]string{
		TCP:      "tcp",
		UDP:      "udp",
		Unixgram: "unixgram",
	}
	return ntToStrMap[nt]
}

// Set sets the SyslogConn value to given string, when valid, otherwise it
// falls back to using unixgram. There is never an error here, due to fallback.
// This method is required to implement the flag.Value interface.
func (nt *SyslogConn) Set(arg string) error {
	*nt = strToSyslogConn(arg)
	return nil
}

func strToSyslogConn(netTypeStr string) SyslogConn {
	strToNtMap := map[string]SyslogConn{
		"tcp":      TCP,
		"udp":      UDP,
		"unixgram": Unixgram,
		"unix":     Unixgram,
	}
	if v, ok := strToNtMap[strings.ToLower(netTypeStr)]; ok {
		return v
	}
	return Unixgram // Fallback to unixgram when given
}
