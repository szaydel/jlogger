package main

import (
	"bufio"
	"log"
	"log/syslog"
	"regexp"
	"strings"
	"time"
)

type Parser struct {
	conf         *args
	parserRegexp *regexp.Regexp
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
		level = detectLevel(scnr, levelsRegexp)
		m["level"] = levelToStr(level, p.conf.level)
	}
	if level < syslog.LOG_DEBUG {
		m["ccs"] = true
	} else {
		m["ccs"] = false
	}
	// if there is a tag property add it to the source with a dot '.' as a
	// separator. This changes source to 'source.tag', which is useful in cases
	// where we might have multiple instances of the same process running.
	if v, ok := m["tag"]; ok {
		m["source"] = p.conf.tag + "." + v.(string)
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
