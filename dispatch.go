package main

import (
	"bufio"
	"encoding/json"
	"log"
	"log/syslog"
	"os"
	"time"
)

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
			p.statsChan <- JSON
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
			for _, v := range []struct {
				name string
				c    chan map[string]interface{}
				t    time.Duration
				skip bool
			}{
				{"redis", t.json, p.conf.chanTimeoutRedis, p.conf.redisDisabled},
				{"syslog", t.syslogplaintext, p.conf.chanTimeoutSyslog, p.conf.syslogDisabled},
			} {
				if v.skip { // Skipping if this sink is disabled
					continue
				}
				switch PutWithTimeout(v.c, m, v.t) {
				case errTimedOut:
					if p.conf.debug {
						log.Printf("dropped PLAIN->%s message after %v timeout", v.name, p.conf.chanTimeoutRedis)
					}
				case nil:
					if p.conf.debug {
						log.Printf("PLAIN->%s: %v", v.name, m)
					}
				}
			}
		} else { // Valid JSON extracted from stdin
			// When "source" key does not exist, we use the tag `-t` with which
			// this program was started.
			if _, ok := getValue("source", m); !ok {
				m["source"] = p.source
			}
			// if there is a tag property add it to the source with a dot '.'
			// as a separator. This changes source to 'source.tag', which is
			// useful in cases where we might have multiple instances of the
			// same process running.
			if v, ok := getValue("tag", m); !ok {
				m["source"] = p.source + "." + v
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
				level = detectLevel(scnr, levelsRegexp)
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
			for _, v := range []struct {
				name string
				c    chan map[string]interface{}
				t    time.Duration
				skip bool
			}{
				{"redis", t.json, p.conf.chanTimeoutRedis, p.conf.redisDisabled},
				{"syslog", t.syslogjson, p.conf.chanTimeoutSyslog, p.conf.syslogDisabled},
			} {
				if v.skip { // skip if this sink is disabled
					continue
				}
				switch PutWithTimeout(v.c, m, v.t) {
				case errTimedOut:
					if p.conf.debug {
						log.Printf("dropped JSON->%s message after %v timeout", v.name, p.conf.chanTimeoutRedis)
					}
				case nil:
					if p.conf.debug {
						log.Printf("JSON->%s: %v", v.name, m)
					}
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
