package main

import (
	"encoding/json"
	"log"
	"time"
)

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
					for _, v := range []struct {
						name string
						c    chan map[string]interface{}
						t    time.Duration
						skip bool
					}{
						// {"redis", t.json, p.conf.chanTimeoutRedis, p.conf.redisDisabled},
						{"syslog", t.syslogjson, p.conf.chanTimeoutSyslog, p.conf.syslogDisabled},
					} {
						if v.skip { // skip if this sink is disabled
							continue
						}
						switch PutWithTimeout(v.c, m, v.t) {
						case errTimedOut:
							if p.conf.debug {
								log.Printf("dropped suppressed JSON->%s report after %v timeout", v.name, p.conf.chanTimeoutRedis)
							}
						case nil:
							if p.conf.debug {
								log.Printf("JSON->%s: %v", v.name, m)
							}
						}
					}
					// select {
					// case t.json <- m:
					// case <-time.After(ChanTimeout):
					// 	log.Println("dropped suppressed report")
					// }
				}
			}
			dupes.Expire(p.conf.expireDupesAfter)
			timestamp = time.Now() // update timestamp for next report
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
