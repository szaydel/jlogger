package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"time"
)

type stat uint16

const (
	Line = iota
	Plain
	JSON
	JSONNoMsg
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

// Counters is meant to be private, but it is "exported" in order to be
// JSON-marshal(able). In reality, it is not visible outside of this method.
type Counters struct {
	Line      int64 `json:"line"`
	Plain     int64 `json:"plaintext"`
	JSON      int64 `json:"json_encoded"`
	JSONNoMsg int64 `json:"json_encoded_missing_message"`
	Duplicate int64 `json:"duplicate"`
	Crit      int64 `json:"critical"`
	Err       int64 `json:"error"`
	Warning   int64 `json:"warning"`
	Notice    int64 `json:"notice"`
	Info      int64 `json:"info"`
	Debug     int64 `json:"debug"`
	conf      *args // arguments from command line
}

type CounterDesc struct {
	name        string
	helpMsg     string
	metricType  string
	metricValue int64
}

func (c *Counters) WriteTo(w io.Writer) (int64, error) {
	cdMap := map[stat]CounterDesc{
		Crit: CounterDesc{
			name:        "messages_level_crit_total",
			helpMsg:     "Total number of messages logged with level CRITICAL",
			metricType:  "counter",
			metricValue: c.Crit,
		},
		Err: CounterDesc{
			name:        "messages_level_err_total",
			helpMsg:     "Total number of messages logged with level ERROR",
			metricType:  "counter",
			metricValue: c.Err,
		},
		Warning: CounterDesc{
			name:        "messages_level_warn_total",
			helpMsg:     "Total number of messages logged with level WARNING",
			metricType:  "counter",
			metricValue: c.Warning,
		},
		Notice: CounterDesc{
			name:        "messages_level_notice_total",
			helpMsg:     "Total number of messages logged with level NOTICE",
			metricType:  "counter",
			metricValue: c.Notice,
		},
		Info: CounterDesc{
			name:        "messages_level_info_total",
			helpMsg:     "Total number of messages logged with level INFO",
			metricType:  "counter",
			metricValue: c.Info,
		},
		Debug: CounterDesc{
			name:        "messages_level_debug_total",
			helpMsg:     "Total number of messages logged with level DEBUG",
			metricType:  "counter",
			metricValue: c.Debug,
		},
		Plain: CounterDesc{
			name:        "plaintext_messages_total",
			helpMsg:     "Total number of plaintext messages",
			metricType:  "counter",
			metricValue: c.Plain,
		},
		JSON: CounterDesc{
			name:        "json_encoded_messages_total",
			helpMsg:     "Total number of JSON encoded messages",
			metricType:  "counter",
			metricValue: c.JSON,
		},
		Duplicate: CounterDesc{
			name:        "duplicate_messages_total",
			helpMsg:     "Total number of messages recorded as duplicates",
			metricType:  "counter",
			metricValue: c.Duplicate,
		},
	}
	return c.prometheusExpoWriter(w, cdMap)
}

func(c *Counters) prometheusExpoWriter(
	w io.Writer,
	cdMap map[stat]CounterDesc,
) (int64, error) {
	// Allocate a buffer and write all necessary lines to the buffer before
	// sending the entire buffer to the supplied writer. This reduces the amount
	// of accounting which we need to do here in terms of tracking bytes
	// actually written. We just return result of a single WriteTo(...) with the
	// supplied writer, after we fill the buffer with all required information.
	var buf bytes.Buffer
	for _, v := range []stat{
		Crit,
		Err,
		Warning,
		Notice,
		Info,
		Debug,
		Plain,
		JSON,
		Duplicate,
	} {

		if _, err := fmt.Fprintf(&buf, "# HELP %s %s\n",
			cdMap[v].name, cdMap[v].helpMsg); err != nil {
			return 0, err
		}
		if _, err := fmt.Fprintf(&buf, "# TYPE %s %s\n",
			cdMap[v].name, cdMap[v].metricType); err != nil {
			return 0, err
		}
		if _, err := fmt.Fprintf(&buf, "%s{tag=\"%s\"} %d\n",
			cdMap[v].name, c.conf.tag, cdMap[v].metricValue); err != nil {
			return 0, err
		}
	}
	return buf.WriteTo(w)
}

// stats tracks number of unique messages received. Levels are only extracted
// from JSON messages where object contains field called "level", and that
// field contains a name known to this utility.
func (p *Publish) stats(w io.Writer) {
	const reportInterval = 10

	defer p.AckDone() // join the main goroutine
	var counts [Unknown]int64
	var t = time.NewTicker(reportInterval * time.Second).C
	for {
		select {
		case <-t:
			c := &Counters{
				Line:      counts[Line],
				Plain:     counts[Plain],
				JSONNoMsg: counts[JSONNoMsg],
				JSON:      counts[JSON],
				Duplicate: counts[Duplicate],
				Crit:      counts[Crit],
				Err:       counts[Err],
				Warning:   counts[Warning],
				Notice:    counts[Notice],
				Info:      counts[Info],
				Debug:     counts[Debug],
				conf:      p.conf, // passing configuration from publish
			}
			c.WriteTo(w)
			b, _ := json.Marshal(
				map[string]interface{}{
					"counts": Counters{
						Line:      counts[Line],
						Plain:     counts[Plain],
						JSONNoMsg: counts[JSONNoMsg],
						JSON:      counts[JSON],
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
			return
		}
	}
}
