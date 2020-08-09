package main

import (
	"bufio"
	// "encoding/json"
	"log"
	"regexp"
	"time"

	// "github.com/go-redis/redis/v7"
)

// type PubSubInterface interface {
// 	Publish(channel string, message interface{}) *redis.IntCmd
// 	Subscribe(channels ...string) *redis.PubSub
// }

type Transformer interface {
	MsgToMap(scnr *bufio.Scanner) map[string]interface{}
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
	conf         *args // arguments from command line
	chans        *topics
	ackDoneChan  chan struct{}
	doneChan     chan struct{}
	statsChan    chan stat
	source       string
	numOfWorkers int
	transform    Transformer
}

func (p *Publish) AckDone() {
	p.ackDoneChan <- struct{}{}
}

// Close shuts down publishers, and should be called before terminating
// the program.
func (p *Publish) Close() {
	close(p.doneChan)
	// This blocks until all publishers acknowledge and corresponding
	// goroutines return. We do not close any of the *topics channels to avoid
	// a write on closed channel in the dispatch(...) goroutine.
	for i := 0; i < p.numOfWorkers; i++ {
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

func (p *Publish) publishToSyslog(w SyslogWriter) {
	id := time.Now().Nanosecond()
	if p.conf.debug {
		log.Printf("publishToSyslog (%d): started", id)
	}
	defer p.AckDone() // join the main goroutine
	for {
		select {
		case m, ok := <-p.chans.syslogplaintext:
			if !ok { // closed channel
				return
			}
			if p.conf.debug {
				log.Printf("publishToSyslog (%d): plaintext", id)
			}
			mapToSyslogWriter(m, p.conf.SyslogLevelString(), w)
		case m, ok := <-p.chans.syslogjson:
			if !ok { // closed channel
				return
			}
			if p.conf.debug {
				log.Printf("publishToSyslog (%d): json", id)
			}
			mapToSyslogWriter(m, p.conf.SyslogLevelString(), w)
		case <-p.doneChan:
			if p.conf.debug {
				log.Println("Shutting down publishToSyslog")
			}
			return
		}
	}
}

// func (p *Publish) publishToRedis(ps PubSubInterface) {
// 	defer p.AckDone() // join the main goroutine
// 	psValidate := ps.Subscribe("json_msgs")
// 	var connected bool
// 	var delay time.Duration = 1 * time.Second
// 	// Retry connecting to Redis indefinitely. Upon connection the library
// 	// will take over and help to reconnect and re-subscribe as necessary.
// 	for !connected {
// 		select {
// 		case <-p.doneChan:
// 			if p.conf.debug {
// 				log.Println("Shutting down publishToRedis")
// 			}
// 			return
// 		default:
// 			if _, err := psValidate.Receive(); err != nil {
// 				log.Printf("Redis error %v", err)
// 				time.Sleep(delay)
// 				if delay < 30*time.Second {
// 					delay += (delay/2 + 1)
// 				}
// 				if p.conf.debug {
// 					log.Printf("Delaying reconnect by %v", delay)
// 				}
// 			} else {
// 				connected = true
// 			}
// 		}
// 	}

// 	for {
// 		select {
// 		case msg := <-p.chans.json:
// 			msgEncoded, _ := json.Marshal(msg)
// 			ps.Publish("json_msgs", msgEncoded)
// 		case <-p.doneChan:
// 			if p.conf.debug {
// 				log.Println("Shutting down publishToRedis")
// 			}
// 			return
// 		}
// 	}
// }
