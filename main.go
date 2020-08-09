package main

import (
	"log"
	"log/syslog"
	"os"
	"os/signal"
	"syscall"

	// "github.com/go-redis/redis/v7"
)

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

	// Setup pipelines
	p := NewPublish(&cliArgs)

	// If we did not choose to disable Redis, setup the client, however the
	// actual publisher setup happens in the p.publishToRedis(...) goroutine.
	// if !cliArgs.redisDisabled {
	// 	redisConfig := NewRedisConfig(cliArgs.redisConfigFile).ToRedisOptions()
	// 	rdb := redis.NewClient(redisConfig)
	// 	// Start Redis publishing goroutine
	// 	go p.publishToRedis(rdb)
	// 	p.numOfWorkers += 1
	// }

	// If we did not choose to disable Syslog, setup connection.
	if !cliArgs.syslogDisabled {
		sysLog, err := syslog.Dial(
			// "tcp",
			// "localhost:5514",
			// "unixgram",
			// "/dev/log",
			cliArgs.SyslogNetworkString(),
			cliArgs.SyslogRAddrString(),
			strToPriority(
				cliArgs.priority,
				syslog.LOG_NOTICE,
				syslog.LOG_DAEMON,
			), cliArgs.tag)
		if err != nil {
			log.Fatal(err)
		}
		// Start syslog publishing goroutines for Plaintext and JSON messages
		go p.publishToSyslog(sysLog)
		go p.publishToSyslog(sysLog)
		go p.publishToSyslog(sysLog)
		p.numOfWorkers += 3
	}

	// Initialize duplicate messages structure
	dupes := NewMap()

	// Start statistics processing goroutine
	pw := NewPrometheusExportWriter("/run/jlogger", cliArgs.tag)
	go p.stats(pw)
	p.numOfWorkers += 1

	// Start dispatch goroutine (it feeds the publishing goroutines)
	go dispatch(p, dupes)
	// Start duplicate message processing goroutine
	go reportSuppressed(p, dupes)

	<-doneChan
	p.Close()
}
