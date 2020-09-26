package main

import (
	"log"
	"log/syslog"
	"os"
	"os/signal"
	"runtime/pprof"
	"syscall"
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

	// Setup profiling if enabled
	if cliArgs.CPUProfileEnabled() {
		log.Printf("Writing CPU profiling data to: %s", cliArgs.cpuprofile)
		f, err := os.Create(cliArgs.cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

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
		var sysLog *syslog.Writer
		var err error
		var pri = strToPriority(
			cliArgs.priority,
			syslog.LOG_NOTICE,
			syslog.LOG_DAEMON)

		if strToSyslogConn(cliArgs.SyslogNetworkString()) == Local {
			if cliArgs.debug {
				log.Println("Local syslog connection")
			}
			sysLog, err = syslog.New(pri, cliArgs.tag)
		} else {
			if cliArgs.debug {
				log.Printf("Remote syslog connection to %s:%d", cliArgs.syslogHost, cliArgs.syslogPort)
			}
			sysLog, err = syslog.Dial(
				// "tcp",
				// "localhost:5514",
				// "unixgram",
				// "/dev/log",
				cliArgs.SyslogNetworkString(),
				cliArgs.SyslogRAddrString(),
				pri, cliArgs.tag)
		}

		if err != nil {
			log.Fatal(err)
		}
		// Start syslog publishing goroutines for Plaintext and JSON messages
		for i := 0; i < int(cliArgs.workerCount); i++ {
			go p.publishToSyslog(sysLog)
			p.numOfWorkers += 1
		}
	}

	// Initialize duplicate messages structure
	dupes := NewMap()

	// Start statistics processing goroutine
	pw := NewPrometheusExportWriter(cliArgs.statsDir, cliArgs.tag)
	go p.stats(pw)
	p.numOfWorkers += 1

	// Start dispatch goroutine (it feeds the publishing goroutines)
	go dispatch(p, dupes)
	// Start duplicate message processing goroutine
	go reportSuppressed(p, dupes)

	<-doneChan
	p.Close()
}
