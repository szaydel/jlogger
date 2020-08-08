package main

import (
	"flag"
	"fmt"
	"log/syslog"
	"time"
)

type args struct {
	// redisConfigFile   string
	debug             bool
	ignoreMissingMsg  bool
	syslogDisabled    bool
	// redisDisabled     bool
	chanBufLen        int
	chanTimeoutRedis  time.Duration
	chanTimeoutSyslog time.Duration
	expireDupesAfter  time.Duration
	key               string
	level             string // not implemented yet
	parserPattern     string
	priority          string
	tag               string
	syslogSyslogConn  SyslogConn
	syslogHost        string
	syslogPort        Port
}

func (a *args) SyslogLevel() syslog.Priority {
	return strToLevel(a.level, syslog.LOG_NOTICE)
}

func (a *args) SyslogLevelString() string {
	return a.level
}

func (a args) SyslogNetworkString() string {
	switch a.syslogSyslogConn {
	case TCP:
		return "tcp"
	case UDP:
		return "udp"
	default:
		return "unixgram"
	}
}

func (a args) SyslogRAddrString() string {
	if a.syslogSyslogConn == Unixgram {
		return "/dev/log"
	}
	return fmt.Sprintf("%s:%d", a.syslogHost, a.syslogPort)
}

func setupCliFlags() {
	flag.IntVar(&cliArgs.chanBufLen, "channel.buffer.length", ChanBufferLen, "How many messages to allow in the buffer before discards may happen")
	flag.BoolVar(&cliArgs.debug, "debug", false, "Enable debugging")
	flag.BoolVar(&cliArgs.ignoreMissingMsg, "ignore.missing.msg", false, "Do not look for a message key after parsing lines")
	// flag.BoolVar(&cliArgs.redisDisabled, "redis.disabled", false, "Skip publishing to redis when set")
	flag.BoolVar(&cliArgs.syslogDisabled, "syslog.disabled", false, "Skip publishing to syslog when set")
	flag.DurationVar(&cliArgs.chanTimeoutRedis, "redis.timeout.ms", ChanTimeout, "Set timeout value for sending messages to Redis")
	flag.DurationVar(&cliArgs.chanTimeoutSyslog, "syslog.timeout.ms", ChanTimeout, "Set timeout value for sending messages to Syslog")
	flag.DurationVar(&cliArgs.expireDupesAfter, "expire.dupes.after.s", DefaultExpireDupesAfter, "Amount of time after which previously seen message is not a duplicate")
	flag.StringVar(&cliArgs.key, "key", DefaultKey, "Key with which to publish messages")
	flag.StringVar(&cliArgs.tag, "t", "demotag", "Tag with which to publish messages")
	flag.StringVar(&cliArgs.parserPattern, "pattern", DefaultParserPattern, "Pattern containing minimally a <msg> capture group")
	flag.StringVar(&cliArgs.priority, "p", "daemon.notice", "Priority as 'facility.level' to use when message does not have one already")
	// flag.StringVar(&cliArgs.redisConfigFile, "redis.config.file", "redis.", "Configuration file location with Redis db info")
	flag.Var(&cliArgs.syslogSyslogConn, "syslog.conn", "One of three possible choices: tcp, udp, unixgram")
	flag.Var(&cliArgs.syslogPort, "syslog.port", "Which port to use for syslog connection")
	flag.Parse()
	// If the flag holds an out of range value for cliArgs.syslogPort, an error
	// will be raised when flags are parsed. Otherwise, we use the default
	// syslog port, 514.
	if cliArgs.syslogPort == 0 {
		cliArgs.syslogPort = 514
	}

	cliArgs.level = "notice" // FIXME: should derive from CLI args
}
