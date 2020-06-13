package main

import (
	"time"
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
