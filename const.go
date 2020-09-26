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
	// any in the tail of the message, enclosed with '[' and ']'.
	// In other words if message is:
	// "read failed [function=alpha key=NotReadable]", then the matched pairs
	// will be function=alpha and key=NotReadable.
	// The default pattern will produce at least one named group: message and
	// possibly repeating labels group. In RE2 lingo these are submatches.
	DefaultParserPattern = `(?m)^(?P<message>[^\[\]]+\b)|(?P<labels>\b\S+=\S+\b)`

	DefaultKey = "SyslogMessage"

	// DefaultExpireDupesAfter is the amount of time after which a previously
	// seen message will not be considered a duplicate. For example, if the
	// if this value is N, and two identical messages are logged N + 1 units
	// of time apart, they will not be considered duplicates.
	DefaultExpireDupesAfter = 10 * time.Second

	// DefaultConcurrentWorkers is the number of concurrently executing message
	// forwarding goroutines. Theses are the routines which receive messages
	// from the actual processing goroutine and are responsible for delivery of
	// messages to the target.
	DefaultConcurrentWorkers = 3

	// MaximumConcurrentWorkers sets an upper limit on the number of currently
	// executing message forwarding goroutines.
	MaximumConcurrentWorkers = 100
)
