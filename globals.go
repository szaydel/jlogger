package main

import (
	"errors"
	"regexp"
)

var errTimedOut = errors.New("Write failed with a timeout")
var errMissingMsgKey = errors.New("Mapping missing required message key")
var errInvalidPort = errors.New("Port must be a positive 16-bit integer")

type LevelsRegexp struct {
	debug *regexp.Regexp
	err   *regexp.Regexp
	info  *regexp.Regexp
	warn  *regexp.Regexp
}

var levelsRegexp = LevelsRegexp{
	debug: regexp.MustCompile("(?i:debug)"),
	err:   regexp.MustCompile("(?i:error|fail|fault|crit|panic)"),
	info:  regexp.MustCompile("(?i:info)"),
	warn:  regexp.MustCompile("(?i:warn|caution)"),
}

var cliArgs args
