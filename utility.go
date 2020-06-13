package main

import (
	"math"
	"strings"
	"time"
)

// IsBoolType detects if a given string should be treated as a boolean value.
// It uses struct{}(s) as values in the map because they don't use any memory
// and we don't actually care for contents of the values in this case, just that
// we can perform Θ(1) lookups with an input string.
func IsBoolType(s string) bool {
	var m = map[string]struct{}{
		"true":  struct{}{},
		"True":  struct{}{},
		"TRUE":  struct{}{},
		"false": struct{}{},
		"False": struct{}{},
		"FALSE": struct{}{},
		"yes":   struct{}{},
		"Yes":   struct{}{},
		"YES":   struct{}{},
		"no":    struct{}{},
		"No":    struct{}{},
		"NO":    struct{}{},
	}
	_, ok := m[s]
	return ok
}

// StringToBool returns a boolean value for a given string. It assumes that a
// string which it received via argument `s` is going to exist in the map.
// There is no checking performed as an optimization. IsBoolType must be used
// first to make sure that a given string is in fact something we recognize
// semantically as a boolean value.
func StringToBool(s string) bool {
	var m = map[string]bool{
		"true":  true,
		"True":  true,
		"TRUE":  true,
		"false": false,
		"False": false,
		"FALSE": false,
		"yes":   true,
		"Yes":   true,
		"YES":   true,
		"no":    false,
		"No":    false,
		"NO":    false,
	}
	return m[s]
}

// KeyValueTuple is really an approximation of a "namedtuple" using a struct.
type KeyValueTuple struct {
	Key   string
	Value string
}

// BytesToKeyValue converts a slice of bytes to a KeyValueTuple struct, and
// returns a pointer to KeyValueTuple on success or a nil on failure.
func BytesToKeyValue(b []byte) *KeyValueTuple {
	split := strings.Split(string(b), "=")
	if len(split) != 2 {
		return nil
	}
	return &KeyValueTuple{
		Key:   split[0],
		Value: split[1],
	}
}

func getMessage(m map[string]interface{}) (string, bool) {
	for _, key := range []string{"message", "msg"} {
		if v, ok := (m[key]).(string); ok {
			return v, true
		}
	}
	return "", false
}

func getMessageBytes(m map[string]interface{}) ([]byte, bool) {
	for _, key := range []string{"message", "msg"} {
		if v, ok := (m[key]).(string); ok {
			return []byte(v), true
		}
	}
	return []byte{}, false
}

func getValue(k string, m map[string]interface{}) (string, bool) {
	if v, ok := (m[k]).(string); ok {
		return v, true
	}
	return "", false
}

func setValueWhenMissing(k, v string, m map[string]interface{}) {
	if _, ok := m[k]; !ok {
		m[k] = v
	}
	return
}

// atoi is a variant of the C implementation of the atoi function for converting
// string representations of numbers into integer values.
func atoi(s string) int {
	if len(s) == 1 {
		return int(s[0] - '0')
	}
	y := atoi(s[1:])
	x := int(s[0] - '0')
	pow := math.Pow(10., float64(len(s)-1))
	x = (x * int(pow)) + y
	return x
}

func computeRatio(n, d int64) float64 {
	if n == 0 || d == 0 {
		return 0.
	}
	return float64(n) / float64(d)
}

// PutWithTimeout sends the message on the supplied channel and times out after
// interval `t` if the channel in argument `c` is blocked.
func PutWithTimeout(
	c chan map[string]interface{},
	m map[string]interface{},
	t time.Duration) error {
	select {
	case c <- m:
		return nil
	case <-time.After(t):
		return errTimedOut
	}
}
