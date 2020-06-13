package main

import (
	"fmt"
	"math"
)

type Port uint16

// String returns a string value for a given Port.
// This method is required to implement the flag.Value interface.
func (p Port) String() string {
	return fmt.Sprintf("%d", p)
}

// Set sets the Port value to given string, when valid, otherwise it
// returns a errInvalidPort error.
// This method is required to implement the flag.Value interface.
func (p *Port) Set(arg string) error {
	port := atoi(arg)
	if atoi(arg) <= 0 || atoi(arg) > math.MaxUint16 {
		return errInvalidPort
	}
	*p = Port(port)
	return nil
}
