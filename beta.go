package main

import (
	"encoding/json"
	"sync"
)

type Message struct {
	data []byte
	count uint64
}

type CounterMap map[uint32]*Message

type Messages struct {
	cm CounterMap
	mtx sync.Mutex
}

func NewMap() *Messages {
	m := &Messages{
		cm: make(CounterMap),
		mtx: sync.Mutex{},
	}
	return m
}

func (m *Messages) sum(b []byte) uint32 {
	mi := make(map[string]interface{})
	if err := json.Unmarshal(b, &mi); err == nil {
		// If this is valid JSON we should have a "msg" or "message"
		// key in the map. If neither key exists, we take a sum of
		// entire byte slice as a fallback mechanism.
		if msg, ok := getValue("msg", mi); ok {
			return SumString32(msg)
		}
		if msg, ok := getValue("message", mi); ok {
			return SumString32(msg)
		}
	}
	return Sum32(b)
}

// Insert stores unique messages, and increments count for non-unique messages.
func (m *Messages) Insert(b []byte) bool {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	sum := m.sum(b)
	if _, ok := m.cm[sum]; !ok {
		m.cm[sum] = &Message{b, 0}
		return true
	}
	m.cm[sum].count++
	return false
}

// Iter is an iterator-like method for data in the duplicate messages map.
func (m *Messages) Iter() chan *Message {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	c := make(chan *Message, 1)
	go func() {
		for _, v := range m.cm {
			c <- v
		}
		close(c)
	return
	}()
	return c
}

// Reset clears the counter map structure.
func (m *Messages) Reset() {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	m.cm = make(CounterMap)
}
