package main

import (
	"encoding/json"
)

type Message struct {
	data []byte
	count uint64
}

type CounterMap map[uint32]*Message

type Messages struct {
	cm CounterMap
	// mtx sync.
}

func NewMap() *Messages {
	m := &Messages{
		cm: make(CounterMap),
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

func (m *Messages) Insert(b []byte) bool {
	sum := m.sum(b)
	if _, ok := m.cm[sum]; !ok {
		m.cm[sum] = &Message{b, 1}
		return true
	}
	m.cm[sum].count++
	return false
}

func (m *Messages) Iter() chan *Message {
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

// func main() {
// 	m := NewMap()
// 	m.Insert([]byte("foo"))
// 	m.Insert([]byte("foo"))
// 	m.Insert([]byte("bar"))
// 	m.Insert([]byte("baz"))
// 	m.Insert([]byte("baz"))
// 	m.Insert([]byte("baz"))
// 	m.Insert([]byte("foo"))
// 	m.Insert([]byte("bar"))

// 	for v := range m.Iter() {
// 		fmt.Printf("%s => %d\n", v.data, v.count)
// 	}
// }
