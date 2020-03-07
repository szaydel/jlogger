package main

import (
	"hash"
	//"fmt"
	"hash/fnv"
)

type Message struct {
	data []byte
	count uint64
}

type CounterMap map[uint32]*Message

type Messages struct {
	h hash.Hash32
	cm CounterMap
}

func NewMap() *Messages {
	m := &Messages{
		h: fnv.New32a(),
		cm: make(CounterMap),
	}
	return m
}

func (m *Messages) Insert(b []byte) bool {
	m.h.Reset()
	m.h.Write(b)
	sum := m.h.Sum32()
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
