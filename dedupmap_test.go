package main

import (
	"sync"
	"testing"
)

func TestNewMap(t *testing.T) {
	m := NewMap()
	for i := 0; i < 10; i++ {
		m.Insert([]byte("alpha"))
		m.Insert([]byte("beta"))
		m.Insert([]byte("gamma"))
		m.Insert([]byte("delta"))
		m.Insert([]byte("epsilon"))
	}
	if len(m.cm) != 5 {
		t.Errorf("NewMap() = %v, want length == 5", m)
	}
	for _, key := range []uint32{
		2847937341,
		2022730153,
		977130622,
		3876143916,
		4090937797,
	} {
		if _, ok := m.cm[key]; !ok {
			t.Errorf("expected key: %d to exist in map", key)
		}
	}
}

func TestMessages_Insert(t *testing.T) {
	type fields struct {
		cm  CounterMap
		mtx sync.Mutex
	}
	type args struct {
		b []byte
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
		{
			name:   "empty map should succeed",
			fields: fields{CounterMap{}, sync.Mutex{}},
			args:   args{b: []byte("alpha")},
			want:   true,
		},
		{
			name: "non-empty map should succeed",
			fields: fields{
				CounterMap{2847937341: &Message{data: []byte("alpha")}},
				sync.Mutex{},
			},
			args: args{b: []byte("beta")},
			want: true,
		},
		{
			name: "value exists in map",
			fields: fields{
				CounterMap{2847937341: &Message{data: []byte("alpha")}},
				sync.Mutex{},
			},
			args: args{b: []byte("alpha")},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &Messages{
				cm:  tt.fields.cm,
				mtx: tt.fields.mtx,
			}
			if got := m.Insert(tt.args.b); got != tt.want {
				t.Errorf("Messages.Insert() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMessages_Iter(t *testing.T) {

	m := NewMap()
	for i := 0; i < 10; i++ {
		m.Insert([]byte("alpha"))
		m.Insert([]byte("beta"))
		m.Insert([]byte("gamma"))
		m.Insert([]byte("delta"))
		m.Insert([]byte("epsilon"))
	}

	dataElems := map[string]struct{}{
		"alpha":   struct{}{},
		"beta":    struct{}{},
		"gamma":   struct{}{},
		"delta":   struct{}{},
		"epsilon": struct{}{},
	}

	for v := range m.Iter() {
		if v.count != 9 {
			t.Errorf("Messages.Iter() = %v, want %v", v.count, 9)
		}
		key := string(v.data)
		if _, ok := dataElems[key]; !ok {
			t.Errorf("Messages.Iter(); expected key: %s to exist in map", v.data)
		}
	}
}

func TestMessages_Reset(t *testing.T) {
	m := NewMap()
	for i := 0; i < 10; i++ {
		m.Insert([]byte("alpha"))
		m.Insert([]byte("beta"))
		m.Insert([]byte("gamma"))
		m.Insert([]byte("delta"))
		m.Insert([]byte("epsilon"))
	}
	if len(m.cm) == 0 {
		t.Errorf("Messages.Reset(); expected map to contain data before reset")
	}
	m.Reset()
	if len(m.cm) != 0 {
		t.Errorf("Messages.Reset(); expected map to contain no keys")
	}
}

func TestMessages_sum(t *testing.T) {
	m := NewMap()

	elems := [][]byte{
		[]byte("alpha"),
		[]byte("beta"),
		[]byte("gamma"),
		[]byte("delta"),
		[]byte("epsilon"),
		[]byte(`{"msg": "alpha"}`),
		[]byte(`{"msg": "beta"}`),
		[]byte(`{"msg": "gamma"}`),
		[]byte(`{"msg": "delta"}`),
		[]byte(`{"msg": "epsilon"}`),
		[]byte(`{"message": "alpha"}`),
		[]byte(`{"message": "beta"}`),
		[]byte(`{"message": "gamma"}`),
		[]byte(`{"message": "delta"}`),
		[]byte(`{"message": "epsilon"}`),
	}
	sums := []uint32{
		2847937341,
		2022730153,
		977130622,
		3876143916,
		4090937797,
	}
	for i, v := range elems {
		if sums[i%5] != m.sum(v) {
			t.Errorf("Messages.sum(): expected bytes %v to hash to: %d",
				v, m.sum(v))
		}
	}
}
