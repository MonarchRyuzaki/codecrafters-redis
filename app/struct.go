package main

import (
	"sync/atomic"
	"time"
)

const (
	STRING_ = "string"
	LIST    = "list"
	STREAM  = "stream"
	ATOMIC_INT = "a_int"
)

type StringValue struct {
	Value       string
	EntryTime   time.Time
	IsPermanent bool
	ExitTime    time.Time
}

type AtomicIntegerValue struct {
	Value *atomic.Int64
}

type ListValue struct {
	Value []string
}

type MapValue struct {
	Type  string
	Value interface{}
}

type BlockingTicket struct {
	// A channel that only RECEIVES a string (from the Pusher's perspective)
	// or only SENDS a string (from the Waiter's perspective)
	ValueChan chan string
	Active    atomic.Int32
}

type BlockingTicketStream struct {
	LastID    string
	ValueChan chan StreamEntry
	Active    atomic.Int32
}

type StreamEntry struct {
	ID     string
	Fields map[string]string
}

type StreamValue struct {
	Entries []StreamEntry
}
