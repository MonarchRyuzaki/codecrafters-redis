package main

import (
	"sync/atomic"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/data_structures"
)

const (
	STRING_    = "string"
	LIST       = "list"
	STREAM     = "stream"
	ATOMIC_INT = "a_int"
	ZSET       = "sorted_set"
	PUBSUB     = "pubsub"
)

type StringValue struct {
	Value       string
	EntryTime   time.Time
	IsPermanent bool
	ExitTime    time.Time
	Version     int
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

type ZsetValue struct {
	zset *data_structures.SortedSet
}

type PubSubValue struct {
	channels []chan Value
}
