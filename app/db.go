package main

import (
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/data_structures"
)

type DB struct {
	mu                   sync.RWMutex
	execMu               sync.Mutex
	mmap                 map[string]MapValue
	blockingClients      map[string][]*BlockingTicket
	blockingClientStream map[string][]*BlockingTicketStream
}

func NewDB() *DB {
	return &DB{
		mmap:                 make(map[string]MapValue),
		blockingClients:      make(map[string][]*BlockingTicket),
		blockingClientStream: make(map[string][]*BlockingTicketStream),
	}
}

var db = NewDB()

func (db *DB) WithExecLock(fn func()) {
	db.execMu.Lock()
	defer db.execMu.Unlock()
	fn()
}

func (db *DB) Set(key string, value string, ttl time.Duration, isPermanent bool) {
	db.mu.Lock()
	defer db.mu.Unlock()
	val, ok := db.mmap[key]
	newVersion := 1
	strValue, _ := val.Value.(StringValue)
	if ok {
		newVersion = strValue.Version + 1
	}
	db.mmap[key] = MapValue{
		Type: STRING_,
		Value: StringValue{
			Value:       value,
			EntryTime:   time.Now(),
			ExitTime:    time.Now().Add(ttl),
			IsPermanent: isPermanent,
			Version:     newVersion,
		},
	}
}

func (db *DB) SetWithVersion(key string, value string, version int, ttl time.Duration, isPermanent bool) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	val, ok := db.mmap[key]
	currentVersion := 0
	if ok {
		if val.Type != STRING_ {
			return errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		sv := val.Value.(StringValue)
		currentVersion = sv.Version
	}

	if currentVersion != version {
		return fmt.Errorf("version mismatch: expected %d, got %d", version, currentVersion)
	}

	newVersion := currentVersion + 1
	db.mmap[key] = MapValue{
		Type: STRING_,
		Value: StringValue{
			Value:       value,
			EntryTime:   time.Now(),
			ExitTime:    time.Now().Add(ttl),
			IsPermanent: isPermanent,
			Version:     newVersion,
		},
	}
	return nil
}

func (db *DB) Get(key string) (MapValue, bool) {
	db.mu.Lock()
	val, ok := db.mmap[key]
	db.mu.Unlock()

	if ok && val.Type == STRING_ {
		sv, oksv := val.Value.(StringValue)
		if oksv && !sv.IsPermanent && time.Now().After(sv.ExitTime) {
			db.Erase(key)
			return MapValue{}, false
		}
	}
	return val, ok
}

func (db *DB) GetWithVersion(key string) (string, int, bool) {
	db.mu.Lock()
	val, ok := db.mmap[key]
	db.mu.Unlock()

	if !ok {
		return "", 0, false
	}

	if val.Type == STRING_ {
		sv, oksv := val.Value.(StringValue)
		if oksv && !sv.IsPermanent && time.Now().After(sv.ExitTime) {
			db.Erase(key)
			return "", 0, false
		}
		return sv.Value, sv.Version, true
	}

	if val.Type == ATOMIC_INT {
		av, ok := val.Value.(AtomicIntegerValue)
		if ok {
			return strconv.FormatInt(av.Value.Load(), 10), 0, true
		}
	}

	return "", 0, false
}

func (db *DB) CleanupExpired() {
	db.mu.Lock()
	defer db.mu.Unlock()

	now := time.Now()
	for key, val := range db.mmap {
		if val.Type == STRING_ {
			sv, ok := val.Value.(StringValue)
			if ok && !sv.IsPermanent && now.After(sv.ExitTime) {
				delete(db.mmap, key)
			}
		}
	}
}

func (db *DB) Erase(key string) {
	db.mu.Lock()
	defer db.mu.Unlock()
	delete(db.mmap, key)
}

func (db *DB) RPUSH(key string, items []string) (int, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	if val, ok := db.mmap[key]; ok && val.Type != LIST {
		return -1, errors.New("ERR Existing Key is not a List")
	}

	currentLength := 0
	if val, ok := db.mmap[key]; ok && val.Type == LIST {
		existingList, _ := val.Value.(ListValue)
		currentLength = len(existingList.Value)
	}
	expectedLength := currentLength + len(items)

	remainingItems := items
	// 1. Try to fulfill waiting clients first
	for len(remainingItems) > 0 && len(db.blockingClients[key]) > 0 {
		waiter := db.blockingClients[key][0]
		db.blockingClients[key] = db.blockingClients[key][1:]

		if waiter.Active.CompareAndSwap(0, 1) {
			waiter.ValueChan <- remainingItems[0]
			remainingItems = remainingItems[1:]
		}
	}

	// 2. Add any leftover items to the actual list
	if len(remainingItems) > 0 {
		var list []string
		if val, ok := db.mmap[key]; ok && val.Type == LIST {
			existingList, _ := val.Value.(ListValue)
			list = append(existingList.Value, remainingItems...)
		} else {
			list = append([]string{}, remainingItems...)
		}

		db.mmap[key] = MapValue{
			Type: LIST,
			Value: ListValue{
				Value: list,
			},
		}
	}

	return expectedLength, nil
}

func (db *DB) LRANGE(key string, start int, end int) []string {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if val, ok := db.mmap[key]; ok && val.Type != LIST {
		return []string{}
	}

	var list []string
	if val, ok := db.mmap[key]; ok && val.Type == LIST {
		existingList, _ := val.Value.(ListValue)
		l := len(existingList.Value)
		if start < 0 {
			start = max(0, l+start)
		}
		if end < 0 {
			end = l + end
		}
		if start <= end && start < l {
			list = existingList.Value[start : min(end, l-1)+1]
		}
	}

	return list
}

func (db *DB) LPUSH(key string, items []string) (int, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	if val, ok := db.mmap[key]; ok && val.Type != LIST {
		return -1, errors.New("ERR Existing Key is not a List")
	}

	currentLength := 0
	if val, ok := db.mmap[key]; ok && val.Type == LIST {
		existingList, _ := val.Value.(ListValue)
		currentLength = len(existingList.Value)
	}
	expectedLength := currentLength + len(items)

	remainingItems := items
	// 1. Try to fulfill waiting clients first
	for len(remainingItems) > 0 && len(db.blockingClients[key]) > 0 {
		waiter := db.blockingClients[key][0]
		db.blockingClients[key] = db.blockingClients[key][1:]

		if waiter.Active.CompareAndSwap(0, 1) {
			waiter.ValueChan <- remainingItems[0]
			remainingItems = remainingItems[1:]
		}
	}

	// 2. Add remaining to list
	if len(remainingItems) > 0 {
		var list []string
		if val, ok := db.mmap[key]; ok && val.Type == LIST {
			existingList, _ := val.Value.(ListValue)
			list = append([]string{}, remainingItems...)
			list = append(list, existingList.Value...)
		} else {
			list = append([]string{}, remainingItems...)
		}

		db.mmap[key] = MapValue{
			Type: LIST,
			Value: ListValue{
				Value: list,
			},
		}
	}

	return expectedLength, nil
}

func (db *DB) LLEN(key string) (int, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if val, ok := db.mmap[key]; ok && val.Type != LIST {
		return -1, errors.New("ERR Existing Key is not a List")
	}

	length := 0
	if val, ok := db.mmap[key]; ok && val.Type == LIST {
		existingList, _ := val.Value.(ListValue)
		length = len(existingList.Value)
	}

	return length, nil
}

func (db *DB) LPOP(key string, cnt int) ([]string, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if val, ok := db.mmap[key]; ok && val.Type != LIST {
		return []string{}, errors.New("ERR Existing Key is not a List")
	}

	var item []string
	if val, ok := db.mmap[key]; ok && val.Type == LIST {
		existingList, _ := val.Value.(ListValue)
		item = existingList.Value[0:cnt]
		existingList.Value = existingList.Value[cnt:]

		db.mmap[key] = MapValue{
			Type:  LIST,
			Value: existingList,
		}
	}

	return item, nil
}

func (db *DB) BLPOP(key string, timeout float64) (string, bool) {
	db.mu.Lock()

	// 1. Immediate check: if list exists and has items, pop and return immediately
	if val, ok := db.mmap[key]; ok && val.Type == LIST {
		existingList, _ := val.Value.(ListValue)
		if len(existingList.Value) > 0 {
			item := existingList.Value[0]
			existingList.Value = existingList.Value[1:]
			db.mmap[key] = MapValue{
				Type:  LIST,
				Value: existingList,
			}
			db.mu.Unlock()
			return item, true
		}
	}

	// 2. Register a blocking ticket
	ticket := &BlockingTicket{
		ValueChan: make(chan string, 1),
	}
	db.blockingClients[key] = append(db.blockingClients[key], ticket)
	db.mu.Unlock()

	// 3. Block until value received or timeout
	var timeoutChan <-chan time.Time
	if timeout > 0 {
		timer := time.NewTimer(time.Duration(timeout * float64(time.Second)))
		defer timer.Stop()
		timeoutChan = timer.C
	}

	select {
	case item := <-ticket.ValueChan:
		return item, true
	case <-timeoutChan:
		// Attempt to mark as TimedOut (2) to prevent pusher from sending
		if ticket.Active.CompareAndSwap(0, 2) {
			return "", false
		}
		// If CAS failed, it means pusher already won. Must receive the value.
		item := <-ticket.ValueChan
		return item, true
	}
}

func (db *DB) TYPE(key string) string {
	db.mu.RLock()
	defer db.mu.RUnlock()
	val, ok := db.mmap[key]
	if !ok {
		return "none"
	}
	return val.Type
}

func (db *DB) XADD(key string, id string, streamValue map[string]string) (string, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	var stream StreamValue
	if val, ok := db.mmap[key]; ok {
		if val.Type != STREAM {
			return "", errors.New("ERR Existing Key is not a stream")
		}
		stream = val.Value.(StreamValue)
	}

	var lastID string
	if len(stream.Entries) > 0 {
		lastID = stream.Entries[len(stream.Entries)-1].ID
	}

	newID, err := generateAndValidateStreamID(id, lastID)
	if err != nil {
		return "", err
	}

	stream.Entries = append(stream.Entries, StreamEntry{
		ID:     newID,
		Fields: streamValue,
	})

	db.mmap[key] = MapValue{
		Type:  STREAM,
		Value: stream,
	}

	for _, x := range db.blockingClientStream[key] {
		if x.LastID > id {
			continue
		}
		if x.Active.CompareAndSwap(0, 1) {
			x.ValueChan <- stream.Entries[len(stream.Entries)-1]
		}
	}

	return newID, nil
}

func generateAndValidateStreamID(requestedID string, lastID string) (string, error) {
	var lastMs, lastSeq int64
	if lastID != "" {
		parts := strings.Split(lastID, "-")
		lastMs, _ = strconv.ParseInt(parts[0], 10, 64)
		lastSeq, _ = strconv.ParseInt(parts[1], 10, 64)
	}

	var ms, seq int64
	var err error

	if requestedID == "*" {
		ms = time.Now().UnixMilli()
		if ms < lastMs {
			ms = lastMs
		}
		if ms == lastMs {
			seq = lastSeq + 1
		} else {
			if ms == 0 {
				seq = 1
			} else {
				seq = 0
			}
		}
		return fmt.Sprintf("%d-%d", ms, seq), nil
	}

	if strings.HasSuffix(requestedID, "-*") {
		msPart := strings.TrimSuffix(requestedID, "-*")
		ms, err = strconv.ParseInt(msPart, 10, 64)
		if err != nil {
			return "", errors.New("ERR invalid stream ID format")
		}

		if ms < lastMs {
			return "", errors.New("ERR The ID specified in XADD is equal or smaller than the target stream top item")
		}

		if ms == lastMs {
			seq = lastSeq + 1
		} else {
			if ms == 0 {
				seq = 1
			} else {
				seq = 0
			}
		}
		return fmt.Sprintf("%d-%d", ms, seq), nil
	}

	parts := strings.Split(requestedID, "-")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", errors.New("ERR invalid stream ID format")
	}

	ms, err = strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return "", errors.New("ERR invalid stream ID format")
	}
	seq, err = strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return "", errors.New("ERR invalid stream ID format")
	}

	if ms == 0 && seq == 0 {
		return "", errors.New("ERR The ID specified in XADD must be greater than 0-0")
	}

	if lastID != "" {
		if ms < lastMs || (ms == lastMs && seq <= lastSeq) {
			return "", errors.New("ERR The ID specified in XADD is equal or smaller than the target stream top item")
		}
	}

	return requestedID, nil
}

func parseStreamID(id string) (int64, int64, error) {
	if id == "-" {
		return 0, 0, nil
	}
	if id == "+" {
		// Using max int64 to represent the end of the stream
		return 9223372036854775807, 9223372036854775807, nil
	}
	parts := strings.Split(id, "-")
	ms, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, 0, err
	}
	var seq int64
	if len(parts) > 1 {
		seq, err = strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			return 0, 0, err
		}
	} else {
		seq = 0
	}
	return ms, seq, nil
}

func (db *DB) XRANGE(key, start, end string) (StreamValue, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	val, ok := db.mmap[key]
	if !ok {
		return StreamValue{}, nil
	}
	if val.Type != STREAM {
		return StreamValue{}, errors.New("ERR Existing Key is not a stream")
	}

	stream := val.Value.(StreamValue)
	var result StreamValue

	startMs, startSeq, err := parseStreamID(start)
	if err != nil {
		return StreamValue{}, err
	}
	endMs, endSeq, err := parseStreamID(end)
	if err != nil {
		return StreamValue{}, err
	}

	for _, entry := range stream.Entries {
		parts := strings.Split(entry.ID, "-")
		eMs, _ := strconv.ParseInt(parts[0], 10, 64)
		eSeq, _ := strconv.ParseInt(parts[1], 10, 64)

		if eMs < startMs || (eMs == startMs && eSeq < startSeq) {
			continue
		}
		if eMs > endMs || (eMs == endMs && eSeq > endSeq) {
			continue
		}
		result.Entries = append(result.Entries, entry)
	}

	return result, nil
}

func (db *DB) XReadStream(key, lastID string, timeout int) (StreamValue, error) {
	db.mu.RLock()
	val, ok := db.mmap[key]
	if ok && val.Type == STREAM {
		if lastID == "$" {
			lastID = val.Value.(StreamValue).Entries[len(val.Value.(StreamValue).Entries)-1].ID
		}
	}
	db.mu.RUnlock()
	if !ok {
		return StreamValue{}, nil
	}
	if val.Type != STREAM {
		return StreamValue{}, errors.New("ERR Existing Key is not a stream")
	}

	stream := val.Value.(StreamValue)
	var result StreamValue

	lastMs, lastSeq, err := parseStreamID(lastID)
	if err != nil {
		return StreamValue{}, err
	}

	for _, entry := range stream.Entries {
		parts := strings.Split(entry.ID, "-")
		eMs, _ := strconv.ParseInt(parts[0], 10, 64)
		eSeq, _ := strconv.ParseInt(parts[1], 10, 64)

		if eMs > lastMs || (eMs == lastMs && eSeq > lastSeq) {
			result.Entries = append(result.Entries, entry)
		}
	}

	if len(result.Entries) == 0 && timeout >= 0 {
		ticket := &BlockingTicketStream{
			LastID:    lastID,
			ValueChan: make(chan StreamEntry, 1),
		}
		db.mu.Lock()
		db.blockingClientStream[key] = append(db.blockingClientStream[key], ticket)
		db.mu.Unlock()

		var timeoutChan <-chan time.Time
		if timeout > 0 {
			timer := time.NewTimer(time.Duration(timeout) * time.Millisecond)
			defer timer.Stop()
			timeoutChan = timer.C
		}

		for {
			select {
			case item := <-ticket.ValueChan:
				return StreamValue{Entries: []StreamEntry{item}}, nil
			case <-timeoutChan:
				if ticket.Active.CompareAndSwap(0, 2) {
					return StreamValue{}, nil
				}
				item := <-ticket.ValueChan
				return StreamValue{Entries: []StreamEntry{item}}, nil
			}
		}
	}

	return result, nil
}

func (db *DB) INCR(key string) (int, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	val, ok := db.mmap[key]
	if !ok {
		atomicInt := &atomic.Int64{}
		atomicInt.Store(1)
		db.mmap[key] = MapValue{
			Type: ATOMIC_INT,
			Value: AtomicIntegerValue{
				Value: atomicInt,
			},
		}
		return 1, nil
	}

	if val.Type == ATOMIC_INT {
		atomicVal, _ := val.Value.(AtomicIntegerValue)
		newVal := atomicVal.Value.Add(1)
		return int(newVal), nil
	}

	if val.Type != STRING_ {
		return 0, errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	strValue, ok := val.Value.(StringValue)
	if !ok {
		return 0, errors.New("ERR value is not an integer or out of range")
	}

	ival, err := strconv.Atoi(strValue.Value)
	if err != nil {
		return 0, errors.New("ERR value is not an integer or out of range")
	}

	atomicInt := &atomic.Int64{}
	atomicInt.Store(int64(ival + 1))
	db.mmap[key] = MapValue{
		Type: ATOMIC_INT,
		Value: AtomicIntegerValue{
			Value: atomicInt,
		},
	}

	return ival + 1, nil
}

func (db *DB) Keys(pattern string) []string {
	db.mu.RLock()
	defer db.mu.RUnlock()

	var result []string
	now := time.Now()

	for key, val := range db.mmap {
		if val.Type == STRING_ {
			sv, ok := val.Value.(StringValue)
			if ok && !sv.IsPermanent && now.After(sv.ExitTime) {
				continue
			}
		}

		// Use Go's built-in glob matching, though only "*" pattern is checked
		matched, err := filepath.Match(pattern, key)
		if err == nil && matched {
			result = append(result, key)
		}
	}

	return result
}

func (db *DB) ZADD(key string, score float64, member string) (int, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	var zset ZsetValue
	if val, ok := db.mmap[key]; ok {
		if val.Type != ZSET {
			return 0, errors.New("ERR Existing Key is not a Sorted Set")
		}
		zset = val.Value.(ZsetValue)
	} else if !ok {
		zset = ZsetValue{
			zset: data_structures.NewSortedSet(),
		}
	}

	del := zset.zset.Add(score, member)

	db.mmap[key] = MapValue{
		Type:  ZSET,
		Value: zset,
	}

	return del, nil
}

func (db *DB) ZRANK(key, member string) (int, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	var zset ZsetValue
	if val, ok := db.mmap[key]; ok {
		if val.Type != ZSET {
			return -1, errors.New("ERR Existing Key is not a Sorted Set")
		}
		zset = val.Value.(ZsetValue)
	} else if !ok {
		return -1, nil
	}

	del := zset.zset.Rank(member)

	return del, nil
}

func (db *DB) ZRANGE(key string, start, end int) ([]string, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	var zset ZsetValue
	if val, ok := db.mmap[key]; ok {
		if val.Type != ZSET {
			return nil, errors.New("ERR Existing Key is not a Sorted Set")
		}
		zset = val.Value.(ZsetValue)
	} else if !ok {
		return []string{}, nil
	}

	nodes := zset.zset.GetRange(start, end)

	members := make([]string, len(nodes))

	for i, x := range nodes {
		members[i] = x.Member
	}
	

	return members, nil
}
