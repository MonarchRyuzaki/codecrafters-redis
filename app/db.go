package main

import (
	"errors"
	"sync"
	"time"
)

type DB struct {
	mu              sync.RWMutex
	mmap            map[string]MapValue
	blockingClients map[string][]*BlockingTicket
}

func NewDB() *DB {
	return &DB{
		mmap:            make(map[string]MapValue),
		blockingClients: make(map[string][]*BlockingTicket),
	}
}

var db = NewDB()

func (db *DB) Set(key string, value MapValue) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.mmap[key] = value
}

func (db *DB) Get(key string) (MapValue, bool) {
	db.mu.Lock()
	val, ok := db.mmap[key]
	db.mu.Unlock()

	if ok && val.Type == SET {
		sv, oksv := val.Value.(StringValue)
		if oksv && !sv.IsPermanent && time.Now().After(sv.ExitTime) {
			db.Erase(key)
			return MapValue{}, false
		}
	}
	return val, ok
}

func (db *DB) CleanupExpired() {
	db.mu.Lock()
	defer db.mu.Unlock()

	now := time.Now()
	for key, val := range db.mmap {
		if val.Type == SET {
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

func (db *DB) BLPOP(key string, timeout int) (string, bool) {
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
		timer := time.NewTimer(time.Duration(timeout) * time.Second)
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
