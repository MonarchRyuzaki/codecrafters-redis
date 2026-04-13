package main

import (
	"errors"
	"sync"
	"time"
)

type DB struct {
	mu   sync.RWMutex
	mmap map[string]MapValue
}

func NewDB() *DB {
	return &DB{
		mmap: make(map[string]MapValue),
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

	var list []string
	if val, ok := db.mmap[key]; ok && val.Type == LIST {
		existingList, _ := val.Value.(ListValue)
		list = append(existingList.Value, items...)
	} else {
		list = append([]string{}, items...)
	}

	db.mmap[key] = MapValue{
		Type: LIST,
		Value: ListValue{
			Value: list,
		},
	}

	return len(list), nil
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
		if (start < 0) {
			start = max(0, l + start)
		}
		if (end < 0) {
			end = l + end
		}
		if start <= end && start < l {
			list = existingList.Value[start:min(end, l-1)+1]
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

	var list []string
	if val, ok := db.mmap[key]; ok && val.Type == LIST {
		existingList, _ := val.Value.(ListValue)
		list = append([]string{}, items...)
		list = append(list, existingList.Value...)
	} else {
		list = append([]string{}, items...)
	}

	db.mmap[key] = MapValue{
		Type: LIST,
		Value: ListValue{
			Value: list,
		},
	}

	return len(list), nil
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