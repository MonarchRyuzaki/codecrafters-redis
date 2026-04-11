package main

import (
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
	if !val.IsPermanent && time.Now().After(val.TimeToLive) {
		delete(db.mmap, key)
		val = MapValue{}
		ok = false
	}
	return val, ok
}

func (db *DB) CleanupExpired() {
	db.mu.Lock()
	defer db.mu.Unlock()

	now := time.Now()
	for key, val := range db.mmap {
		if !val.IsPermanent && now.After(val.TimeToLive) {
			delete(db.mmap, key)
		}
	}
}

func (db *DB) Erase(key string) {
	db.mu.Lock()
	defer db.mu.Unlock()
	delete(db.mmap, key)
}
