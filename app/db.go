package main

import "sync"

type DB struct {
	mu   sync.RWMutex
	data map[string]string
}

func NewDB() *DB {
	return &DB{
		data: make(map[string]string),
	}
}


var db = NewDB()

func (db *DB) Set(key, value string) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.data[key] = value
}

func (db *DB) Get(key string) (string, bool) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	val, ok := db.data[key]
	return val, ok
}
