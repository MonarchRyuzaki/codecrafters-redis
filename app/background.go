package main

import "time"

func handleActiveDeletion() {
	for {
		time.Sleep(100 * time.Millisecond)
		db.CleanupExpired()
	}
}