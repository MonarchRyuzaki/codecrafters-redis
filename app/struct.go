package main

import "time"

type MapValue struct {
	Value       string
	EntryTime   time.Time
	IsPermanent bool
	ExitTime  time.Time
}
