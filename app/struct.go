package main

import "time"

const (
	SET  = "SET"
	LIST = "LIST"
)

type StringValue struct {
	Value       string
	EntryTime   time.Time
	IsPermanent bool
	ExitTime    time.Time
}

type ListValue struct {
	Value []string
}

type MapValue struct {
	Type  string
	Value interface{}
}

