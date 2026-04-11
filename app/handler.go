package main

import "fmt"

var Handlers = map[string]func([]Value) Value{
	"PING": ping,
	"ECHO": echo,
	"SET":  set,
	"GET":  get,
}

func ping(args []Value) Value {
	if len(args) == 0 {
		return Value{Type: STRING, Str: "PONG"}
	}
	return Value{Type: STRING, Str: args[0].Bulk}
}

func echo(args []Value) Value {
	return Value{Type: BULK, Bulk: args[0].Bulk}
}

func set(args []Value) Value {
	if len(args) < 2 {
		return Value{Type: ERROR, Str: "ERR wrong number of arguments for 'set' command"}
	}

	key := args[0].Bulk
	value := args[1].Bulk

	db.Set(key, value)

	return Value{Type: STRING, Str: "OK"}
}

func get(args []Value) Value {
	if len(args) < 1 {
		return Value{Type: ERROR, Str: "ERR wrong number of arguments for 'get' command"}
	}

	key := args[0].Bulk

	val, ok := db.Get(key)

	if !ok {
		return Value{Type: BULK, Bulk: "$NULL$"}
	}

	return Value{Type: BULK, Bulk: val}
}
