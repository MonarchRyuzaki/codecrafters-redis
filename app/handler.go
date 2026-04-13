package main

import (
	"fmt"
	"strconv"
	"time"
)

var Handlers = map[string]func([]Value) Value{
	"PING":   ping,
	"ECHO":   echo,
	"SET":    set,
	"GET":    get,
	"RPUSH":  rpush,
	"LRANGE": lrange,
	"LPUSH":  lpush,
	"LLEN":   llen,
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
	ttl := time.Duration(0)
	isPermanent := true
	if len(args) >= 4 && args[2].Bulk == "PX" {
		ms, err := strconv.Atoi(args[3].Bulk)
		if err == nil && ms > 0 {
			ttl = time.Duration(ms) * time.Millisecond
			isPermanent = false
		}
	}

	db.Set(key, MapValue{
		Type: SET,
		Value: StringValue{
			Value:       value,
			EntryTime:   time.Now(),
			ExitTime:    time.Now().Add(ttl),
			IsPermanent: isPermanent,
		},
	})

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

	if val.Type != SET {
		return Value{Type: ERROR, Str: "WRONGTYPE Operation against a key holding the wrong kind of value"}
	}

	sv, ok := val.Value.(StringValue)
	if !ok {
		return Value{Type: ERROR, Str: "ERR internal value type mismatch"}
	}

	return Value{Type: BULK, Bulk: sv.Value}
}

func rpush(args []Value) Value {
	if len(args) < 2 {
		return Value{Type: ERROR, Str: "ERR wrong number of arguments for 'rpush' command"}
	}

	key := args[0].Bulk
	items := make([]string, 0, len(args)-1)
	for i := 1; i < len(args); i++ {
		items = append(items, args[i].Bulk)
	}

	length, err := db.RPUSH(key, items)
	if err != nil {
		return Value{Type: ERROR, Str: err.Error()}
	}

	return Value{Type: INTEGER, Num: length}
}

func lrange(args []Value) Value {
	if len(args) != 3 {
		return Value{Type: ERROR, Str: "ERR wrong number of arguments for 'lrange' command"}
	}

	key := args[0].Bulk
	start, err := strconv.Atoi(args[1].Bulk)
	if err != nil {
		return Value{Type: ERROR, Str: fmt.Sprintf("ERR %s", err.Error())}
	}
	end, err := strconv.Atoi(args[2].Bulk)
	if err != nil {
		return Value{Type: ERROR, Str: fmt.Sprintf("ERR %s", err.Error())}
	}

	items := db.LRANGE(key, start, end)

	var values []Value
	for _, it := range items {
		values = append(values, Value{
			Type: BULK,
			Bulk: it,
		})
	}

	return Value{Type: ARRAY, Array: values}
}

func lpush(args []Value) Value { 
	if len(args) < 2 {
		return Value{Type: ERROR, Str: "ERR wrong number of arguments for 'lpush' command"}
	}

	key := args[0].Bulk
	items := make([]string, 0, len(args)-1)
	for i := len(args) - 1; i > 0; i-- {
		items = append(items, args[i].Bulk)
	}

	length, err := db.LPUSH(key, items)
	if err != nil {
		return Value{Type: ERROR, Str: err.Error()}
	}

	return Value{Type: INTEGER, Num: length}
}

func llen(args []Value) Value {
	if (len(args) != 1) {
		return Value{Type: ERROR, Str: "ERR wrong number of arguments for 'llen' command"}
	}

	key := args[0].Bulk

	length, err := db.LLEN(key)
	if err != nil {
		return Value{Type: ERROR, Str: err.Error()}
	}

	return Value{Type: INTEGER, Num: length}
}