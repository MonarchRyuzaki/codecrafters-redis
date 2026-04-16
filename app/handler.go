package main

import (
	"fmt"
	"strconv"
	"strings"
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
	"LPOP":   lpop,
	"BLPOP":  blpop,
	"TYPE":   type_,
	"XADD":   xadd,
	"XRANGE": xrange,
	"XREAD":  xread,
	"INCR":   incr,
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
		Type: STRING_,
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

	if val.Type == ATOMIC_INT {
		av, ok := val.Value.(AtomicIntegerValue)
		if !ok {
			return Value{Type: ERROR, Str: "ERR internal value type mismatch"}
		}
		return Value{Type: BULK, Bulk: strconv.FormatInt(av.Value.Load(), 10)}
	}

	if val.Type != STRING_ {
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
	if len(args) != 1 {
		return Value{Type: ERROR, Str: "ERR wrong number of arguments for 'llen' command"}
	}

	key := args[0].Bulk

	length, err := db.LLEN(key)
	if err != nil {
		return Value{Type: ERROR, Str: err.Error()}
	}

	return Value{Type: INTEGER, Num: length}
}

func lpop(args []Value) Value {
	if len(args) > 2 {
		return Value{Type: ERROR, Str: "ERR wrong number of arguments for 'lpop' command"}
	}

	key := args[0].Bulk
	cnt := 1
	if len(args) == 2 {
		x, err := strconv.Atoi(args[1].Bulk)
		if err != nil {
			return Value{Type: ERROR, Str: err.Error()}
		}
		cnt = x
	}

	item, err := db.LPOP(key, cnt)
	if err != nil {
		return Value{Type: ERROR, Str: err.Error()}
	}
	if len(item) == 1 {
		return Value{Type: BULK, Bulk: item[0]}
	}
	var values []Value
	for _, it := range item {
		values = append(values, Value{
			Type: BULK,
			Bulk: it,
		})
	}

	return Value{Type: ARRAY, Array: values}
}

func blpop(args []Value) Value {
	if len(args) != 2 {
		return Value{Type: ERROR, Str: "ERR wrong number of arguments for 'blpop' command"}
	}

	key := args[0].Bulk
	timeout, err := strconv.ParseFloat(args[1].Bulk, 64)
	if err != nil {
		return Value{Type: ERROR, Str: err.Error()}
	}

	item, ok := db.BLPOP(key, timeout)
	if !ok {
		return Value{Type: ARRAY, Array: []Value{
			{Type: BULK, Bulk: "$NULL$"},
		}}
	}

	return Value{Type: ARRAY, Array: []Value{
		{Type: BULK, Bulk: key},
		{Type: BULK, Bulk: item},
	}}
}

func type_(args []Value) Value {
	if len(args) != 1 {
		return Value{Type: ERROR, Str: "ERR wrong number of arguments for 'TYPE' command"}
	}

	key := args[0].Bulk

	str := db.TYPE(key)
	return Value{
		Type: STRING,
		Str:  str,
	}
}

func xadd(args []Value) Value {
	if len(args) < 2 {
		return Value{Type: ERROR, Str: "ERR wrong number of arguments for 'XADD' command"}
	}

	key := args[0].Bulk
	streamValue := make(map[string]string)
	id := args[1].Bulk
	for i := 2; i < len(args); i += 2 {
		if i+1 < len(args) {
			streamValue[args[i].Bulk] = args[i+1].Bulk
		}
	}

	returnedId, err := db.XADD(key, id, streamValue)

	if err != nil {
		return Value{Type: ERROR, Str: err.Error()}
	}

	return Value{Type: BULK, Bulk: returnedId}
}

func xrange(args []Value) Value {
	if len(args) != 3 {
		return Value{Type: ERROR, Str: "ERR wrong number of arguments for 'XRANGE' command"}
	}

	key := args[0].Bulk
	start := args[1].Bulk
	end := args[2].Bulk

	items, err := db.XRANGE(key, start, end)

	if err != nil {
		return Value{Type: ERROR, Str: err.Error()}
	}

	arr := make([]Value, len(items.Entries))
	for i, it := range items.Entries {
		var tempVal []Value
		for k, v := range it.Fields {
			tempVal = append(tempVal, Value{Type: BULK, Bulk: k})
			tempVal = append(tempVal, Value{Type: BULK, Bulk: v})
		}

		arr[i] = Value{
			Type: ARRAY,
			Array: []Value{
				{Type: BULK, Bulk: it.ID},
				{Type: ARRAY, Array: tempVal},
			},
		}
	}

	return Value{Type: ARRAY, Array: arr}
}

func xread(args []Value) Value {
	streamsIdx := -1
	for i, arg := range args {
		if strings.ToUpper(arg.Bulk) == "STREAMS" {
			streamsIdx = i
			break
		}
	}
	blockMs := -1
	if strings.ToUpper(args[0].Bulk) == "BLOCK" {
		ms, err := strconv.Atoi(args[1].Bulk)
		if err != nil {
			return Value{Type: ERROR, Str: err.Error()}
		}
		blockMs = ms
	}

	if streamsIdx == -1 {
		return Value{Type: ERROR, Str: "ERR syntax error"}
	}

	streamArgs := args[streamsIdx+1:]
	if len(streamArgs)%2 != 0 {
		return Value{Type: ERROR, Str: "ERR syntax error"}
	}

	n := len(streamArgs) / 2
	keys := streamArgs[:n]
	ids := streamArgs[n:]

	var result []Value
	for i := 0; i < n; i++ {
		key := keys[i].Bulk
		id := ids[i].Bulk

		items, err := db.XReadStream(key, id, blockMs)
		if err != nil {
			return Value{Type: ERROR, Str: err.Error()}
		}

		if len(items.Entries) == 0 {
			continue
		}

		var entries []Value
		for _, entry := range items.Entries {
			var fields []Value
			for k, v := range entry.Fields {
				fields = append(fields, Value{Type: BULK, Bulk: k})
				fields = append(fields, Value{Type: BULK, Bulk: v})
			}
			entries = append(entries, Value{
				Type: ARRAY,
				Array: []Value{
					{Type: BULK, Bulk: entry.ID},
					{Type: ARRAY, Array: fields},
				},
			})
		}

		result = append(result, Value{
			Type: ARRAY,
			Array: []Value{
				{Type: BULK, Bulk: key},
				{Type: ARRAY, Array: entries},
			},
		})
	}

	if len(result) == 0 {
		return Value{Type: ARRAY, Array: []Value{
			{Type: BULK, Bulk: "$NULL$"},
		}}
	}

	return Value{Type: ARRAY, Array: result}
}

func incr(args []Value) Value {
	if len(args) != 1 {
		return Value{Type: ERROR, Str: "ERR wrong number of arguments for 'INCR' command"}
	}

	key := args[0].Bulk

	val, err := db.INCR(key)
	if err != nil {
		return Value{Type: ERROR, Str: err.Error()}
	}

	return Value{Type: INTEGER, Num: val}
}
