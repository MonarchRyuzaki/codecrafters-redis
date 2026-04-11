package main

var Handlers = map[string]func([]Value) Value{
	"PING": ping,
	"ECHO": echo,
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
