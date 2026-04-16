package main

import (
	"fmt"
	"io"
	"net"
	"strings"
)

type QueuedCmd struct {
	name string
	args []Value
}

type TxState struct {
	active   bool
	queue    []QueuedCmd
	hasError bool
}

var ConnHandlers = map[string]func(*TxState, []Value) Value{
	"MULTI":   handleMulti,
	// "EXEC":    handleExec,
	// "DISCARD": handleDiscard,
}

func handleMulti(tx *TxState, args []Value) Value {
	if tx.active {
		return Value{Type: ERROR, Str: "ERR MULTI calls can not be nested"}
	}
	tx.active = true
	return Value{Type: STRING, Str: "OK"}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	resp := NewResp(conn)
	writer := NewWriter(conn)
	var tx TxState

	for {
		value, err := resp.Read()
		if err != nil {
			if err != io.EOF {
				fmt.Println("Error reading from client: ", err.Error())
			}
			break
		}

		if value.Type != ARRAY || len(value.Array) == 0 {
			fmt.Println("Invalid request")
			continue
		}

		command := strings.ToUpper(value.Array[0].Bulk)
		args := value.Array[1:]

		if connHandler, ok := ConnHandlers[command]; ok {
			writer.Write(connHandler(&tx, args))
			continue
		}

		handler, ok := Handlers[command]
		if !ok {
			fmt.Println("Unknown command: ", command)
			writer.Write(Value{Type: ERROR, Str: "ERR unknown command '" + command + "'"})
			continue
		}
		writer.Write(handler(args))
	}
}
