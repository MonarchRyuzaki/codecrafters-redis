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
	"EXEC":    handleExec,
	"DISCARD": handleDiscard,
}

func handleMulti(tx *TxState, args []Value) Value {
	if tx.active {
		return Value{Type: ERROR, Str: "ERR MULTI calls can not be nested"}
	}
	tx.active = true
	return Value{Type: STRING, Str: "OK"}
}

func handleExec(tx *TxState, args []Value) Value {
	if !tx.active {
		return Value{Type: ERROR, Str: "ERR EXEC without MULTI"}
	}
	if tx.hasError {
		*tx = TxState{}
		return Value{Type: ERROR, Str: "EXECABORT Transaction discarded because of previous errors."}
	}

	results := make([]Value, 0, len(tx.queue))
	for _, qcmd := range tx.queue {
		handler := Handlers[qcmd.name] 
		results = append(results, handler(qcmd.args))
	}

	*tx = TxState{}
	return Value{Type: ARRAY, Array: results}
}

func handleDiscard(tx *TxState, args []Value) Value {
	if !tx.active {
		return Value{Type: ERROR, Str: "ERR DISCARD without MULTI"}
	}
	*tx = TxState{}
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

		if tx.active {
			if _, ok := Handlers[command]; !ok {
				tx.hasError = true
				writer.Write(Value{Type: ERROR, Str: "ERR unknown command '" + command + "'"})
			} else {
				tx.queue = append(tx.queue, QueuedCmd{command, args})
				writer.Write(Value{Type: STRING, Str: "QUEUED"})
			}
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
