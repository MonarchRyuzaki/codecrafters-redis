package main

import (
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
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

type WatchState struct {
	state map[string]WatchStateValue
}

type WatchStateValue struct {
	value   string
	version int
}

type ConnState struct {
	watchState *WatchState
	tx         *TxState
}

var ConnHandlers = map[string]func(*ConnState, []Value) Value{
	"MULTI":   handleMulti,
	"EXEC":    handleExec,
	"DISCARD": handleDiscard,
	"WATCH":   handleWatch,
}

func handleMulti(cs *ConnState, args []Value) Value {
	if cs.tx.active {
		return Value{Type: ERROR, Str: "ERR MULTI calls can not be nested"}
	}
	cs.tx.active = true
	return Value{Type: STRING, Str: "OK"}
}

func handleExec(cs *ConnState, args []Value) Value {
	if !cs.tx.active {
		return Value{Type: ERROR, Str: "ERR EXEC without MULTI"}
	}
	if cs.tx.hasError {
		*cs.tx = TxState{}
		cs.watchState.state = make(map[string]WatchStateValue)
		return Value{Type: ERROR, Str: "EXECABORT Transaction discarded because of previous errors."}
	}

	results := make([]Value, 0, len(cs.tx.queue))
	for _, qcmd := range cs.tx.queue {
		var res Value
		if qcmd.name == "GET" {
			res = Handlers["GETWITHVERSION"](qcmd.args)
			key := qcmd.args[0].Bulk
			if watchVal, ok := cs.watchState.state[key]; ok {
				if res.Type != ARRAY || len(res.Array) != 2 ||
					res.Array[0].Bulk != watchVal.value ||
					res.Array[1].Num != watchVal.version {
					// Mismatch! Abort transaction.
					*cs.tx = TxState{}
					cs.watchState.state = make(map[string]WatchStateValue)
					return Value{Type: ARRAY, Array: []Value{{Type: BULK, Bulk: "$NULL$"}}}
				}
			}
			// Use only the bulk value for results
			res = res.Array[0]
		} else {
			handler := Handlers[qcmd.name]
			res = handler(qcmd.args)
		}
		results = append(results, res)
	}

	*cs.tx = TxState{}
	cs.watchState.state = make(map[string]WatchStateValue)
	return Value{Type: ARRAY, Array: results}
}

func handleDiscard(cs *ConnState, args []Value) Value {
	if !cs.tx.active {
		return Value{Type: ERROR, Str: "ERR DISCARD without MULTI"}
	}
	*cs.tx = TxState{}
	cs.watchState.state = make(map[string]WatchStateValue)
	return Value{Type: STRING, Str: "OK"}
}

func handleWatch(cs *ConnState, args []Value) Value {

	if cs.tx.active {
		return Value{Type: ERROR, Str: "ERR WATCH inside MULTI is not allowed"}
	}

	var wg sync.WaitGroup

	for _, x := range args {
		getHandler := Handlers["GETWITHVERSION"]
		wg.Add(1)
		go func(x Value) {
			defer wg.Done()
			res := getHandler([]Value{x})
			if res.Type == ARRAY && len(res.Array) == 2 {
				cs.watchState.state[x.Bulk] = WatchStateValue{
					value:   res.Array[0].Bulk,
					version: res.Array[1].Num,
				}
			}
		}(x)
	}
	wg.Wait()

	return Value{Type: STRING, Str: "OK"}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	resp := NewResp(conn)
	writer := NewWriter(conn)
	connState := &ConnState{
		watchState: &WatchState{
			state: make(map[string]WatchStateValue),
		},
		tx: &TxState{},
	}

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
			writer.Write(connHandler(connState, args))
			continue
		}

		if connState.tx.active {
			if _, ok := Handlers[command]; !ok {
				connState.tx.hasError = true
				writer.Write(Value{Type: ERROR, Str: "ERR unknown command '" + command + "'"})
			} else {
				connState.tx.queue = append(connState.tx.queue, QueuedCmd{command, args})
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
