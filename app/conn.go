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
	isDirty  bool
}

type WatchState struct {
	state map[string]WatchStateValue
}

type WatchStateValue struct {
	value   string
	version int
}

type ConnState struct {
	watchState      *WatchState
	tx              *TxState
	isAuthenticated bool
}

var ConnHandlers = map[string]func(*ConnState, []Value) Value{
	"MULTI":   handleMulti,
	"EXEC":    handleExec,
	"DISCARD": handleDiscard,
	"WATCH":   handleWatch,
	"UNWATCH": handleUnwatch,
	"ACL":     handleAcl,
	"AUTH":    handleAuth,
}

var writeCommands = map[string]bool{
	"SET":            true,
	"DEL":            true,
	"SETWITHVERSION": true,
	"RPUSH":          true,
	"LPUSH":          true,
	"LPOP":           true,
	"BLPOP":          true,
	"XADD":           true,
	"INCR":           true,
}

var noExecLockCommands = map[string]bool{
	"BLPOP": true,
	"XREAD": true,
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

	db.execMu.Lock()
	defer db.execMu.Unlock()

	for key, watchVal := range cs.watchState.state {
		res := Handlers["GETWITHVERSION"]([]Value{{Type: BULK, Bulk: key}})
		if res.Type != ARRAY || len(res.Array) != 2 ||
			res.Array[0].Bulk != watchVal.value ||
			res.Array[1].Num != watchVal.version {
			*cs.tx = TxState{}
			cs.watchState.state = make(map[string]WatchStateValue)
			return Value{Type: ARRAY, Array: []Value{{Type: BULK, Bulk: "$NULL$"}}}
		}
	}

	results := make([]Value, 0, len(cs.tx.queue))
	for _, qcmd := range cs.tx.queue {
		handler := Handlers[qcmd.name]
		res := handler(qcmd.args)
		results = append(results, res)
	}
	queueToPropagate := cs.tx.queue
	*cs.tx = TxState{}
	cs.watchState.state = make(map[string]WatchStateValue)

	go propagateTransaction(queueToPropagate)

	return Value{Type: ARRAY, Array: results}
}

func propagateTransaction(queue []QueuedCmd) {
	if serverInfo.role != "master" {
		return
	}

	hasWrite := false
	for _, cmd := range queue {
		if writeCommands[cmd.name] {
			hasWrite = true
			break
		}
	}

	if !hasWrite {
		return
	}

	serverInfo.propagateCh <- []Value{{Type: BULK, Bulk: "MULTI"}}

	for _, cmd := range queue {
		if writeCommands[cmd.name] {
			fullCmd := append([]Value{{Type: BULK, Bulk: cmd.name}}, cmd.args...)
			serverInfo.propagateCh <- fullCmd
		}
	}

	serverInfo.propagateCh <- []Value{{Type: BULK, Bulk: "EXEC"}}
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

	for _, x := range args {
		getHandler := Handlers["GETWITHVERSION"]
		res := getHandler([]Value{x})
		if res.Type == ARRAY && len(res.Array) == 2 {
			cs.watchState.state[x.Bulk] = WatchStateValue{
				value:   res.Array[0].Bulk,
				version: res.Array[1].Num,
			}
		}
	}

	return Value{Type: STRING, Str: "OK"}
}

func handleUnwatch(cs *ConnState, args []Value) Value {
	if len(args) != 0 {
		return Value{Type: ERROR, Str: "ERR Incorrect number of arguments for UNWATCH command"}
	}
	cs.watchState.state = make(map[string]WatchStateValue)
	return Value{Type: STRING, Str: "OK"}
}

func handleAcl(cs *ConnState, args []Value) Value {
	if len(args) < 1 {
		return Value{Type: ERROR, Str: "ERR wrong number of arguments for 'ACL' command"}
	}

	subCommand := strings.ToUpper(args[0].Bulk)
	switch subCommand {
	case "WHOAMI":
		return Value{Type: BULK, Bulk: GetUser().username}
	case "GETUSER":
		if len(args) < 2 {
			return Value{Type: ERROR, Str: "ERR wrong number of arguments for 'ACL GETUSER' command"}
		}

		targetUser := args[1].Bulk
		u := GetUser()

		if u == nil || u.username != targetUser {
			return Value{Type: BULK, Bulk: "$NULL$"}
		}

		flagsArray := make([]Value, len(u.Flags))
		for i, flag := range u.Flags {
			flagsArray[i] = Value{Type: BULK, Bulk: flag}
		}

		passwordsArray := make([]Value, len(u.Passwords))
		for i, pwd := range u.Passwords {
			passwordsArray[i] = Value{Type: BULK, Bulk: pwd}
		}

		return Value{
			Type: ARRAY,
			Array: []Value{
				{Type: BULK, Bulk: "flags"},
				{Type: ARRAY, Array: flagsArray},
				{Type: BULK, Bulk: "passwords"},
				{Type: ARRAY, Array: passwordsArray},
			},
		}
	case "SETUSER":
		if len(args) < 2 {
			return Value{Type: ERROR, Str: "ERR wrong number of arguments for 'ACL SETUSER' command"}
		}

		targetUser := args[1].Bulk
		u := GetUser()

		if u == nil || u.username != targetUser {
			return Value{Type: ERROR, Str: "ERR user does not exist"}
		}

		for i := 2; i < len(args); i++ {
			rule := args[i].Bulk

			if strings.HasPrefix(rule, ">") {
				plainPassword := strings.TrimPrefix(rule, ">")
				u.setPassword(plainPassword)

			} else if rule == "nopass" {
				u.setNoPass()
			}
		}

		return Value{Type: STRING, Str: "OK"}
	default:
		return Value{}
	}
}

func handleAuth(cs *ConnState, args []Value) Value {
	if len(args) < 2 {
		return Value{Type: ERROR, Str: "ERR wrong number of arguments for 'ACL' command"}
	}

	targetUser := args[0].Bulk
	targetPassword := args[1].Bulk

	success := GetUser().authenticate(targetUser, targetPassword)

	cs.isAuthenticated = success
	if !success {
		return Value{Type: ERROR, Str: "WRONGPASS invalid username-password pair or user is disabled."}
	}
	return Value{Type: STRING, Str: "OK"}
}

func handleConnection(conn net.Conn, isMasterStream bool, resp *Resp, writer *Writer) {
	defer conn.Close()
	defer func() {
		serverInfo.mu.Lock()
		delete(serverInfo.replicaInfo, conn.RemoteAddr().String())
		serverInfo.mu.Unlock()
	}()

	u := NewUserAuth()
	connState := &ConnState{
		watchState: &WatchState{
			state: make(map[string]WatchStateValue),
		},
		tx:              &TxState{},
		isAuthenticated: u.authenticateOnStartup(),
	}

	for {
		offsetBefore := resp.BytesRead()
		value, err := resp.Read()
		offsetAfter := resp.BytesRead()
		bytesRead := offsetAfter - offsetBefore

		if err != nil {
			if err != io.EOF {
				fmt.Println("Error reading from client: ", err.Error())
			}
			break
		}

		if value.Type != ARRAY || len(value.Array) == 0 {
			fmt.Println("Invalid request")
			if isMasterStream {
				serverInfo.master_repl_offset.Add(bytesRead)
			}
			continue
		}

		command := strings.ToUpper(value.Array[0].Bulk)
		args := value.Array[1:]

		if command == "AUTH" {
			writer.Write(handleAuth(connState, args))
			continue
		}

		if !connState.isAuthenticated {
			writer.Write(Value{Type: ERROR, Str: "NOAUTH Authentication required."})
		}

		aofManager := GetAofManager()
		if aofManager != nil && writeCommands[command] {
			fullCmd := append([]Value{{Type: BULK, Bulk: command}}, args...)
			aofManager.Write(Value{Type: ARRAY, Array: fullCmd})
		}

		if connHandler, ok := ConnHandlers[command]; ok {
			if !isMasterStream {
				writer.Write(connHandler(connState, args))
			}
			continue
		}

		if connState.tx.active {
			if _, ok := Handlers[command]; !ok {
				connState.tx.hasError = true
				if !isMasterStream {
					writer.Write(Value{Type: ERROR, Str: "ERR unknown command '" + command + "'"})
				}
			} else {
				connState.tx.queue = append(connState.tx.queue, QueuedCmd{command, args})
				if !isMasterStream {
					writer.Write(Value{Type: STRING, Str: "QUEUED"})
				}
			}
			continue
		}

		if servInfoHandler, ok := ServerHandler[command]; ok {
			res := servInfoHandler(&serverInfo, conn, args, resp, writer)
			// For master stream, we respond to REPLCONF GETACK
			if isMasterStream && command == "REPLCONF" && len(args) > 0 && strings.ToUpper(args[0].Bulk) == "GETACK" {
				writer.Write(res)
			} else if !isMasterStream {
				writer.Write(res)
			}
		} else if persHandler, ok := PersistanceHandler[command]; ok {
			if !isMasterStream {
				writer.Write(persHandler(getPersister(), conn, args, resp, writer))
			}
		} else {
			handler, ok := Handlers[command]
			if !ok {
				fmt.Println("Unknown command: ", command)
				if !isMasterStream {
					writer.Write(Value{Type: ERROR, Str: "ERR unknown command '" + command + "'"})
				}
			} else {
				if noExecLockCommands[command] {
					if !isMasterStream {
						writer.Write(handler(args))
					} else {
						handler(args)
					}
				} else {
					var result Value
					db.WithExecLock(func() {
						result = handler(args)
					})
					if !isMasterStream {
						writer.Write(result)
					}
				}
			}
		}

		if isMasterStream {
			serverInfo.master_repl_offset.Add(bytesRead)
		}

		if serverInfo.role == "master" && writeCommands[command] {
			fullCmd := append([]Value{{Type: BULK, Bulk: command}}, args...)
			serverInfo.propagateCh <- fullCmd
		}
	}
}
