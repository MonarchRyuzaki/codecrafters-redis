package main

import (
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
)

type ServerInfo struct {
	role               string
	master_replid      string
	master_repl_offset int
	master_host        string
	master_port        string
	self_port          string
	replicaInfo        map[string]*ReplicaInfo
	mu                 sync.Mutex
	propagateCh        chan []Value
}

type ReplicaInfo struct {
	conn        net.Conn
	replicaPort string
	replicaCapa []string
}

var serverInfo = ServerInfo{}

func NewServerInfo(role string, host, masterPort string, selfPort string) *ServerInfo {
	serverInfo.role = role
	serverInfo.master_replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
	serverInfo.master_repl_offset = 0
	serverInfo.master_host = host
	serverInfo.master_port = masterPort
	serverInfo.self_port = selfPort
	serverInfo.replicaInfo = make(map[string]*ReplicaInfo)
	serverInfo.propagateCh = make(chan []Value, 100)

	go serverInfo.broadcaster()

	return &serverInfo
}

var ServerHandler = map[string]func(*ServerInfo, net.Conn, []Value) Value{
	"INFO":     handleInfo,
	"REPLCONF": handleReplConf,
	"PSYNC":    handlePsync,
}

func handleInfo(s *ServerInfo, conn net.Conn, args []Value) Value {
	return Value{Type: BULK, Bulk: fmt.Sprintf("role:%s\nmaster_replid:%s\nmaster_repl_offset:%v", s.role, s.master_replid, s.master_repl_offset)}
}

func handleReplConf(s *ServerInfo, conn net.Conn, args []Value) Value {
	connId := conn.RemoteAddr().String()
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.replicaInfo[connId]; !exists {
		s.replicaInfo[connId] = &ReplicaInfo{
			replicaCapa: []string{},
			conn:        conn,
		}
	}

	//TODO: Since the s.replicaInfo is stateful, we need to also check when a conn drops so we can delete the key
	subCommand := strings.ToUpper(args[0].Bulk)
	switch subCommand {
	case "LISTENING-PORT":
		if len(args) != 2 {
			return Value{Type: ERROR, Str: "Invalid Arguments for 'ReplConf' command"}
		}
		s.replicaInfo[connId].replicaPort = args[1].Bulk

	case "CAPA":
		if len(args) < 2 {
			return Value{Type: ERROR, Str: "Invalid Arguments for 'ReplConf' command"}
		}
		s.replicaInfo[connId].replicaCapa = append(s.replicaInfo[connId].replicaCapa, args[1].Bulk)
	case "GETACK":
		return Value{
			Type: ARRAY,
			Array: []Value{
				{Type: BULK, Bulk: "REPLCONF"},
				{Type: BULK, Bulk: "ACK"},
				{Type: BULK, Bulk: "0"},
			},
		}
	}

	return Value{Type: STRING, Str: "OK"}
}

func handlePsync(s *ServerInfo, conn net.Conn, args []Value) Value {
	if len(args) < 2 {
		return Value{Type: ERROR, Str: "Invalid Arguments for 'PSYNC' command"}
	}
	if args[0].Bulk == "?" && args[1].Bulk == "-1" {
		emptyRDBHex := "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"
		rdbBytes, _ := hex.DecodeString(emptyRDBHex)

		return Value{
			Type: STREAMS,
			Array: []Value{
				{
					Type: STRING,
					Str:  fmt.Sprintf("FULLRESYNC %s %v", s.master_replid, s.master_repl_offset),
				},
				{
					Type: RDB_FILE,
					Bulk: string(rdbBytes),
				},
			},
		}
	}
	return Value{}
}

func (s *ServerInfo) startReplicator() {
	if s.role == "master" {
		return
	}

	address := net.JoinHostPort(s.master_host, s.master_port)

	conn, err := net.Dial("tcp", address)

	if err != nil {
		fmt.Println("Failed to connect:", err)
		os.Exit(1)
	}
	defer conn.Close()

	resp := NewResp(conn)
	writer := NewWriter(conn)

	s.performReplicationHandshake(conn, resp, writer)
	handleConnection(conn, true, resp, writer)
}

func (s *ServerInfo) performReplicationHandshake(conn net.Conn, resp *Resp, writer *Writer) {
	writer.Write(Value{Type: ARRAY, Array: []Value{
		{
			Type: BULK,
			Bulk: "PING",
		},
	}})
	resp.Read()

	writer.Write(Value{Type: ARRAY, Array: []Value{
		{
			Type: BULK,
			Bulk: "REPLCONF",
		},
		{
			Type: BULK,
			Bulk: "listening-port",
		},
		{
			Type: BULK,
			Bulk: s.self_port,
		},
	}})
	resp.Read()

	writer.Write(Value{Type: ARRAY, Array: []Value{
		{
			Type: BULK,
			Bulk: "REPLCONF",
		},
		{
			Type: BULK,
			Bulk: "capa",
		},
		{
			Type: BULK,
			Bulk: "psync2",
		},
	}})
	resp.Read()

	writer.Write(Value{Type: ARRAY, Array: []Value{
		{
			Type: BULK,
			Bulk: "PSYNC",
		},
		{
			Type: BULK,
			Bulk: "?",
		},
		{
			Type: BULK,
			Bulk: "-1",
		},
	}})
	resp.Read()
	resp.reader.ReadByte() // Consume the '$'

	lengthLine, _, _ := resp.readLine() // Get the length string
	length, _ := strconv.Atoi(string(lengthLine))

	rdbBytes := make([]byte, length)
	io.ReadFull(resp.reader, rdbBytes) // Read exactly 'length' bytes
}

func (s *ServerInfo) broadcaster() {
	for cmdArgs := range s.propagateCh {
		val := Value{Type: ARRAY, Array: cmdArgs}

		s.mu.Lock()
		for _, replica := range s.replicaInfo {
			if replica.conn != nil {
				writer := NewWriter(replica.conn)
				writer.Write(val)
			}
		}
		s.mu.Unlock()
	}
}
