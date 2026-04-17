package main

import (
	"fmt"
	"net"
	"os"
)

type ServerInfo struct {
	role               string
	master_replid      string
	master_repl_offset int
	master_host        string
	master_port        string
}

var serverInfo = ServerInfo{}

func NewServerInfo(role string, host, port string) *ServerInfo {
	serverInfo.role = role
	serverInfo.master_replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
	serverInfo.master_repl_offset = 0
	serverInfo.master_host = host
	serverInfo.master_port = port
	return &serverInfo
}

var ServerHandler = map[string]func(*ServerInfo, []Value) Value{
	"INFO": handleInfo,
}

func handleInfo(s *ServerInfo, args []Value) Value {
	return Value{Type: BULK, Bulk: fmt.Sprintf("role:%s\nmaster_replid:%s\nmaster_repl_offset:%v", s.role, s.master_replid, s.master_repl_offset)}
}

func (s *ServerInfo) performReplicationHandshake() {
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

	// resp := NewResp(conn)
	writer := NewWriter(conn)

	writer.Write(Value{Type: ARRAY, Array: []Value{
		{
			Type: BULK,
			Bulk: "PING",
		},
	}})
}
