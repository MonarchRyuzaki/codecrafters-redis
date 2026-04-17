package main

import "fmt"

type ServerInfo struct {
	role               string
	master_replid      string
	master_repl_offset int
}

var serverInfo = ServerInfo{}

func NewServerInfo(role string) *ServerInfo {
	serverInfo.role = role
	serverInfo.master_replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
	serverInfo.master_repl_offset = 0
	return &serverInfo
}

var ServerHandler = map[string]func(*ServerInfo, []Value) Value{
	"INFO": handleInfo,
}

func handleInfo(s *ServerInfo, args []Value) Value {
	return Value{Type: BULK, Bulk: fmt.Sprintf("role:%s\nmaster_replid:%s\nmaster_repl_offset:%v", s.role, s.master_replid, s.master_repl_offset)}
}
