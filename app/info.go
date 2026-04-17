package main

import "fmt"

type ServerInfo struct {
	role string
}

var serverInfo = ServerInfo{}

func NewServerInfo(role string) *ServerInfo {
	serverInfo.role = role
	return &serverInfo
}

var ServerHandler = map[string]func(*ServerInfo, []Value) Value{
	"INFO": handleInfo,
}

func handleInfo(s *ServerInfo, args []Value) Value {
	return Value{Type: BULK, Bulk: fmt.Sprintf("role:%s", s.role)}
}
