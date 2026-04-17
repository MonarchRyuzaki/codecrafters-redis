package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"strings"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	port := flag.String("port", "6379", "Port to bind the server to")
	replicaof := flag.String("replicaof", "", "master server to connect to")
	flag.Parse()

	fmt.Println("Logs from your program will appear here!")

	role := "master"
	host, masterPort := "", ""

	if len(*replicaof) != 0 {
		role = "slave"
		parts := strings.Split(*replicaof, " ")
		if len(parts) >= 2 {
			host, masterPort = parts[0], parts[1]
		}
	}

	s := NewServerInfo(role, host, masterPort, *port)
	go s.startReplicator()

	l, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%s", *port))
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	// All background processes
	go handleActiveDeletion()

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go handleConnection(conn, false)
	}
}
