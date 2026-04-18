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
	cwd, err := os.Getwd()
	if err != nil {
		cwd = "."
	}
	dir := flag.String("dir", cwd, "The base directory where Redis stores its data files")
	dbFileName := flag.String("dbfilename", "rdbfile.rdb", " the name of the RDB file")
	appendOnly := flag.String("appendonly", "no", "Controls whether AOF persistence is enabled or disabled")
	appendDirName := flag.String("appenddirname", "appendonlydir", "The subdirectory under dir where AOF and manifest files are stored")
	appendFileName := flag.String("appendfilename", "appendonly.aof", "The name of the append-only file that records write operations")
	appendFsync := flag.String("appendfsync", "everysec", "How often buffered writes are flushed to the AOF file on disk")

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
	p := NewPersister(*dir, *dbFileName, *appendOnly, *appendDirName, *appendFileName, *appendFsync)
	err = LoadRDB(p.rdbPath, db)

	var aof *AOF
	if p.appendOnly == "yes" {
		aof, err = NewAOF()
		if err != nil {
			fmt.Println("Failed to open AOF:", err)
		}
		defer aof.Close()
		aof.ReadAndRestore()
	}

	if err != nil {
		fmt.Println("Error loading RDB:", err)
	}
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
		go handleConnection(conn, false, NewResp(conn), NewWriter(conn))
	}
}
