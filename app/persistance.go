package main

import (
	"net"
	"strings"
)

type Persistance struct {
	dir            string
	dbFileName     string
	rdbPath        string
	appendOnly     string
	appendDirName  string
	appendFileName string
	appendFsync    string
}

var p *Persistance = nil

func getPersister(dir, dbFileName, appendOnly, appendDirName, appendFileName, appendFsync string) *Persistance {
	if p != nil {
		return p
	}
	p = &Persistance{
		dir:            dir,
		dbFileName:     dbFileName,
		rdbPath:        dir + "/" + dbFileName,
		appendOnly:     appendOnly,
		appendDirName:  appendDirName,
		appendFileName: appendFileName,
		appendFsync:    appendFsync,
	}
	return p
}

var PersistanceHandler = map[string]func(*Persistance, net.Conn, []Value, *Resp, *Writer) Value{
	"CONFIG": handleConfig,
}

func handleConfig(p *Persistance, conn net.Conn, args []Value, reader *Resp, writer *Writer) Value {
	if len(args) < 2 {
		return Value{Type: ERROR, Str: "ERR wrong number of arguments for 'config' command"}
	}

	subCommand := strings.ToUpper(args[0].Bulk)

	switch subCommand {
	case "GET":
		var responseArray []Value

		for _, arg := range args[1:] {
			param := strings.ToLower(arg.Bulk)

			switch param {
			case "dir":
				responseArray = append(responseArray,
					Value{Type: BULK, Bulk: "dir"},
					Value{Type: BULK, Bulk: p.dir},
				)
			case "dbfilename":
				responseArray = append(responseArray,
					Value{Type: BULK, Bulk: "dbfilename"},
					Value{Type: BULK, Bulk: p.dbFileName},
				)
			case "appendonly":
				responseArray = append(responseArray,
					Value{Type: BULK, Bulk: "appendonly"},
					Value{Type: BULK, Bulk: p.appendOnly},
				)
			case "appenddirname":
				responseArray = append(responseArray,
					Value{Type: BULK, Bulk: "appenddirname"},
					Value{Type: BULK, Bulk: p.appendDirName},
				)
			case "appendfilename":
				responseArray = append(responseArray,
					Value{Type: BULK, Bulk: "appendfilename"},
					Value{Type: BULK, Bulk: p.appendFileName},
				)
			case "appendfsync":
				responseArray = append(responseArray,
					Value{Type: BULK, Bulk: "appendfsync"},
					Value{Type: BULK, Bulk: p.appendFsync},
				)
			default:
				continue
			}
		}

		return Value{Type: ARRAY, Array: responseArray}

	default:
		return Value{Type: ERROR, Str: "ERR unsupported CONFIG subcommand"}
	}
}
