package main

import (
	"io"
	"os"
	"sync"
)

type AOF struct {
	file *os.File
	mu   sync.Mutex
}

var aofManager *AOF = nil

func NewAOF() (*AOF, error) {
	if aofManager != nil {
		return aofManager, nil
	}
	p := getPersister()
	appendPath := p.dir + "/" + p.appendDirName

	if err := os.MkdirAll(appendPath, 0755); err != nil {
		return nil, err
	}

	appendFilePath := appendPath + "/" + p.appendFileName

	f, err := os.OpenFile(appendFilePath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}
	aofManager = &AOF{file: f}
	return aofManager, nil
}

func (a *AOF) GetAofManager() *AOF {
	return aofManager
}

func (a *AOF) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.file.Close()
}

// Write appends a RESP command to the file
func (a *AOF) Write(val Value) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	// Use your existing Writer logic to marshal the value to bytes
	writer := &Writer{}
	bytes := writer.marshalValue(val)

	_, err := a.file.Write(bytes)
	if err != nil {
		return err
	}

	// Ensure it hits the disk
	return a.file.Sync()
}

// ReadAndRestore reads the AOF file on startup and applies commands
func (a *AOF) ReadAndRestore() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	// Move cursor to the beginning of the file
	a.file.Seek(0, 0)

	// Wrap the file in your existing Resp parser
	respParser := NewResp(a.file)

	for {
		val, err := respParser.Read()
		if err != nil {
			if err == io.EOF {
				break // Reached end of file
			}
			return err
		}

		if val.Type != ARRAY || len(val.Array) == 0 {
			continue
		}

		// Re-execute the command to restore state
		command := val.Array[0].Bulk
		args := val.Array[1:]

		if handler, ok := Handlers[command]; ok {
			db.WithExecLock(func() {
				handler(args)
			})
		}
	}
	return nil
}
