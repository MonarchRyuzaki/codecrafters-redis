package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"time"
)

const (
	RDB_OPCODE_EOF           = 255 // FF
	RDB_OPCODE_SELECTDB      = 254 // FE
	RDB_OPCODE_EXPIRETIME    = 253 // FD
	RDB_OPCODE_EXPIRETIME_MS = 252 // FC
	RDB_OPCODE_RESIZEDB      = 251 // FB
	RDB_OPCODE_AUX           = 250 // FA
)

// LoadRDB reads the binary RDB file and populates the database
func LoadRDB(filepath string, database *DB) error {
	file, err := os.Open(filepath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil 
		}
		return err
	}
	defer file.Close()

	reader := bufio.NewReader(file)

	// 1. Check Magic String ("REDIS") and Version
	magic := make([]byte, 9)
	if _, err := io.ReadFull(reader, magic); err != nil {
		return fmt.Errorf("failed to read RDB header: %v", err)
	}
	if string(magic[:5]) != "REDIS" {
		return fmt.Errorf("invalid RDB file format")
	}

	// 2. Parse the file byte by byte
	for {
		opcode, err := reader.ReadByte()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		switch opcode {
		case RDB_OPCODE_EOF:
			return nil // Reached the end of the file

		case RDB_OPCODE_SELECTDB:
			// Read the database index (usually 0) and ignore it
			_, _, _ = readLength(reader)

		case RDB_OPCODE_RESIZEDB:
			// Read hash table size and expiry hash table size (ignore for now)
			_, _, _ = readLength(reader)
			_, _, _ = readLength(reader)

		case RDB_OPCODE_AUX:
			// Read key and value for metadata and ignore
			_, _ = readString(reader)
			_, _ = readString(reader)

		case RDB_OPCODE_EXPIRETIME_MS:
			// Expiry in milliseconds (8 bytes, Little Endian)
			expireBytes := make([]byte, 8)
			io.ReadFull(reader, expireBytes)
			expiryMs := binary.LittleEndian.Uint64(expireBytes)
			expireTime := time.UnixMilli(int64(expiryMs))
			
			// Next byte is the value type
			valType, _ := reader.ReadByte()
			readAndStoreKeyValue(reader, database, valType, expireTime, false)

		case RDB_OPCODE_EXPIRETIME:
			// Expiry in seconds (4 bytes, Little Endian)
			expireBytes := make([]byte, 4)
			io.ReadFull(reader, expireBytes)
			expirySec := binary.LittleEndian.Uint32(expireBytes)
			expireTime := time.Unix(int64(expirySec), 0)
			
			// Next byte is the value type
			valType, _ := reader.ReadByte()
			readAndStoreKeyValue(reader, database, valType, expireTime, false)

		default:
			// If it's not a special opcode, it's a value type indicator for a key with NO expiry
			readAndStoreKeyValue(reader, database, opcode, time.Time{}, true)
		}
	}

	return nil
}

// readAndStoreKeyValue parses the actual key/value and injects it into your DB struct
func readAndStoreKeyValue(reader *bufio.Reader, database *DB, valType byte, exitTime time.Time, isPermanent bool) {
	// For this stage, valType is almost always 0 (String)
	if valType != 0 {
		// If it's a list/set/etc, we skip it for now to keep things simple
		return 
	}

	key, _ := readString(reader)
	value, _ := readString(reader)

	// Don't load expired keys into memory
	if !isPermanent && time.Now().After(exitTime) {
		return
	}

	database.mu.Lock()
	database.mmap[key] = MapValue{
		Type: STRING_,
		Value: StringValue{
			Value:       value,
			EntryTime:   time.Now(),
			ExitTime:    exitTime,
			IsPermanent: isPermanent,
			Version:     1, // Defaulting version to 1 on load
		},
	}
	database.mu.Unlock()
}

// readLength handles the tricky 2-bit prefix logic for lengths in Redis
func readLength(reader *bufio.Reader) (uint32, bool, error) {
	b, err := reader.ReadByte()
	if err != nil {
		return 0, false, err
	}

	// Read the first 2 bits
	encoding := (b & 0xC0) >> 6 

	switch encoding {
	case 0:
		// 00: Next 6 bits represent the length
		return uint32(b & 0x3F), false, nil
	case 1:
		// 01: Next 14 bits represent the length
		b2, _ := reader.ReadByte()
		len := (uint32(b&0x3F) << 8) | uint32(b2)
		return len, false, nil
	case 2:
		// 10: Discard remaining 6 bits, next 4 bytes are the length (Big Endian)
		lenBytes := make([]byte, 4)
		io.ReadFull(reader, lenBytes)
		return binary.BigEndian.Uint32(lenBytes), false, nil
	case 3:
		// 11: Special string encoding (integer as string)
		return uint32(b & 0x3F), true, nil
	default:
		return 0, false, fmt.Errorf("invalid length encoding")
	}
}

// readString parses strings based on the length encoding
func readString(reader *bufio.Reader) (string, error) {
	length, isEncoded, err := readLength(reader)
	if err != nil {
		return "", err
	}

	if isEncoded {
		// Handle integer-encoded strings (saves space in RDB)
		switch length {
		case 0: // 8-bit integer
			b, _ := reader.ReadByte()
			return fmt.Sprintf("%d", int8(b)), nil
		case 1: // 16-bit integer
			bBytes := make([]byte, 2)
			io.ReadFull(reader, bBytes)
			return fmt.Sprintf("%d", int16(binary.LittleEndian.Uint16(bBytes))), nil
		case 2: // 32-bit integer
			bBytes := make([]byte, 4)
			io.ReadFull(reader, bBytes)
			return fmt.Sprintf("%d", int32(binary.LittleEndian.Uint32(bBytes))), nil
		default:
			return "", fmt.Errorf("unknown string encoding")
		}
	}

	// Normal string
	strBytes := make([]byte, length)
	io.ReadFull(reader, strBytes)
	return string(strBytes), nil
}