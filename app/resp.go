package main

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
)

const (
	STRING   = '+'
	ERROR    = '-'
	INTEGER  = ':'
	BULK     = '$'
	ARRAY    = '*'
	RDB_FILE = 'R'
	STREAMS  = 'S'
)

type Value struct {
	Type  byte
	Str   string
	Num   int
	Bulk  string
	Array []Value
}

type CountingReader struct {
	reader io.Reader
	offset int64
}

func (r *CountingReader) Read(p []byte) (n int, err error) {
	n, err = r.reader.Read(p)
	r.offset += int64(n)
	return n, err
}

type Resp struct {
	reader *bufio.Reader
	cr     *CountingReader
}

func NewResp(rd io.Reader) *Resp {
	cr := &CountingReader{reader: rd}
	return &Resp{
		reader: bufio.NewReader(cr),
		cr:     cr,
	}
}

func (r *Resp) BytesRead() int64 {
	// bufio.Reader might have buffered some bytes that haven't been "consumed" by our high-level Read calls.
	// But in Redis replication, the offset is usually calculated based on the bytes consumed from the stream.
	// However, bufio.Buffered() tells us how many bytes are currently in the buffer.
	// The number of bytes actually "read" by the application is cr.offset - r.reader.Buffered()
	return r.cr.offset - int64(r.reader.Buffered())
}

type Writer struct {
	writer io.Writer
}

func NewWriter(w io.Writer) *Writer {
	return &Writer{writer: w}
}

func (w *Writer) Write(v Value) error {
	if v.Type == STREAMS {
		for _, val := range v.Array {
			if err := w.Write(val); err != nil {
				return err
			}
		}
		return nil
	}
	var bytes []byte

	switch v.Type {
	case STRING:
		bytes = w.marshalString(v)
	case ERROR:
		bytes = w.marshalError(v)
	case INTEGER:
		bytes = w.marshalInt(v)
	case BULK:
		bytes = w.marshalBulk(v)
	case ARRAY:
		bytes = w.marshalArray(v)
	case RDB_FILE:
		bytes = w.marshalRDB(v)
	default:
		return fmt.Errorf("unknown type: %v", v.Type)
	}

	_, err := w.writer.Write(bytes)
	if err != nil {
		return err
	}

	return nil
}

func (w *Writer) marshalString(v Value) []byte {
	return []byte(fmt.Sprintf("+%s\r\n", v.Str))
}

func (w *Writer) marshalError(v Value) []byte {
	return []byte(fmt.Sprintf("-%s\r\n", v.Str))
}

func (w *Writer) marshalInt(v Value) []byte {
	return []byte(fmt.Sprintf(":%d\r\n", v.Num))
}

func (w *Writer) marshalBulk(v Value) []byte {
	if v.Bulk == "$NULL$" {
		return []byte(fmt.Sprintf("$%d\r\n", -1))
	}
	return []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(v.Bulk), v.Bulk))
}

func (w *Writer) marshalArray(v Value) []byte {
	if len(v.Array) > 0 && v.Array[0].Bulk == "$NULL$" {
		return []byte(fmt.Sprintf("*%d\r\n", -1))
	}
	var bytes []byte
	bytes = append(bytes, []byte(fmt.Sprintf("*%d\r\n", len(v.Array)))...)

	for _, val := range v.Array {
		bytes = append(bytes, w.marshalValue(val)...)
	}

	return bytes
}

func (w *Writer) marshalRDB(v Value) []byte {
	header := fmt.Sprintf("$%d\r\n", len(v.Bulk))
	return append([]byte(header), []byte(v.Bulk)...)
}

func (w *Writer) marshalValue(v Value) []byte {
	switch v.Type {
	case STRING:
		return w.marshalString(v)
	case ERROR:
		return w.marshalError(v)
	case INTEGER:
		return w.marshalInt(v)
	case BULK:
		return w.marshalBulk(v)
	case RDB_FILE:
		return w.marshalRDB(v)
	case ARRAY:
		return w.marshalArray(v)
	default:
		return nil
	}
}

// Read reads and parses the next RESP value from the underlying reader.
func (r *Resp) Read() (Value, error) {
	_type, err := r.reader.ReadByte()
	if err != nil {
		return Value{}, err
	}

	switch _type {
	case ARRAY:
		return r.readArray()
	case BULK:
		return r.readBulk()
	case STRING:
		return r.readSimpleString()
	case ERROR:
		return r.readError()
	case INTEGER:
		return r.readInt()
	default:
		// Not a standard RESP type. Let's treat it as an inline command.
		err := r.reader.UnreadByte()
		if err != nil {
			return Value{}, err
		}

		line, _, err := r.readLine()
		if err != nil {
			return Value{}, err
		}

		parts := strings.Split(string(line), " ")
		var array []Value
		for _, part := range parts {
			if part != "" {
				array = append(array, Value{Type: BULK, Bulk: part})
			}
		}

		return Value{Type: ARRAY, Array: array}, nil
	}
}

func (r *Resp) readLine() (line []byte, n int, err error) {
	for {
		b, err := r.reader.ReadByte()
		if err != nil {
			return nil, 0, err
		}
		n += 1
		line = append(line, b)
		if len(line) >= 2 && line[len(line)-2] == '\r' && line[len(line)-1] == '\n' {
			break
		}
	}
	return line[:len(line)-2], n, nil
}

func (r *Resp) readInteger() (x int, n int, err error) {
	line, n, err := r.readLine()
	if err != nil {
		return 0, 0, err
	}
	i64, err := strconv.ParseInt(string(line), 10, 64)
	if err != nil {
		return 0, n, err
	}
	return int(i64), n, nil
}

func (r *Resp) readArray() (Value, error) {
	v := Value{}
	v.Type = ARRAY

	len, _, err := r.readInteger()
	if len == -1 {
		v.Array = nil
		return v, err
	}
	if err != nil {
		return v, err
	}

	v.Array = make([]Value, 0)
	for i := 0; i < len; i++ {
		val, err := r.Read()
		if err != nil {
			return v, err
		}
		v.Array = append(v.Array, val)
	}

	return v, nil
}

func (r *Resp) readBulk() (Value, error) {
	v := Value{}
	v.Type = BULK

	len, _, err := r.readInteger()
	if len == -1 {
		v.Bulk = ""
		return v, err
	}
	if err != nil {
		return v, err
	}

	bulk := make([]byte, len)

	_, err = io.ReadFull(r.reader, bulk)
	if err != nil {
		return v, err
	}

	v.Bulk = string(bulk)

	// Read trailing \r\n
	r.readLine()

	return v, nil
}

func (r *Resp) readSimpleString() (Value, error) {
	v := Value{}
	v.Type = STRING

	line, _, err := r.readLine()
	if err != nil {
		return v, err
	}

	v.Str = string(line)

	return v, nil
}

func (r *Resp) readError() (Value, error) {
	v := Value{}
	v.Type = ERROR

	line, _, err := r.readLine()
	if err != nil {
		return v, err
	}

	v.Str = string(line)

	return v, nil
}

func (r *Resp) readInt() (Value, error) {
	v := Value{}
	v.Type = INTEGER

	res, _, err := r.readInteger()
	if err != nil {
		return v, err
	}

	v.Num = res

	return v, nil
}
