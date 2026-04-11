package main

import (
	"bytes"
	"reflect"
	"testing"
)

func TestRead(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    Value
		wantErr bool
	}{
		{
			name:  "Simple String",
			input: "+OK\r\n",
			want:  Value{Type: STRING, Str: "OK"},
		},
		{
			name:  "Error",
			input: "-Error message\r\n",
			want:  Value{Type: ERROR, Str: "Error message"},
		},
		{
			name:  "Integer",
			input: ":1000\r\n",
			want:  Value{Type: INTEGER, Num: 1000},
		},
		{
			name:  "Bulk String",
			input: "$6\r\nfoobar\r\n",
			want:  Value{Type: BULK, Bulk: "foobar"},
		},
		{
			name:  "Empty Bulk String",
			input: "$0\r\n\r\n",
			want:  Value{Type: BULK, Bulk: ""},
		},
		{
			name:  "Null Bulk String",
			input: "$-1\r\n",
			want:  Value{Type: BULK, Bulk: ""}, // Typically nulls are handled by a specific field or check
		},
		{
			name:  "Array",
			input: "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n",
			want: Value{
				Type: ARRAY,
				Array: []Value{
					{Type: BULK, Bulk: "foo"},
					{Type: BULK, Bulk: "bar"},
				},
			},
		},
		{
			name:  "Empty Array",
			input: "*0\r\n",
			want:  Value{Type: ARRAY, Array: []Value{}},
		},
		{
			name:  "Null Array",
			input: "*-1\r\n",
			want:  Value{Type: ARRAY, Array: nil},
		},
		{
			name:    "Invalid Type",
			input:   "!invalid\r\n",
			wantErr: false, // Currently default returns empty Value, no error
		},
		{
			name:    "EOF",
			input:   "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewResp(bytes.NewBufferString(tt.input))
			got, err := r.Read()
			if (err != nil) != tt.wantErr {
				t.Errorf("Read() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Read() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestWrite(t *testing.T) {
	tests := []struct {
		name  string
		input Value
		want  string
	}{
		{
			name:  "Simple String",
			input: Value{Type: STRING, Str: "OK"},
			want:  "+OK\r\n",
		},
		{
			name:  "Error",
			input: Value{Type: ERROR, Str: "Error message"},
			want:  "-Error message\r\n",
		},
		{
			name:  "Integer",
			input: Value{Type: INTEGER, Num: 1000},
			want:  ":1000\r\n",
		},
		{
			name:  "Bulk String",
			input: Value{Type: BULK, Bulk: "foobar"},
			want:  "$6\r\nfoobar\r\n",
		},
		{
			name:  "Empty Bulk String",
			input: Value{Type: BULK, Bulk: ""},
			want:  "$0\r\n\r\n",
		},
		{
			name: "Array",
			input: Value{
				Type: ARRAY,
				Array: []Value{
					{Type: BULK, Bulk: "foo"},
					{Type: BULK, Bulk: "bar"},
				},
			},
			want: "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n",
		},
		{
			name:  "Empty Array",
			input: Value{Type: ARRAY, Array: []Value{}},
			want:  "*0\r\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := &bytes.Buffer{}
			w := NewWriter(buf)
			err := w.Write(tt.input)
			if err != nil {
				t.Fatalf("Write() error = %v", err)
			}
			if got := buf.String(); got != tt.want {
				t.Errorf("Write() got = %q, want %q", got, tt.want)
			}
		})
	}
}
