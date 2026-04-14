package main

import (
	"testing"
)

func TestXADD(t *testing.T) {
	db := NewDB()
	key := "mystream"

	// 1. Explicit ID
	id1, err := db.XADD(key, "0-1", map[string]string{"foo": "bar"})
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if id1 != "0-1" {
		t.Errorf("Expected 0-1, got %s", id1)
	}

	// 2. Reject 0-0
	_, err = db.XADD(key, "0-0", map[string]string{"a": "b"})
	if err == nil {
		t.Error("Expected error for 0-0, got nil")
	}

	// 3. Reject smaller or equal ID
	_, err = db.XADD(key, "0-1", map[string]string{"a": "b"})
	if err == nil {
		t.Error("Expected error for equal ID 0-1, got nil")
	}

	// 4. Auto-generate sequence number (time-*)
	id2, err := db.XADD(key, "0-*", map[string]string{"foo": "baz"})
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if id2 != "0-2" {
		t.Errorf("Expected 0-2, got %s", id2)
	}

	// 5. Auto-generate everything (*)
	id3, err := db.XADD(key, "*", map[string]string{"foo": "qux"})
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	// Since id2 was 0-2, id3 should have a timestamp > 0 or 0-3 if time is still 0
	if id3 == "0-1" || id3 == "0-2" {
		t.Errorf("Unexpected auto-generated ID: %s", id3)
	}

	// 6. Multiple field-value pairs (use a very large timestamp to be sure)
	id4, err := db.XADD(key, "9999999999999-0", map[string]string{"key1": "val1", "key2": "val2"})
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if id4 != "9999999999999-0" {
		t.Errorf("Expected 9999999999999-0, got %s", id4)
	}
}
