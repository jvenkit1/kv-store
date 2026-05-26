package command

import (
	"errors"
	"testing"
)

func TestCommandRoundTripSet(t *testing.T) {
	in := Command{
		RequestID: "req-1",
		Op:        OpSet,
		Key:       "foo",
		Value:     "bar",
	}
	b, err := Encode(in)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	out, err := Decode(b)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if out != in {
		t.Errorf("round-trip mismatch:\n  in  = %+v\n  out = %+v", in, out)
	}
}

func TestCommandRoundTripDelete(t *testing.T) {
	in := Command{
		RequestID: "req-2",
		Op:        OpDelete,
		Key:       "foo",
	}
	b, err := Encode(in)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	out, err := Decode(b)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if out != in {
		t.Errorf("round-trip mismatch:\n  in  = %+v\n  out = %+v", in, out)
	}
}

func TestDecodeMalformedJSON(t *testing.T) {
	_, err := Decode([]byte("not json"))
	if err == nil {
		t.Fatal("expected error on malformed JSON")
	}
}

func TestDecodeUnknownOp(t *testing.T) {
	b := []byte(`{"request_id":"x","op":"frobnicate","key":"k"}`)
	_, err := Decode(b)
	if !errors.Is(err, ErrUnknownOp) {
		t.Errorf("expected ErrUnknownOp, got %v", err)
	}
}

func TestDecodeEmptyKey(t *testing.T) {
	b := []byte(`{"request_id":"x","op":"set","key":"","value":"v"}`)
	_, err := Decode(b)
	if !errors.Is(err, ErrEmptyKey) {
		t.Errorf("expected ErrEmptyKey, got %v", err)
	}
}

func TestDecodeEmptyRequestID(t *testing.T) {
	b := []byte(`{"request_id":"","op":"set","key":"k","value":"v"}`)
	_, err := Decode(b)
	if !errors.Is(err, ErrEmptyRequestID) {
		t.Errorf("expected ErrEmptyRequestID, got %v", err)
	}
}

func TestEncodeOmitsEmptyValue(t *testing.T) {
	// Delete commands should not carry a "value" field — keeps the on-wire
	// log compact and unambiguous.
	in := Command{RequestID: "r", Op: OpDelete, Key: "k"}
	b, err := Encode(in)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if got := string(b); got != `{"request_id":"r","op":"delete","key":"k"}` {
		t.Errorf("unexpected encoding: %s", got)
	}
}
