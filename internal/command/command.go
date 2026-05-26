// Package command defines the set of state-machine mutations that the
// replicated key-value store agrees on via Paxos. Each Command is encoded as
// bytes and pushed as a single Paxos proposal value; on commit, every replica
// decodes and applies the command to its local store.
package command

import (
	"encoding/json"
	"errors"
	"fmt"
)

// Op identifies the kind of mutation a Command performs.
type Op string

const (
	// OpSet writes Value at Key.
	OpSet Op = "set"
	// OpDelete removes Key. Value is ignored.
	OpDelete Op = "delete"
)

// Command is the unit of replicated mutation.
type Command struct {
	// RequestID is a client- or server-generated unique identifier. The
	// replica uses it to match a committed entry back to the in-flight HTTP
	// request that proposed it, so the handler can wait for apply before
	// responding.
	RequestID string `json:"request_id"`

	// Op is the mutation kind. Must be a known Op value; Decode rejects
	// unknown ops.
	Op Op `json:"op"`

	// Key is the target key. Must be non-empty.
	Key string `json:"key"`

	// Value is the value to set. Ignored for OpDelete.
	Value string `json:"value,omitempty"`
}

// ErrUnknownOp is returned by Decode when the encoded Op is not one of the
// known values.
var ErrUnknownOp = errors.New("command: unknown op")

// ErrEmptyKey is returned by Decode when the encoded Key is empty.
var ErrEmptyKey = errors.New("command: empty key")

// ErrEmptyRequestID is returned by Decode when the encoded RequestID is empty.
var ErrEmptyRequestID = errors.New("command: empty request id")

// Encode serializes a Command as JSON.
func Encode(c Command) ([]byte, error) {
	return json.Marshal(c)
}

// Decode parses a previously Encoded Command. It validates the Op, Key, and
// RequestID fields; malformed input returns a wrapped error.
func Decode(b []byte) (Command, error) {
	var c Command
	if err := json.Unmarshal(b, &c); err != nil {
		return Command{}, fmt.Errorf("command: decode: %w", err)
	}
	switch c.Op {
	case OpSet, OpDelete:
	default:
		return Command{}, fmt.Errorf("%w: %q", ErrUnknownOp, c.Op)
	}
	if c.Key == "" {
		return Command{}, ErrEmptyKey
	}
	if c.RequestID == "" {
		return Command{}, ErrEmptyRequestID
	}
	return c, nil
}
