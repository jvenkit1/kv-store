package store

import (
	"fmt"
	"sync"

	"github.com/jvenkit1/kv-store/internal/command"
)

type ApplyResult struct {
	Op       command.Op
	Key      string
	OldValue string
	Err      error
}

type Store struct {
	mu   sync.RWMutex
	data map[string]string
}

func New() *Store {
	return &Store{data: make(map[string]string)}
}

func (s *Store) Apply(cmd command.Command) ApplyResult {
	s.mu.Lock()
	defer s.mu.Unlock()

	prev := s.data[cmd.Key]
	res := ApplyResult{
		Op:       cmd.Op,
		Key:      cmd.Key,
		OldValue: prev,
	}

	switch cmd.Op {
	case command.OpSet:
		s.data[cmd.Key] = cmd.Value

	case command.OpDelete:
		delete(s.data, cmd.Key)
	default:
		res.Err = fmt.Errorf("kv-store: unknown op %q", cmd.Op)
	}
	return res
}

func (s *Store) Get(key string) string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	v := s.data[key]
	return v
}

func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return len(s.data)
}
