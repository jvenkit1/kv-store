package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/jvenkit1/kv-store/internal/command"
	"github.com/jvenkit1/kv-store/internal/node"
)

type Server struct {
	node *node.Node
	srv  *http.Server
}

const (
	maxBodyBytes      = 1 << 14
	proposeTimeout    = 5 * time.Second
	readHeaderTimeout = 10 * time.Second
)

func New(n *node.Node, addr string) *Server {
	s := &Server{node: n}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /kv/{key}", s.handleGet)
	mux.HandleFunc("PUT /kv/{key}", s.handlePut)
	mux.HandleFunc("DELETE /kv/{key}", s.handleDelete)
	mux.HandleFunc("GET /status", s.handleStatus)

	s.srv = &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: readHeaderTimeout,
	}

	return s
}

func (s *Server) Start() error {
	if err := s.srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func (s *Server) Shutdown(ctx context.Context) error {
	return s.srv.Shutdown(ctx)
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")

	v := s.node.Get(key)
	if v == "" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]string{"Error": "key not found"})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{"value": v})

}

func (s *Server) handlePut(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")

	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	var putRequestBody struct {
		Value string `json:"value"`
	}

	if err := decoder.Decode(&putRequestBody); err != nil {
		var maxErr *http.MaxBytesError
		w.Header().Set("Content-Type", "application/json")
		if errors.As(err, &maxErr) {
			w.WriteHeader(http.StatusRequestEntityTooLarge)
			_ = json.NewEncoder(w).Encode(map[string]string{"Error": "Request Body too large"})
			return
		}

		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"Error": "Invalid JSON: " + err.Error()})
		return
	}

	if putRequestBody.Value == "" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"Error": "value must be non-empty"})
		return
	}

	// Setting a request timeout and deferring it to trigger until "proposeTimeout" seconds
	ctx, cancel := context.WithTimeout(r.Context(), proposeTimeout)
	defer cancel()

	result, err := s.node.Propose(ctx, command.Command{
		RequestID: uuid.NewString(),
		Op:        command.OpSet,
		Key:       key,
		Value:     putRequestBody.Value,
	})
	if err != nil {
		writeProposeError(w, err)
		return
	}

	created := result.OldValue == ""
	statusCode := http.StatusOK
	if created {
		statusCode = http.StatusCreated
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(map[string]bool{"Created": created})
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")

	ctx, cancel := context.WithTimeout(r.Context(), proposeTimeout)
	defer cancel()

	result, err := s.node.Propose(ctx, command.Command{
		RequestID: uuid.NewString(),
		Op:        command.OpDelete,
		Key:       key,
	})
	if err != nil {
		writeProposeError(w, err)
		return
	}

	if result.OldValue == "" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]string{"Error": "Key not found"})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]bool{"Deleted": true})
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"node_id":   s.node.ID(),
		"is_leader": s.node.IsLeader(),
		"store_len": s.node.StoreLen(),
	})
}

// Captures errors when invoking the Paxos proposer
func writeProposeError(w http.ResponseWriter, err error) {
	var code int
	var msg string
	switch {
	case errors.Is(err, node.ErrNotLeader):
		code, msg = http.StatusServiceUnavailable, "not leader"
	case errors.Is(err, context.DeadlineExceeded):
		code, msg = http.StatusGatewayTimeout, "apply timeout"
	default:
		code, msg = http.StatusInternalServerError, fmt.Errorf("propose: %w", err).Error()
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"Error": msg})
}
