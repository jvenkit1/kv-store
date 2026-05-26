package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/jvenkit1/kv-store/internal/node"
	"github.com/jvenkit1/kv-store/internal/server"
	"github.com/jvenkit1/paxos-go/paxos"
)

const (
	// transport.Start now blocks until all peers are reachable; 30s gives the
	// operator time to launch the other nodes in separate terminals.
	transportStartTimeout = 30 * time.Second
	shutdownTimeout       = 3 * time.Second
)

type nodeSpec struct {
	paxosAddr string
	httpAddr  string
}

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, nil)))

	id := flag.Int("id", 0, "Node's Unique ID (required, >0)")
	paxosAddr := flag.String("paxos", "", "Bind address for inbound paxos gRPC (127.0.0.1:9001)")
	httpAddr := flag.String("http", "", "Bind address for HTTP API (127.0.0.1:8080)")
	peersFlag := flag.String("peers", "", "Comma separated peers list as id=paxosAddr=httpAddr")
	flag.Parse()

	if *id <= 0 || *paxosAddr == "" || *httpAddr == "" {
		flag.Usage()
		os.Exit(2)
	}

	peers, err := parsePeers(*peersFlag, *id)
	if err != nil {
		slog.Error("invalid -peers", "err", err)
		os.Exit(2)
	}

	paxosAddrs := make(map[int]string, len(peers))
	httpAddrs := make(map[int]string, len(peers)+1)
	siblingIDs := make([]int, 0, len(peers))

	for pid, spec := range peers {
		paxosAddrs[pid] = spec.paxosAddr
		httpAddrs[pid] = spec.httpAddr
		siblingIDs = append(siblingIDs, pid)
	}

	httpAddrs[*id] = *httpAddr

	transport, err := paxos.NewGRPCTransport(*id, *paxosAddr, paxosAddrs)
	if err != nil {
		slog.Error("NewGRPCTransport", "err", err)
		os.Exit(1)
	}

	startCtx, startCancel := context.WithTimeout(context.Background(), transportStartTimeout)
	defer startCancel()

	if err := transport.Start(startCtx); err != nil {
		slog.Error("Error setting up transport", "err", err)
		os.Exit(1)
	}

	n, err := node.New(node.Config{
		NodeID:     *id,
		SiblingIDs: siblingIDs,
		Transport:  transport,
		HTTPAddrs:  httpAddrs,
	})
	if err != nil {
		slog.Error("Error creating node", "err", err)
		os.Exit(1)
	}
	if err := n.Start(context.Background()); err != nil {
		slog.Error("Error starting node", "err", err)
		os.Exit(1)
	}

	srv := server.New(n, *httpAddr)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		if err := srv.Start(); err != nil {
			slog.Error("Error starting server", "err", err)

			// Self signal to initiate a shutdown sequence
			sigCh <- syscall.SIGTERM
		}
	}()

	slog.Info("KV-Store Node up",
		"id", *id,
		"paxos", *paxosAddr,
		"http", *httpAddr,
		"peers", paxosAddrs,
	)

	<-sigCh
	slog.Info("Shutting down")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer shutdownCancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.Error("Shutting down server", "err", err)
	}
	n.Stop()
	if err := transport.Stop(shutdownCtx); err != nil {
		slog.Error("Stopping transport", "err", err)
	}
}

func parsePeers(s string, selfID int) (map[int]nodeSpec, error) {
	out := map[int]nodeSpec{}

	s = strings.TrimSpace(s)
	if s == "" {
		return out, nil
	}

	for _, raw := range strings.Split(s, ",") {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}

		parts := strings.Split(raw, "=")
		if len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
			return nil, fmt.Errorf("entry %q: expected id=paxosAddr=httpAddr", raw)
		}
		id, err := strconv.Atoi(parts[0])
		if err != nil {
			return nil, fmt.Errorf("entry %q: bad id: %w", raw, err)
		}

		if id == selfID {
			return nil, fmt.Errorf("entry %q: peer id matches -id", raw)
		}
		if _, dup := out[id]; dup {
			return nil, fmt.Errorf("entry %q: duplicate peer id %d", raw, id)
		}

		out[id] = nodeSpec{paxosAddr: parts[1], httpAddr: parts[2]}
	}

	return out, nil
}
