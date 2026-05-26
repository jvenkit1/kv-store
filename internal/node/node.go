package node

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/jvenkit1/kv-store/internal/command"
	"github.com/jvenkit1/kv-store/internal/store"
	"github.com/jvenkit1/paxos-go/paxos"
)

type Config struct {
	NodeID     int
	SiblingIDs []int
	Transport  paxos.Transport
	HTTPAddrs  map[int]string
}

type Node struct {
	cfg   Config
	node  *paxos.Node
	store *store.Store

	waitersMu sync.Mutex
	waiters   map[string]chan store.ApplyResult

	done     chan struct{}
	stopOnce sync.Once
}

var ErrNotLeader = errors.New("node is not the leader")

func New(cfg Config) (*Node, error) {
	if cfg.NodeID <= 0 {
		return nil, fmt.Errorf("NodeID must be >0")
	}
	if cfg.Transport == nil {
		return nil, fmt.Errorf("Transport must be setup")
	}

	return &Node{
		cfg:     cfg,
		node:    paxos.NewNode(cfg.NodeID, cfg.SiblingIDs, cfg.Transport),
		store:   store.New(),
		waiters: make(map[string]chan store.ApplyResult),
		done:    make(chan struct{}),
	}, nil
}

// Launches the paxos node and the apply-loop goroutine
func (n *Node) Start(ctx context.Context) error {
	n.node.Start(ctx)
	go n.applyLoop()

	return nil
}

// Halts the apply loop and the underlying paxos node
func (n *Node) Stop() {
	n.stopOnce.Do(func() {
		close(n.done)
		n.node.Stop()
	})
}

func (n *Node) IsLeader() bool {
	return n.node.IsLeader()
}

func (n *Node) Get(key string) string {
	return n.store.Get(key)
}

func (n *Node) LeaderHTTPAddr() (string, bool) {
	if n.node.IsLeader() {
		addr, ok := n.cfg.HTTPAddrs[n.cfg.NodeID]
		return addr, ok
	}
	return "", false
}

func (n *Node) ID() int       { return n.cfg.NodeID }
func (n *Node) StoreLen() int { return n.store.Len() }

func (n *Node) Propose(ctx context.Context, cmd command.Command) (store.ApplyResult, error) {
	if !n.node.IsLeader() {
		return store.ApplyResult{}, ErrNotLeader
	}

	if cmd.RequestID == "" {
		return store.ApplyResult{}, fmt.Errorf("missing requestId")
	}

	ch := make(chan store.ApplyResult, 1)
	n.registerWaiter(cmd.RequestID, ch)
	defer n.unregisterWaiter(cmd.RequestID)

	encoded, err := command.Encode(cmd)
	if err != nil {
		return store.ApplyResult{}, fmt.Errorf("error with encoding %w", err)
	}

	if err := n.node.Propose(ctx, encoded); err != nil {
		return store.ApplyResult{}, fmt.Errorf("error while proposing %w", err)
	}

	select {
	case result := <-ch:
		return result, nil
	case <-ctx.Done():
		return store.ApplyResult{}, ctx.Err()
	case <-n.done:
		return store.ApplyResult{}, fmt.Errorf("node has been stopped")
	}
}

func (n *Node) applyLoop() {
	for {
		select {
		case entry := <-n.node.Committed():
			cmd, err := command.Decode(entry.Value)
			if err != nil {
				continue
			}
			result := n.store.Apply(cmd)
			n.notifyWaiter(cmd.RequestID, result)
		case <-n.done:
			return
		}
	}
}

func (n *Node) registerWaiter(id string, ch chan store.ApplyResult) {
	n.waitersMu.Lock()
	n.waiters[id] = ch
	n.waitersMu.Unlock()
}

func (n *Node) unregisterWaiter(id string) {
	n.waitersMu.Lock()
	delete(n.waiters, id)
	n.waitersMu.Unlock()
}

func (n *Node) notifyWaiter(id string, result store.ApplyResult) {
	n.waitersMu.Lock()

	ch, ok := n.waiters[id]
	n.waitersMu.Unlock()

	if !ok {
		return
	}

	select {
	case ch <- result:
	default:
	}
}
