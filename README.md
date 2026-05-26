# kv-store

A replicated, strongly-consistent key-value store built on top of [`paxos-go`](https://github.com/jvenkit1/paxos-go).

Each node runs an identical state machine (`map[string]string`); writes go through Paxos consensus before being applied. Reads are served locally (eventually consistent).

## Build & test

```
make dep      # download dependencies
make test     # run unit tests
make build    # build bin/kv
```

## Running a 3-node cluster

Each `go run ./cmd/kv` blocks at startup until all peers are reachable — paxos-go's `transport.Start` waits for peer gRPC connections to become `Ready`, eliminating the split-brain election race. You have 30 seconds between launching the first and third node before any will give up.

```bash
# Terminal 1
go run ./cmd/kv -id 1 -paxos 127.0.0.1:9001 -http 127.0.0.1:8001 \
  -peers 2=127.0.0.1:9002=127.0.0.1:8002,3=127.0.0.1:9003=127.0.0.1:8003

# Terminal 2
go run ./cmd/kv -id 2 -paxos 127.0.0.1:9002 -http 127.0.0.1:8002 \
  -peers 1=127.0.0.1:9001=127.0.0.1:8001,3=127.0.0.1:9003=127.0.0.1:8003

# Terminal 3
go run ./cmd/kv -id 3 -paxos 127.0.0.1:9003 -http 127.0.0.1:8003 \
  -peers 1=127.0.0.1:9001=127.0.0.1:8001,2=127.0.0.1:9002=127.0.0.1:8002

# Terminal 4 — node 3 is the leader (highest ID)
curl -X PUT -H 'Content-Type: application/json' \
     -d '{"value":"bar"}' http://127.0.0.1:8003/kv/foo
curl http://127.0.0.1:8001/kv/foo   # → {"value":"bar"} (replicated)
curl http://127.0.0.1:8002/kv/foo   # → {"value":"bar"} (replicated)
```

## Layout

```
cmd/kv/             # CLI binary
internal/command/   # Command type + JSON encoding
internal/store/     # In-memory replicated state machine
internal/node/      # Wraps paxos.Node, owns apply loop + waiters
internal/server/    # HTTP server
```

## Known limitations (v1)

- **No persistence.** Restarting a node loses state; restarting the cluster loses everything.
- **Local reads can be stale.** Cross-replica read-your-writes is not guaranteed.
- **Followers reject writes.** Clients receive 503 with no `Location` header (paxos-go doesn't yet expose leader identity to followers).
- **No idempotency for client retries.** A retried write may be applied twice (harmless for SET/DELETE; unsafe for future CAS).