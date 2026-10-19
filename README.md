# Engram

A replicated memory store for AI agents. Raft-based 3-node clusters with
linearizable CAS, TTL expiry, exactly-once semantics, and MCP integration.

## Quickstart (single node)

```bash
make build

./engramd --data ./engram-data --addr :50051

# Write a memory
./engramctl put agents task-status '{"status":"running"}' --created-by planner

# Read it back
./engramctl get agents task-status

# Compare-and-set (version must match)
./engramctl cas agents task-status '{"status":"done"}' --version 1

# List all memories in a namespace
./engramctl list agents

# Delete
./engramctl delete agents task-status

# Write with TTL (auto-expires)
./engramctl put agents temp-note "remember this" --ttl 5m
```

## 3-Node Cluster

### Docker Compose

```bash
cd deploy
docker compose up --build
```

This starts 3 Engram nodes, Prometheus, and Grafana:

| Service | Address |
|---------|---------|
| engram-1 (gRPC) | localhost:50051 |
| engram-2 (gRPC) | localhost:50052 |
| engram-3 (gRPC) | localhost:50053 |
| Prometheus | localhost:9090 |
| Grafana | localhost:3000 |

```bash
# Write through any node (smart client follows redirects)
./engramctl --addr localhost:50051 put agents key "value"

# Read with linearizable consistency
./engramctl --addr localhost:50051 get agents key --consistency linearizable

# Check cluster status
./engramctl --addr localhost:50051 cluster-status
```

### Manual

```bash
# Terminal 1: bootstrap node
./engramd --addr :50051 --raft-addr :50061 --node-id node-1 --bootstrap \
  --peers node-2=:50062/:50052,node-3=:50063/:50053 --data ./data-1

# Terminal 2
./engramd --addr :50052 --raft-addr :50062 --node-id node-2 \
  --peers node-1=:50061/:50051,node-3=:50063/:50053 --data ./data-2

# Terminal 3
./engramd --addr :50053 --raft-addr :50063 --node-id node-3 \
  --peers node-1=:50061/:50051,node-2=:50062/:50052 --data ./data-3
```

## Consistency Levels

| Level | Flag | Guarantee |
|-------|------|-----------|
| Session | `--consistency session` | Read-your-writes (default) |
| Linearizable | `--consistency linearizable` | Reads reflect all committed writes |
| Stale | `--consistency stale` | Fast local read, may be stale |

## MCP Integration

Start the MCP stdio server (connects to a running engramd):

```bash
./engramctl mcp
```

Tools exposed: `memory_get`, `memory_put`, `memory_cas`, `memory_delete`, `memory_list`.

## Tests

```bash
# All tests (includes cluster integration + Porcupine linearizability)
make test

# Skip slow cluster tests
go test -short ./...
```

## Architecture

See [docs/DESIGN.md](docs/DESIGN.md) for Raft replication, consistency levels,
exactly-once semantics, and the Apply constraint.
