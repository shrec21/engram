# Engram

A replicated memory store for AI agents with semantic search. Raft-based
clusters provide linearizable CAS, TTL expiry, exactly-once semantics,
decay-based eviction, HNSW vector search, and MCP integration.

```
                       ┌──────────────────────────────────────┐
                       │           Smart Client               │
                       │  redirect │ session │ dedup │ search  │
                       └─────┬──────────┬──────────┬──────────┘
                             │          │          │
                    ┌────────▼──┐  ┌────▼─────┐  ┌▼──────────┐
                    │  engram-1  │  │ engram-2  │  │ engram-3  │
                    │  (leader)  │  │(follower) │  │(follower) │
                    ├───────────┤  ├──────────┤  ├───────────┤
                    │ gRPC API  │  │ gRPC API │  │ gRPC API  │
                    │ HNSW Index│  │HNSW Index│  │HNSW Index │
                    │ Pebble DB │  │Pebble DB │  │Pebble DB  │
                    └─────┬─────┘  └────┬─────┘  └─────┬─────┘
                          │    Raft Log  │              │
                          └──────────────┴──────────────┘
```

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

# Pin a record (exempt from decay eviction)
./engramctl put agents critical-config '{"key":"val"}' --pinned
```

## Semantic Search

Engram embeds record values asynchronously using Ollama and indexes them in an
HNSW graph for nearest-neighbor retrieval. Search results blend vector
similarity with decay scores.

```bash
# Start with embedding enabled (requires Ollama running)
./engramd --data ./engram-data --addr :50051 \
  --embedding-model nomic-embed-text --ollama-url http://localhost:11434

# Store some memories
./engramctl put research finding-1 "Transformer attention scales quadratically"
./engramctl put research finding-2 "Flash attention reduces memory to linear"
./engramctl put research finding-3 "Mixture of experts enables conditional compute"

# Search by meaning
./engramctl search research "efficient attention mechanisms" --k 5

# Search with filters
./engramctl search research "attention" --k 3 --created-by analyst
```

Without `--embedding-model`, the server runs without embeddings and the Search
RPC returns an error.

### Decay-Based Eviction

Records accumulate a decay score based on age, access frequency, and confidence.
When score drops below a threshold, non-pinned records are evicted.

```bash
# Set a decay policy for a namespace
./engramctl set-decay-policy research \
  --half-life 86400 --access-weight 0.5 --min-score 0.1

# Check the policy
./engramctl get-decay-policy research
```

Score formula: `confidence x 2^(-age/half_life) x (1 + w * ln(1 + access_count))`

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

Tools exposed: `memory_get`, `memory_put`, `memory_cas`, `memory_delete`,
`memory_list`, `memory_search`.

### Two-Agent CAS Coordination

```
Agent A (planner):                    Agent B (executor):
  memory_put("tasks", "build",          memory_get("tasks", "build")
    '{"phase":"plan","step":1}')         → version 1
                                         memory_cas("tasks", "build",
  memory_get("tasks", "build")             '{"phase":"exec","step":1}',
  → version 2 (B updated it)              expected_version=1) → version 2
  memory_cas(..., expected_version=1)
  → CONFLICT (re-read, merge, retry)
```

CAS prevents lost updates when multiple agents coordinate through shared memory.

## Benchmarks

```bash
# Run against a cluster
engramctl bench --records 10000 --duration 30s --workers 20 --rate 5000

# Generate charts
python3 tools/plot_bench.py results.json docs/
```

See [docs/BENCHMARKS.md](docs/BENCHMARKS.md) for methodology and configuration.

## Tests

```bash
# All tests (includes cluster integration + Porcupine linearizability)
make test

# Skip slow cluster tests
go test -short ./...

# Chaos tests (requires toxiproxy-server in PATH)
go test -run Chaos -count=1 -race ./test/
```

## Architecture

See [docs/DESIGN.md](docs/DESIGN.md) for Raft replication, consistency levels,
exactly-once semantics, the Apply constraint, async embedding architecture,
HNSW-as-projection, and decay model.
