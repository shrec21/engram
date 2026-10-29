# Engram — Project Guide

## Commit Discipline

- Commit after every small, single logical change (one new proto message, one
  function plus its test, one config change, one doc section).
- Never batch unrelated changes into one commit. If a diff touches unrelated
  things, split it.
- Run `go build ./...` and the relevant tests before each commit. Never commit
  code that does not compile.
- Use Conventional Commits: `feat(shard):`, `fix(raft):`, `test(chaos):`,
  `docs(design):`, `refactor(store):`, `chore(ci):`.
- Messages say WHAT changed and WHY in the subject, details in the body if needed.

## Build & Test

```bash
make build          # builds engramd + engramctl
make test           # go test -race ./...
make proto          # regenerate protobuf (requires protoc)
go test -short ./...  # skip slow cluster tests
```

## Invariants

### Apply is deterministic
`store.Apply(cmd)` must never read wall-clock time, use randomness, or perform
I/O beyond Pebble reads/writes. All timestamps are carried in the Command.
Breaking this causes Raft replicas to diverge.

### System key prefix
Keys under `\xFF` (SystemKeyPrefix) are internal (dedup table). They must be
excluded from user-visible List and ScanExpired. The Pebble UpperBound in
ScanExpired enforces this.

### Proposer is the only write path
All mutations go through `Proposer.Propose()`. In Raft mode this is `RaftNode`.
In single-node mode this is `localProposer` (calls `store.Apply` directly).
Never write to the store outside of Apply.

### LeaderService pattern
Services that must run on the leader only (sweeper, touch flusher) implement
`LeaderService{Start(), Stop()}`. The `LeaderMonitor` watches the Raft
leadership channel and starts/stops them on transitions.

### NOT_LEADER uses codes.Unavailable
Write requests to a follower return `codes.Unavailable` with `NotLeaderDetail`
in error details. CAS conflicts use `codes.FailedPrecondition`. Do not confuse
these two error paths.

### Get is a pure read
`store.Get()` does not write access metadata. Access deltas accumulate in the
in-memory `AccessTracker` and are flushed through Raft by `TouchFlusher`.

### Dedup requires client_id
If `client_id` is empty, dedup is skipped (fire-and-forget mode). The smart
client always sets `client_id` and `seq`.

### Embedding never in Apply
The `Embedder` (Ollama HTTP call) must never be invoked inside `store.Apply`.
Embeddings are computed asynchronously by the `EmbeddingWorker` (a
LeaderService) and proposed as `SetEmbeddingCommand` through Raft. The
version guard (`expected_version`) prevents stale embeddings.

### HNSW is a projection
The HNSW index is not replicated through Raft. It is a per-node projection
of Pebble state, rebuilt on startup and after snapshot restore. Losing the
index is safe — it can always be rebuilt from `ScanAllRecords`.

### Decay uses node clock
Decay scores are computed at read time using the node's local clock, not
inside Apply. The `DecayPolicy` itself is replicated through Raft (stored
under system key `\xFF\x01decay\x00<namespace>`).

### Evictor uses conditional deletes
The decay `Evictor` passes `expected_version` when deleting low-score records.
If a record was updated between score computation and delete proposal, the
delete is rejected. This prevents the evictor from deleting freshly-updated
records.

### applyCh is non-blocking
The FSM sends `ApplyEvent` on a buffered channel (cap 4096) using a
non-blocking send. If the consumer falls behind, events are dropped — the
consumer catches up via Pebble scan on the next index rebuild cycle. Never
make the FSM block on this channel.

## Package Layout

```
internal/store/       Pebble state machine (Apply, Get, List)
internal/raft/        Raft node, FSM, snapshots, dedup, leader monitor
internal/api/         gRPC servers (MemoryService, AdminService)
internal/decay/       TTL sweeper, access touch flusher, decay scoring, evictor
internal/embedding/   Embedder interface, Ollama client, async worker
internal/index/       HNSW semantic index, FSM event consumer
internal/bench/       Open-loop benchmark engine with HDR histograms
internal/client/      Smart gRPC client with redirect + session + dedup
internal/metrics/     Prometheus metric definitions
internal/clock/       Clock interface for testing
cmd/engramd/          Server binary
cmd/engramctl/        CLI + MCP server
proto/                Protobuf definitions
test/                 Cluster integration + chaos tests (toxiproxy)
tools/                Benchmark chart generation (plot_bench.py)
docs/                 DESIGN.md, BENCHMARKS.md
deploy/               Dockerfile, docker-compose, Prometheus config
```
