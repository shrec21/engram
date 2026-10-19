# Engram — Project Guide

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

## Package Layout

```
internal/store/       Pebble state machine (Apply, Get, List)
internal/raft/        Raft node, FSM, snapshots, dedup, leader monitor
internal/api/         gRPC servers (MemoryService, AdminService)
internal/decay/       TTL sweeper + access touch flusher
internal/client/      Smart gRPC client with redirect + session + dedup
internal/metrics/     Prometheus metric definitions
internal/clock/       Clock interface for testing
cmd/engramd/          Server binary
cmd/engramctl/        CLI + MCP server
proto/                Protobuf definitions
test/                 Cluster integration tests
deploy/               Dockerfile, docker-compose, Prometheus config
```
