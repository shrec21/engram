# Engram Design

## The Apply Constraint

All mutations flow through a single function:

```go
func (s *Store) Apply(cmd *pb.Command) (*pb.CommandResult, error)
```

**Rules:**
- No wall-clock reads (`time.Now()`) inside Apply. Timestamps are carried in the Command.
- No randomness or external I/O besides Pebble reads/writes.
- The function is deterministic: given the same state and command, it produces the same result.

**Why:** Apply is the Raft FSM's apply method. The Raft log serializes Commands
across nodes. For replicas to converge, Apply must be deterministic — the same
sequence of commands must produce identical state on every node. Any
non-determinism (wall-clock, randomness, network calls) would cause replicas to
diverge.

In single-node mode, a `localProposer` calls Apply directly. In a Raft cluster,
the Raft log serializes Apply calls through the FSM goroutine.

## Raft Replication

Engram uses hashicorp/raft with a 3-node cluster for fault tolerance.

### Write Path

1. Client sends a write (Put/CAS/Delete) to any node.
2. If the node is not the leader, it returns `codes.Unavailable` with a
   `NotLeaderDetail` containing the leader's gRPC address. The smart client
   follows the redirect automatically.
3. The leader wraps the request into a `pb.Command`, marshals it, and calls
   `raft.Apply()`.
4. Raft replicates the log entry to a majority of nodes.
5. Each node's FSM calls `store.Apply(cmd)` to apply the command to Pebble.
6. The leader returns the result to the client.

### Read Path

Three consistency levels:

| Level | Guarantee | Cost |
|-------|-----------|------|
| **LINEARIZABLE** | Read reflects all committed writes | Leader VerifyLeader + Barrier |
| **SESSION** | Read-your-writes via `applied_index` token | Wait for local index to catch up |
| **STALE** | Local read, possibly stale | No coordination |

Clients track `applied_index` from write responses and pass it on subsequent
reads for SESSION consistency. The server waits until its local applied index
is >= the client's token before serving the read.

### Snapshots

Snapshots use Pebble's `Checkpoint()` which creates a hardlink-based copy.
The checkpoint directory is tar'd into the Raft snapshot sink. On restore,
the tar is extracted and `store.ReplaceFromDir()` swaps the Pebble instance.

Dedup table entries live in Pebble under the `\xFF\x00dedup\x00` prefix, so
they survive snapshots naturally.

## Exactly-Once Semantics

Write requests carry `client_id` and `seq` (monotonically increasing per
client). The FSM checks a dedup table before applying:

1. If `client_id` is empty, skip dedup (fire-and-forget).
2. If `seq <= recorded_seq` for that client, return the cached result.
3. Otherwise, apply the command and record `{seq, result, timestamp}`.

The dedup table is stored in Pebble under system key prefix `\xFF`, so it is
included in snapshots and excluded from user-visible List/Scan operations.

Entries idle for > 5 minutes are pruned on each FSM apply.

## Why CAS Instead of Last-Writer-Wins

AI agents often coordinate through shared memory. Consider two agents updating
the same task status:

**Last-writer-wins (Put):**
1. Agent A reads status: `{"phase": "planning", "step": 3}`
2. Agent B reads status: `{"phase": "planning", "step": 3}`
3. Agent A writes: `{"phase": "planning", "step": 4}`
4. Agent B writes: `{"phase": "executing", "step": 3}`
5. Result: Agent A's increment is lost.

**Compare-and-set (CAS):**
1. Agent A reads status at version 5
2. Agent B reads status at version 5
3. Agent A CAS(expected=5): succeeds → version 6
4. Agent B CAS(expected=5): fails (current=6) → re-reads, merges, retries
5. Result: no lost updates.

CAS gives agents the building block for safe coordination without distributed
locks. It's the minimum viable primitive for correct concurrent access.

`expected_version = 0` is a special case: create-only. This lets an agent claim
a key atomically (e.g., leader election, task assignment) without risk of
overwriting an existing value.

## TTL and Decay

Records support an optional `expires_at` timestamp. Expiry is enforced at two levels:

1. **Lazy (on read):** Get and List check `expires_at` against the current clock.
   Expired records are invisible — returned as not-found.

2. **Eager (background sweep):** The decay sweeper periodically scans all records
   and issues Delete commands through the Proposer interface. In Raft mode, this
   means deletions go through the Raft log. In single-node mode, they go directly
   to `store.Apply()`.

The sweeper is a `LeaderService` — it only runs on the current Raft leader.
The `LeaderMonitor` starts it on leadership gain and stops it on loss.

## Access Metadata

Read operations track `last_accessed_at` and `access_count` via an in-memory
`AccessTracker` that accumulates deltas. A `TouchFlusher` (also a
`LeaderService`) periodically drains the tracker and proposes a `TouchCommand`
through Raft. This keeps access metadata replicated without writing to Pebble
on every read.

**Tradeoff:** Access stats are approximate. Deltas accumulated since the last
flush are lost on crash. This is acceptable — access metadata is observational,
not part of the correctness-critical state.

## Key Interfaces

```go
// Proposer submits commands (Raft leader or local store).
type Proposer interface {
    Propose(ctx context.Context, cmd *pb.Command) (*pb.CommandResult, uint64, error)
}

// ReadStore provides read access to Pebble state.
type ReadStore interface {
    Get(namespace, key string) (*pb.MemoryRecord, error)
    List(ns, prefix string, limit int, token string) ([]*pb.MemoryRecord, string, error)
}

// ClusterInfo provides Raft cluster state.
type ClusterInfo interface {
    Leader() (grpcAddr, id string, isLeader bool)
    LinearizableBarrier(ctx context.Context) error
    WaitForIndex(ctx context.Context, index uint64) error
    AppliedIndex() uint64
}

// LeaderService runs only on the leader (sweeper, touch flusher).
type LeaderService interface {
    Start()
    Stop()
}
```
