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

## Async Embedding Architecture

Embeddings are computed **outside** the Raft FSM to preserve determinism.

### Flow

1. A `Put` command is applied through Raft. The record is stored without an
   embedding (or with a stale one).
2. The FSM sends a non-blocking `ApplyEvent` on a buffered channel (cap 4096).
3. The `EmbeddingWorker` (a LeaderService, runs on leader only) receives the
   event and queues the record for embedding.
4. The worker calls the `Embedder` (Ollama HTTP API) to compute the vector.
5. The worker proposes a `SetEmbeddingCommand` through Raft with the record's
   current version as a guard.
6. The FSM applies `SetEmbedding` only if `expected_version` matches — if the
   record was updated between embedding and apply, the stale embedding is
   discarded and the worker will retry on the next scan.

### Reliability: Belt and Suspenders

- **Event-driven:** ApplyEvent channel provides low-latency notification.
- **Periodic scan:** `ScanNeedingEmbedding` catches anything missed (dropped
  events, restarts, worker downtime).
- **Version guard:** Prevents stale embeddings from overwriting fresh content.

### Why Not Embed in Apply?

Calling an HTTP API inside Apply would make the FSM non-deterministic —
different nodes would get different vectors (or timeouts), causing state
divergence. The embedding worker runs only on the leader, proposes the result
through Raft, and all nodes see the same vector in the same log position.

## HNSW Index as Projection

The HNSW vector index is a **projection** of Pebble state, not authoritative
data. It is rebuilt from scratch on startup and after snapshot restore.

### Design

- Each node maintains its own HNSW index (not replicated through Raft).
- The `Consumer` goroutine reads `ApplyEvent` from the FSM channel and updates
  the HNSW graph.
- On startup, `RebuildFromPebble` scans all records with embeddings.
- On snapshot restore (ApplyEvent with Index=0), the consumer clears and
  rebuilds the index.

### Search Path

1. Client sends `SearchRequest` with `query_text` or `query_vector`.
2. Server embeds `query_text` if needed (using the same Embedder).
3. HNSW returns top k×3 candidates (over-fetch for post-filtering).
4. Server reads full records from Pebble, applies filters (namespace, prefix,
   created_by, updated_after, min_confidence).
5. Computes decay scores using the namespace's `DecayPolicy`.
6. Blends: `score = alpha × similarity + (1-alpha) × decay_score`
7. Returns top-k by blended score.

### Why Not a Replicated Index?

Replicating HNSW mutations through Raft would add complexity and serialize
index updates behind the log. Since the index can be rebuilt from Pebble at
any time, treating it as a projection keeps the system simpler and avoids
coupling HNSW internals to the Raft log format.

## Decay Model

Records accumulate a decay score that drives eviction decisions.

### Score Formula

```
score = confidence × 2^(-age / half_life) × (1 + access_weight × ln(1 + access_count))
```

- **confidence:** Record's confidence field (0-1, default 1.0).
- **age:** Time since creation.
- **half_life:** Namespace-specific, set via `DecayPolicy`.
- **access_weight:** How much access frequency boosts the score.
- **access_count:** Number of reads (tracked via AccessTracker + TouchFlusher).

### Decay Policy

Stored in Pebble under system key `\xFF\x01decay\x00<namespace>`, replicated
through Raft. All nodes see the same policy. Score is computed at read time
using the node's local clock (not in Apply).

### Evictor

The `Evictor` (a LeaderService) periodically:
1. Lists all namespaces with decay policies.
2. Scans records in each namespace.
3. Computes decay scores.
4. Deletes records below `min_score` — but only if not pinned and only using
   version-guarded deletes (the delete carries `expected_version` to avoid
   racing with concurrent updates).

## Chaos Testing

Network partition tests use [Toxiproxy](https://github.com/Shopify/toxiproxy)
to inject failures between Raft nodes.

### Setup

Each node's Raft transport binds to a real address but advertises a Toxiproxy
address. Peers connect through the proxy, giving tests control over the
network link.

```
Node 1 (bind :50061) ← proxy (:60061) ← Node 2 connects to :60061
```

### Scenarios

| Test | What It Does |
|------|-------------|
| LeaderIsolated | Partition the leader; verify new election + writes on remaining majority |
| FollowerIsolated | Partition a follower; verify writes continue; heal and verify catch-up |
| HighLatency | Inject 200ms ± 50ms latency on all links; verify writes complete |
| PartitionHealCycles | 5 random partition/heal cycles with writes each round |
| AsymmetricPartition | Isolate one node; concurrent writes from 3 goroutines; heal and verify |

Tests skip gracefully if `toxiproxy-server` is not in PATH.

## Known Limitations

- **HNSW filtering is post-search.** Namespace and metadata filters are applied
  after HNSW retrieval. With many namespaces in one index, the over-fetch
  factor (k×3) may not be enough. A per-namespace index would solve this at
  the cost of memory.
- **Embedding latency.** Records are searchable only after the embedding worker
  processes them. Under load, there can be a lag between Put and searchability.
- **Single embedder model.** All namespaces share one embedding model. Mixed
  models would require per-namespace index partitioning.
- **Access count approximation.** Access deltas accumulated since the last
  TouchFlusher flush are lost on crash.

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

// Embedder computes vector embeddings (never called inside Apply).
type Embedder interface {
    Embed(ctx context.Context, texts []string) ([][]float32, error)
    Dimension() int
    Model() string
}

// SemanticSearcher queries the per-node HNSW projection.
type SemanticSearcher interface {
    Search(namespace string, query []float32, k int) []SearchHit
}

// LeaderService runs only on the leader (sweeper, touch flusher,
// embedding worker, evictor).
type LeaderService interface {
    Start()
    Stop()
}
```
