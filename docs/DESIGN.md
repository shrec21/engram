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

**Why:** In Week 2, Apply becomes the Raft FSM's apply method. The Raft log
serializes Commands across nodes. For replicas to converge, Apply must be
deterministic — the same sequence of commands must produce identical state on
every node. Any non-determinism (wall-clock, randomness, network calls) would
cause replicas to diverge.

On a single node (Week 1), a `sync.Mutex` serializes Apply calls. This ensures
CAS read-check-write sequences are atomic. When Raft is added, the Raft log
replaces this mutex as the serialization mechanism.

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
   and issues Delete commands through Apply for any that have expired. This
   reclaims storage space and ensures the Raft log (in Week 2) carries the
   deletion as a deterministic command.

The sweeper never deletes directly from Pebble. It always goes through Apply,
so deletions replay correctly during Raft log replay.

## Access Metadata

`last_accessed_at` and `access_count` are updated directly on read, bypassing
Apply. These are node-local observational side-effects, not replicated state.
In a Raft cluster, each node tracks its own access patterns independently.
