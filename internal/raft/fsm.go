package raft

import (
	"fmt"
	"io"
	"os"
	"time"

	hraft "github.com/hashicorp/raft"
	"github.com/shrec21/engram/internal/store"
	pb "github.com/shrec21/engram/proto/engrampb"
	"google.golang.org/protobuf/proto"
)

// ApplyResult is stored in raft.Log.Extensions and returned from FSM.Apply.
type ApplyResult struct {
	Result *pb.CommandResult
	Err    error
}

// FSM implements hashicorp/raft.FSM, wrapping the deterministic store.Apply.
type FSM struct {
	store        *store.Store
	dedup        *DedupTable
	indexWaiter  *IndexWaiter
	snapshotDir  string // temp dir prefix for checkpoints
}

// NewFSM creates a new FSM.
func NewFSM(st *store.Store, dedup *DedupTable, iw *IndexWaiter, snapshotDir string) *FSM {
	return &FSM{
		store:       st,
		dedup:       dedup,
		indexWaiter: iw,
		snapshotDir: snapshotDir,
	}
}

// Apply is called by Raft when a log entry is committed.
func (f *FSM) Apply(log *hraft.Log) interface{} {
	var cmd pb.Command
	if err := proto.Unmarshal(log.Data, &cmd); err != nil {
		return &ApplyResult{Err: fmt.Errorf("unmarshal command: %w", err)}
	}

	// Exactly-once dedup: if we've seen this (client_id, seq) before, return cached result.
	if cmd.ClientId != "" {
		if cached, dup := f.dedup.Check(cmd.ClientId, cmd.Seq); dup {
			f.indexWaiter.Notify(log.Index)
			return &ApplyResult{Result: cached}
		}
	}

	result, err := f.store.Apply(&cmd)
	if err != nil {
		f.indexWaiter.Notify(log.Index)
		return &ApplyResult{Err: err}
	}

	// Record for dedup.
	if cmd.ClientId != "" {
		now := time.Now() // dedup bookkeeping, not state-machine state
		_ = f.dedup.Record(cmd.ClientId, cmd.Seq, result, now)
	}

	f.indexWaiter.Notify(log.Index)
	return &ApplyResult{Result: result}
}

// Snapshot creates a Pebble checkpoint for the snapshot.
func (f *FSM) Snapshot() (hraft.FSMSnapshot, error) {
	dir, err := os.MkdirTemp(f.snapshotDir, "engram-checkpoint-*")
	if err != nil {
		return nil, fmt.Errorf("mktemp: %w", err)
	}

	cpDir := dir + "/data"
	if err := f.store.Checkpoint(cpDir); err != nil {
		os.RemoveAll(dir)
		return nil, fmt.Errorf("checkpoint: %w", err)
	}

	return &fsmSnapshot{checkpointDir: cpDir}, nil
}

// Restore replaces the store from a snapshot.
func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	return restoreSnapshot(f.store, rc)
}
