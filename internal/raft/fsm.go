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

// ApplyEvent is a post-apply notification sent on a buffered channel.
// The consumer uses these to keep the HNSW index up to date.
type ApplyEvent struct {
	Index     uint64
	Namespace string
	Key       string
	Deleted   bool
	Embedding []float32 // non-nil only for SetEmbedding
}

// FSM implements hashicorp/raft.FSM, wrapping the deterministic store.Apply.
type FSM struct {
	store       *store.Store
	dedup       *DedupTable
	indexWaiter *IndexWaiter
	snapshotDir string // temp dir prefix for checkpoints
	applyCh     chan<- ApplyEvent
}

// NewFSM creates a new FSM. applyCh may be nil if no consumer is attached.
func NewFSM(st *store.Store, dedup *DedupTable, iw *IndexWaiter, snapshotDir string, applyCh chan<- ApplyEvent) *FSM {
	return &FSM{
		store:       st,
		dedup:       dedup,
		indexWaiter: iw,
		snapshotDir: snapshotDir,
		applyCh:     applyCh,
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

	// Non-blocking send of ApplyEvent for HNSW index consumer.
	if f.applyCh != nil {
		evt := f.buildApplyEvent(log.Index, &cmd, result)
		if evt != nil {
			select {
			case f.applyCh <- *evt:
			default:
				// Consumer behind — it will catch up via Pebble scan.
			}
		}
	}

	f.indexWaiter.Notify(log.Index)
	return &ApplyResult{Result: result}
}

// buildApplyEvent constructs an event from a command and its result.
func (f *FSM) buildApplyEvent(index uint64, cmd *pb.Command, result *pb.CommandResult) *ApplyEvent {
	switch p := cmd.Payload.(type) {
	case *pb.Command_Put:
		return &ApplyEvent{
			Index:     index,
			Namespace: p.Put.Namespace,
			Key:       p.Put.Key,
		}
	case *pb.Command_CompareAndSet:
		if result.GetMutation() != nil {
			return &ApplyEvent{
				Index:     index,
				Namespace: p.CompareAndSet.Namespace,
				Key:       p.CompareAndSet.Key,
			}
		}
	case *pb.Command_Delete:
		if result.GetDeleteResult() != nil {
			return &ApplyEvent{
				Index:     index,
				Namespace: p.Delete.Namespace,
				Key:       p.Delete.Key,
				Deleted:   true,
			}
		}
	case *pb.Command_SetEmbedding:
		if result.GetSetEmbeddingResult() != nil && result.GetSetEmbeddingResult().Applied {
			return &ApplyEvent{
				Index:     index,
				Namespace: p.SetEmbedding.Namespace,
				Key:       p.SetEmbedding.Key,
				Embedding: p.SetEmbedding.Embedding,
			}
		}
	}
	return nil
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
	err := restoreSnapshot(f.store, rc)
	if err != nil {
		return err
	}
	// Signal that a restore happened — consumer should rebuild HNSW from Pebble.
	if f.applyCh != nil {
		select {
		case f.applyCh <- ApplyEvent{Index: 0}: // index=0 signals rebuild
		default:
		}
	}
	return nil
}
