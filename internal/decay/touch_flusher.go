package decay

import (
	"context"
	"log"
	"time"

	"github.com/shrec21/engram/internal/store"
	pb "github.com/shrec21/engram/proto/engrampb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TouchFlusher periodically drains the AccessTracker and proposes TouchCommand
// through Raft. Runs only on the leader (implements LeaderService).
type TouchFlusher struct {
	tracker  *store.AccessTracker
	proposer Proposer
	interval time.Duration
	stopCh   chan struct{}
	doneCh   chan struct{}
}

// NewTouchFlusher creates a touch flusher.
func NewTouchFlusher(tracker *store.AccessTracker, proposer Proposer, interval time.Duration) *TouchFlusher {
	return &TouchFlusher{
		tracker:  tracker,
		proposer: proposer,
		interval: interval,
	}
}

// Start implements LeaderService.
func (f *TouchFlusher) Start() {
	f.stopCh = make(chan struct{})
	f.doneCh = make(chan struct{})
	go f.run()
}

// Stop implements LeaderService.
func (f *TouchFlusher) Stop() {
	close(f.stopCh)
	<-f.doneCh
}

func (f *TouchFlusher) run() {
	defer close(f.doneCh)
	ticker := time.NewTicker(f.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			f.FlushOnce()
		case <-f.stopCh:
			return
		}
	}
}

// FlushOnce drains accumulated access deltas and proposes a TouchCommand.
func (f *TouchFlusher) FlushOnce() {
	deltas := f.tracker.Drain()
	if len(deltas) == 0 {
		return
	}

	entries := make([]*pb.TouchEntry, len(deltas))
	for i, d := range deltas {
		entries[i] = &pb.TouchEntry{
			Namespace:        d.Namespace,
			Key:              d.Key,
			LastAccessedAt:   timestamppb.New(d.LastAccessedAt),
			AccessCountDelta: d.CountDelta,
		}
	}

	cmd := &pb.Command{
		Payload: &pb.Command_Touch{
			Touch: &pb.TouchCommand{Entries: entries},
		},
	}

	ctx := context.Background()
	if _, _, err := f.proposer.Propose(ctx, cmd); err != nil {
		log.Printf("touch flush error: %v", err)
	}
}
