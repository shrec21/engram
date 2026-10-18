package decay

import (
	"context"
	"log"
	"time"

	"github.com/shrec21/engram/internal/clock"
	"github.com/shrec21/engram/internal/store"
	pb "github.com/shrec21/engram/proto/engrampb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Proposer submits commands through the Raft log (or directly for single-node).
type Proposer interface {
	Propose(ctx context.Context, cmd *pb.Command) (*pb.CommandResult, uint64, error)
}

// Sweeper periodically scans for and removes expired records through the proposer.
// Implements LeaderService: started/stopped by LeaderMonitor.
type Sweeper struct {
	scanner  *store.Store
	proposer Proposer
	clock    clock.Clock
	interval time.Duration
	stopCh   chan struct{}
	doneCh   chan struct{}
}

// NewSweeper creates a sweeper. For single-node mode, proposer wraps store.Apply directly.
func NewSweeper(scanner *store.Store, proposer Proposer, clk clock.Clock, interval time.Duration) *Sweeper {
	return &Sweeper{
		scanner:  scanner,
		proposer: proposer,
		clock:    clk,
		interval: interval,
	}
}

// Start implements LeaderService. Starts the sweep loop in a goroutine.
func (sw *Sweeper) Start() {
	sw.stopCh = make(chan struct{})
	sw.doneCh = make(chan struct{})
	go sw.run()
}

// Stop implements LeaderService. Signals the sweeper to shut down and waits.
func (sw *Sweeper) Stop() {
	close(sw.stopCh)
	<-sw.doneCh
}

func (sw *Sweeper) run() {
	defer close(sw.doneCh)
	ticker := time.NewTicker(sw.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			sw.SweepOnce()
		case <-sw.stopCh:
			return
		}
	}
}

// SweepOnce runs a single sweep pass. Exported for testing.
func (sw *Sweeper) SweepOnce() {
	now := sw.clock.Now()

	expired, err := sw.scanner.ScanExpired(now)
	if err != nil {
		log.Printf("sweep scan error: %v", err)
		return
	}

	ctx := context.Background()
	for _, e := range expired {
		cmd := &pb.Command{
			Timestamp: timestamppb.New(now),
			Payload: &pb.Command_Delete{
				Delete: &pb.DeleteCommand{
					Namespace:       e.Namespace,
					Key:             e.Key,
					ExpectedVersion: 0, // unconditional
				},
			},
		}
		if _, _, err := sw.proposer.Propose(ctx, cmd); err != nil {
			log.Printf("sweep delete %s/%s: %v", e.Namespace, e.Key, err)
		}
	}
}
