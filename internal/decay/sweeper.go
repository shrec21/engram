package decay

import (
	"log"
	"time"

	"github.com/shrec21/engram/internal/clock"
	"github.com/shrec21/engram/internal/store"
	pb "github.com/shrec21/engram/proto/engrampb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Sweeper periodically scans for and removes expired records through Apply.
type Sweeper struct {
	store    *store.Store
	clock    clock.Clock
	interval time.Duration
	stopCh   chan struct{}
	doneCh   chan struct{}
}

func NewSweeper(s *store.Store, clk clock.Clock, interval time.Duration) *Sweeper {
	return &Sweeper{
		store:    s,
		clock:    clk,
		interval: interval,
		stopCh:   make(chan struct{}),
		doneCh:   make(chan struct{}),
	}
}

// Run starts the sweep loop. Call from a goroutine.
func (sw *Sweeper) Run() {
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

// Stop signals the sweeper to shut down and waits for it.
func (sw *Sweeper) Stop() {
	close(sw.stopCh)
	<-sw.doneCh
}

// SweepOnce runs a single sweep pass. Exported for testing.
func (sw *Sweeper) SweepOnce() {
	now := sw.clock.Now()

	expired, err := sw.store.ScanExpired(now)
	if err != nil {
		log.Printf("sweep scan error: %v", err)
		return
	}

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
		if _, err := sw.store.Apply(cmd); err != nil {
			log.Printf("sweep delete %s/%s: %v", e.Namespace, e.Key, err)
		}
	}
}
