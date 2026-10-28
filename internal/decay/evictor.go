package decay

import (
	"context"
	"log"
	"time"

	"github.com/shrec21/engram/internal/clock"
	"github.com/shrec21/engram/internal/metrics"
	pb "github.com/shrec21/engram/proto/engrampb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// PolicyStore provides access to decay policies.
type PolicyStore interface {
	GetDecayPolicy(namespace string) (*pb.DecayPolicy, error)
	ListDecayPolicies() (map[string]*pb.DecayPolicy, error)
}

// RecordScanner scans all records for scoring.
type RecordScanner interface {
	List(namespace, keyPrefix string, limit int, pageToken string) ([]*pb.MemoryRecord, string, error)
}

// Evictor periodically scans namespaces with decay policies and deletes
// records whose score falls below the minimum. Implements LeaderService.
type Evictor struct {
	policies PolicyStore
	scanner  RecordScanner
	proposer Proposer
	clock    clock.Clock
	interval time.Duration
	stopCh   chan struct{}
	doneCh   chan struct{}
}

// NewEvictor creates a new evictor.
func NewEvictor(policies PolicyStore, scanner RecordScanner, proposer Proposer, clk clock.Clock, interval time.Duration) *Evictor {
	if interval <= 0 {
		interval = 5 * time.Minute
	}
	return &Evictor{
		policies: policies,
		scanner:  scanner,
		proposer: proposer,
		clock:    clk,
		interval: interval,
	}
}

// Start implements LeaderService.
func (e *Evictor) Start() {
	e.stopCh = make(chan struct{})
	e.doneCh = make(chan struct{})
	go e.run()
}

// Stop implements LeaderService.
func (e *Evictor) Stop() {
	close(e.stopCh)
	<-e.doneCh
}

func (e *Evictor) run() {
	defer close(e.doneCh)
	ticker := time.NewTicker(e.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			e.EvictOnce()
		case <-e.stopCh:
			return
		}
	}
}

// EvictOnce runs a single eviction pass across all namespaces with policies.
func (e *Evictor) EvictOnce() {
	policies, err := e.policies.ListDecayPolicies()
	if err != nil {
		log.Printf("evictor: list policies: %v", err)
		return
	}

	now := e.clock.Now()

	for ns, policy := range policies {
		if policy.MinScore <= 0 {
			continue // no eviction threshold
		}
		e.evictNamespace(ns, policy, now)
	}
}

func (e *Evictor) evictNamespace(namespace string, policy *pb.DecayPolicy, now time.Time) {
	pageToken := ""
	for {
		records, nextToken, err := e.scanner.List(namespace, "", 500, pageToken)
		if err != nil {
			log.Printf("evictor: list %s: %v", namespace, err)
			return
		}

		for _, rec := range records {
			// Skip pinned records.
			if rec.Pinned {
				continue
			}

			score := Score(rec, policy, now)
			if score < policy.MinScore {
				// Delete with expected_version for safety.
				cmd := &pb.Command{
					Timestamp: timestamppb.New(now),
					Payload: &pb.Command_Delete{
						Delete: &pb.DeleteCommand{
							Namespace:       rec.Namespace,
							Key:             rec.Key,
							ExpectedVersion: rec.Version,
						},
					},
				}
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				result, _, err := e.proposer.Propose(ctx, cmd)
				cancel()
				if err != nil {
					log.Printf("evictor: delete %s/%s: %v", namespace, rec.Key, err)
					continue
				}
				if result.GetDeleteResult() != nil {
					metrics.EvictDeletedTotal.Inc()
				}
				// CAS conflict means the record was updated — skip.
			}
		}

		if nextToken == "" {
			break
		}
		pageToken = nextToken
	}
}
