package embedding

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/shrec21/engram/internal/store"
	pb "github.com/shrec21/engram/proto/engrampb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Proposer submits commands through the Raft log.
type Proposer interface {
	Propose(ctx context.Context, cmd *pb.Command) (*pb.CommandResult, uint64, error)
}

// EmbeddingWorker runs on the leader only. It scans for records needing
// embeddings and proposes SetEmbeddingCommand through Raft.
// Implements LeaderService (Start/Stop).
type EmbeddingWorker struct {
	store    *store.Store
	embedder Embedder
	proposer Proposer
	batch    int
	interval time.Duration

	stopCh chan struct{}
	doneCh chan struct{}
	mu     sync.Mutex
}

// NewEmbeddingWorker creates a new worker.
func NewEmbeddingWorker(st *store.Store, embedder Embedder, proposer Proposer, batchSize int, scanInterval time.Duration) *EmbeddingWorker {
	if batchSize <= 0 {
		batchSize = 32
	}
	if scanInterval <= 0 {
		scanInterval = 5 * time.Second
	}
	return &EmbeddingWorker{
		store:    st,
		embedder: embedder,
		proposer: proposer,
		batch:    batchSize,
		interval: scanInterval,
	}
}

// Start implements LeaderService.
func (w *EmbeddingWorker) Start() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.stopCh = make(chan struct{})
	w.doneCh = make(chan struct{})
	go w.run()
}

// Stop implements LeaderService.
func (w *EmbeddingWorker) Stop() {
	w.mu.Lock()
	stopCh := w.stopCh
	doneCh := w.doneCh
	w.mu.Unlock()
	if stopCh != nil {
		close(stopCh)
		<-doneCh
	}
}

func (w *EmbeddingWorker) run() {
	defer close(w.doneCh)

	// Initial backfill scan.
	w.processBackfill()

	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			w.processBackfill()
		case <-w.stopCh:
			return
		}
	}
}

func (w *EmbeddingWorker) processBackfill() {
	backoff := 100 * time.Millisecond
	maxBackoff := 30 * time.Second

	for {
		entries, err := w.store.ScanNeedingEmbedding(w.batch)
		if err != nil {
			log.Printf("embedding: scan error: %v", err)
			return
		}
		if len(entries) == 0 {
			return
		}

		if err := w.processBatch(entries); err != nil {
			log.Printf("embedding: batch error: %v, backing off %v", err, backoff)
			select {
			case <-time.After(backoff):
			case <-w.stopCh:
				return
			}
			backoff = min(backoff*2, maxBackoff)
			continue
		}
		backoff = 100 * time.Millisecond

		// If we got a full batch, there may be more.
		if len(entries) < w.batch {
			return
		}
	}
}

func (w *EmbeddingWorker) processBatch(entries []store.NeedingEmbeddingEntry) error {
	texts := make([]string, len(entries))
	for i, e := range entries {
		texts[i] = string(e.Value)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	vectors, err := w.embedder.Embed(ctx, texts)
	if err != nil {
		return err
	}

	now := time.Now()
	for i, e := range entries {
		cmd := &pb.Command{
			Timestamp: timestamppb.New(now),
			Payload: &pb.Command_SetEmbedding{
				SetEmbedding: &pb.SetEmbeddingCommand{
					Namespace:       e.Namespace,
					Key:             e.Key,
					ExpectedVersion: e.Version,
					Embedding:       vectors[i],
					EmbeddingModel:  w.embedder.Model(),
				},
			},
		}
		propCtx, propCancel := context.WithTimeout(context.Background(), 10*time.Second)
		_, _, propErr := w.proposer.Propose(propCtx, cmd)
		propCancel()
		if propErr != nil {
			log.Printf("embedding: propose SetEmbedding %s/%s: %v", e.Namespace, e.Key, propErr)
		}
	}

	return nil
}
