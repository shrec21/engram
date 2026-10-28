package index

import (
	"log"
	"time"

	engramraft "github.com/shrec21/engram/internal/raft"
	"github.com/shrec21/engram/internal/store"
	"github.com/shrec21/engram/internal/metrics"
)

// Consumer reads ApplyEvents from the FSM channel and updates the HNSW index.
// Runs on ALL nodes — the index is a projection of Pebble state.
type Consumer struct {
	index   *SemanticIndex
	store   *store.Store
	applyCh <-chan engramraft.ApplyEvent
	stopCh  chan struct{}
	doneCh  chan struct{}
}

// NewConsumer creates an index consumer.
func NewConsumer(idx *SemanticIndex, st *store.Store, applyCh <-chan engramraft.ApplyEvent) *Consumer {
	return &Consumer{
		index:   idx,
		store:   st,
		applyCh: applyCh,
		stopCh:  make(chan struct{}),
		doneCh:  make(chan struct{}),
	}
}

// Start performs an initial rebuild from Pebble and then processes events.
func (c *Consumer) Start() {
	go c.run()
}

// Stop signals the consumer to shut down and waits.
func (c *Consumer) Stop() {
	close(c.stopCh)
	<-c.doneCh
}

func (c *Consumer) run() {
	defer close(c.doneCh)

	c.rebuildFromPebble()

	for {
		select {
		case evt, ok := <-c.applyCh:
			if !ok {
				return
			}
			c.handleEvent(evt)
		case <-c.stopCh:
			return
		}
	}
}

func (c *Consumer) handleEvent(evt engramraft.ApplyEvent) {
	// Index=0 signals a snapshot restore — rebuild everything.
	if evt.Index == 0 {
		c.rebuildFromPebble()
		return
	}

	if evt.Deleted {
		c.index.Delete(evt.Namespace, evt.Key)
		metrics.IndexSize.Set(float64(c.index.Len()))
		return
	}

	if evt.Embedding != nil {
		// SetEmbedding event — use the embedding directly.
		c.index.Add(evt.Namespace, evt.Key, evt.Embedding)
		metrics.IndexSize.Set(float64(c.index.Len()))
		return
	}

	// Put/CAS event — record content changed but embedding may be stale.
	// Read the current record to check if it has an embedding.
	rec, err := c.store.Get(evt.Namespace, evt.Key)
	if err != nil || rec == nil {
		return
	}
	if len(rec.Embedding) > 0 {
		c.index.Add(rec.Namespace, rec.Key, rec.Embedding)
		metrics.IndexSize.Set(float64(c.index.Len()))
	}
}

func (c *Consumer) rebuildFromPebble() {
	start := time.Now()
	records, err := c.store.ScanAllRecords()
	if err != nil {
		log.Printf("index consumer: rebuild scan error: %v", err)
		return
	}

	c.index.RebuildFrom(records)
	dur := time.Since(start)
	metrics.IndexRebuildDuration.Observe(dur.Seconds())
	metrics.IndexSize.Set(float64(c.index.Len()))
	log.Printf("index consumer: rebuilt HNSW with %d vectors in %v", c.index.Len(), dur)
}
