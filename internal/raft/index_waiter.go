package raft

import (
	"context"
	"sync"
	"sync/atomic"
)

// IndexWaiter allows callers to block until the FSM has applied up to a
// given Raft log index. Used for SESSION-consistency reads on followers.
type IndexWaiter struct {
	mu      sync.Mutex
	applied uint64
	waiters []waiter
}

type waiter struct {
	index uint64
	ch    chan struct{}
}

// NewIndexWaiter creates a new IndexWaiter.
func NewIndexWaiter() *IndexWaiter {
	return &IndexWaiter{}
}

// Notify is called by the FSM after applying a log entry. It wakes all
// waiters whose target index has been reached.
func (w *IndexWaiter) Notify(index uint64) {
	atomic.StoreUint64(&w.applied, index)

	w.mu.Lock()
	defer w.mu.Unlock()

	remaining := w.waiters[:0]
	for _, wt := range w.waiters {
		if index >= wt.index {
			close(wt.ch)
		} else {
			remaining = append(remaining, wt)
		}
	}
	w.waiters = remaining
}

// WaitForIndex blocks until the applied index reaches at least the given value,
// or the context is cancelled.
func (w *IndexWaiter) WaitForIndex(ctx context.Context, index uint64) error {
	if atomic.LoadUint64(&w.applied) >= index {
		return nil
	}

	ch := make(chan struct{})
	w.mu.Lock()
	// Re-check after acquiring lock.
	if atomic.LoadUint64(&w.applied) >= index {
		w.mu.Unlock()
		return nil
	}
	w.waiters = append(w.waiters, waiter{index: index, ch: ch})
	w.mu.Unlock()

	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// AppliedIndex returns the last applied index.
func (w *IndexWaiter) AppliedIndex() uint64 {
	return atomic.LoadUint64(&w.applied)
}
