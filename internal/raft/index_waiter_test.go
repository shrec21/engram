package raft

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIndexWaiterAlreadyApplied(t *testing.T) {
	w := NewIndexWaiter()
	w.Notify(10)

	err := w.WaitForIndex(context.Background(), 5)
	require.NoError(t, err)
	assert.Equal(t, uint64(10), w.AppliedIndex())
}

func TestIndexWaiterWaitsAndWakes(t *testing.T) {
	w := NewIndexWaiter()

	var wg sync.WaitGroup
	wg.Add(1)

	var waitErr error
	go func() {
		defer wg.Done()
		waitErr = w.WaitForIndex(context.Background(), 5)
	}()

	// Small delay to ensure goroutine is waiting.
	time.Sleep(10 * time.Millisecond)
	w.Notify(3) // not enough
	time.Sleep(10 * time.Millisecond)
	w.Notify(5) // should wake

	wg.Wait()
	assert.NoError(t, waitErr)
}

func TestIndexWaiterContextCancel(t *testing.T) {
	w := NewIndexWaiter()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := w.WaitForIndex(ctx, 100)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestIndexWaiterMultipleWaiters(t *testing.T) {
	w := NewIndexWaiter()

	var wg sync.WaitGroup
	results := make([]error, 3)

	for i, idx := range []uint64{3, 5, 7} {
		wg.Add(1)
		go func(i int, idx uint64) {
			defer wg.Done()
			results[i] = w.WaitForIndex(context.Background(), idx)
		}(i, idx)
	}

	time.Sleep(10 * time.Millisecond)
	w.Notify(5) // wakes waiters for 3 and 5

	time.Sleep(10 * time.Millisecond)
	w.Notify(10) // wakes waiter for 7

	wg.Wait()
	for i, err := range results {
		assert.NoError(t, err, "waiter %d", i)
	}
}
