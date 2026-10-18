package store

import (
	"sync"
	"time"
)

// AccessDelta holds accumulated access metadata for a single key.
type AccessDelta struct {
	Namespace      string
	Key            string
	LastAccessedAt time.Time
	CountDelta     uint64
}

// AccessTracker accumulates access metadata in memory. The leader-only
// TouchFlusher periodically drains it and proposes a TouchCommand through Raft.
type AccessTracker struct {
	mu      sync.Mutex
	pending map[string]*AccessDelta // keyed by "namespace\x00key"
}

// NewAccessTracker creates a new tracker.
func NewAccessTracker() *AccessTracker {
	return &AccessTracker{
		pending: make(map[string]*AccessDelta),
	}
}

// Track records an access to the given key.
func (t *AccessTracker) Track(namespace, key string, accessedAt time.Time) {
	id := namespace + "\x00" + key
	t.mu.Lock()
	defer t.mu.Unlock()
	d, ok := t.pending[id]
	if !ok {
		d = &AccessDelta{Namespace: namespace, Key: key}
		t.pending[id] = d
	}
	d.CountDelta++
	if accessedAt.After(d.LastAccessedAt) {
		d.LastAccessedAt = accessedAt
	}
}

// Drain returns and resets all accumulated deltas.
func (t *AccessTracker) Drain() []AccessDelta {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.pending) == 0 {
		return nil
	}
	result := make([]AccessDelta, 0, len(t.pending))
	for _, d := range t.pending {
		result = append(result, *d)
	}
	t.pending = make(map[string]*AccessDelta)
	return result
}
