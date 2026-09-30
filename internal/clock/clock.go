package clock

import (
	"sync"
	"time"
)

// Clock abstracts time for deterministic testing.
type Clock interface {
	Now() time.Time
}

// RealClock uses the system clock.
type RealClock struct{}

func (RealClock) Now() time.Time { return time.Now() }

// TestClock is manually controlled for tests.
type TestClock struct {
	mu  sync.Mutex
	now time.Time
}

func NewTestClock(start time.Time) *TestClock {
	return &TestClock{now: start}
}

func (c *TestClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *TestClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func (c *TestClock) Set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = t
}
