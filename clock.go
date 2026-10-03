package ratelimiter

import (
	"sync"
	"time"
)

// Clock abstracts time so tests can control refill and idle cleanup
// without real sleeps.
type Clock interface {
	Now() time.Time
}

// RealClock uses the system time. It is the default clock.
type RealClock struct{}

// Now returns the current system time.
func (RealClock) Now() time.Time { return time.Now() }

// ManualClock is a controllable Clock for tests. It is safe for
// concurrent use.
type ManualClock struct {
	mu  sync.Mutex
	now time.Time
}

// NewManualClock creates a ManualClock starting at the given time.
func NewManualClock(start time.Time) *ManualClock {
	return &ManualClock{now: start}
}

// Now returns the clock's current time.
func (c *ManualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// Advance moves the clock forward by d.
func (c *ManualClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}
