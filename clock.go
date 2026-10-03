package ratelimiter

import (
	"sync"
	"time"
)

// Clock abstracts the source of time so that refill and idle-cleanup logic
// can be tested without sleeping in real time.
type Clock interface {
	Now() time.Time
}

// RealClock reads the system clock. It is the default Clock used by Limiter.
type RealClock struct{}

// Now returns the current system time.
func (RealClock) Now() time.Time { return time.Now() }

// FakeClock is a Clock whose time only moves when Advance is called.
// It is safe for concurrent use and is intended for tests and demos that
// must not depend on real sleeps.
type FakeClock struct {
	mu  sync.Mutex
	now time.Time
}

// NewFakeClock returns a FakeClock starting at start.
func NewFakeClock(start time.Time) *FakeClock {
	return &FakeClock{now: start}
}

// Now returns the fake current time.
func (f *FakeClock) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

// Advance moves the fake clock forward by d.
func (f *FakeClock) Advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = f.now.Add(d)
}
