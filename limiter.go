// Package ratelimiter implements a concurrent, per-key token bucket
// rate limiter with lazy refill, idle-key cleanup and statistics.
package ratelimiter

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

// bucket holds the token state of a single key.
type bucket struct {
	tokens float64   // current token count, never exceeds capacity
	last   time.Time // last time this bucket was touched (refill point)
}

// Stats is a point-in-time snapshot of Limiter counters.
type Stats struct {
	Allowed    uint64 // total granted requests (sum of consumed tokens via Allow/AllowN)
	Denied     uint64 // total denied Allow/AllowN calls
	ActiveKeys int    // number of keys currently tracked
	Cleaned    uint64 // total keys removed by CleanupIdle
}

// Limiter is a per-key token bucket rate limiter. It is safe for
// concurrent use by multiple goroutines.
type Limiter struct {
	capacity float64
	rate     float64 // tokens refilled per second
	idleTTL  time.Duration
	clock    Clock

	mu      sync.Mutex
	buckets map[string]*bucket
	allowed uint64
	denied  uint64
	cleaned uint64
}

// Option customizes a Limiter.
type Option func(*Limiter)

// WithClock injects a custom Clock (e.g. ManualClock in tests).
func WithClock(c Clock) Option {
	return func(l *Limiter) { l.clock = c }
}

// NewLimiter creates a Limiter.
//
//   - capacity: maximum tokens per bucket (also the initial token count).
//   - ratePerSecond: token refill rate; tokens refill continuously over
//     time and never exceed capacity.
//   - idleTTL: a key not touched for longer than this becomes eligible
//     for removal by CleanupIdle. Use 0 to disable idle cleanup.
//
// It returns an error if capacity or ratePerSecond is not positive, or
// idleTTL is negative.
func NewLimiter(capacity, ratePerSecond float64, idleTTL time.Duration, opts ...Option) (*Limiter, error) {
	if capacity <= 0 {
		return nil, errors.New("ratelimiter: capacity must be positive")
	}
	if ratePerSecond <= 0 {
		return nil, errors.New("ratelimiter: ratePerSecond must be positive")
	}
	if idleTTL < 0 {
		return nil, errors.New("ratelimiter: idleTTL must not be negative")
	}
	l := &Limiter{
		capacity: capacity,
		rate:     ratePerSecond,
		idleTTL:  idleTTL,
		clock:    RealClock{},
		buckets:  make(map[string]*bucket),
	}
	for _, opt := range opts {
		opt(l)
	}
	if l.clock == nil {
		return nil, errors.New("ratelimiter: clock must not be nil")
	}
	return l, nil
}

// Allow reports whether one token may be consumed for key.
func (l *Limiter) Allow(key string) bool {
	return l.AllowN(key, 1)
}

// AllowN atomically consumes n tokens for key and reports whether the
// request is granted. It is all-or-nothing: if fewer than n tokens are
// available, nothing is consumed. n must be >= 1 and <= capacity;
// otherwise AllowN returns false without touching state or statistics.
func (l *Limiter) AllowN(key string, n int) bool {
	if n < 1 || float64(n) > l.capacity {
		return false
	}
	now := l.clock.Now()

	l.mu.Lock()
	defer l.mu.Unlock()

	b := l.bucketLocked(key, now)
	l.refillLocked(b, now)

	cost := float64(n)
	if b.tokens < cost {
		l.denied++
		return false
	}
	b.tokens -= cost
	b.last = now
	l.allowed += uint64(n)
	return true
}

// Reset removes the bucket for key, so the next request for that key
// starts again with a full bucket. It is a no-op for unknown keys.
//
// Reset does NOT modify the Allowed, Denied or Cleaned statistics; it
// only reduces ActiveKeys (by removing the key's bucket).
func (l *Limiter) Reset(key string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.buckets, key)
}

// CleanupIdle removes every key that has not been touched for longer
// than the configured idle TTL and returns the number of removed keys.
// Keys removed this way are counted in the Cleaned statistic. Other
// keys' buckets and token states are left untouched. If the idle TTL
// is 0, CleanupIdle does nothing.
func (l *Limiter) CleanupIdle() int {
	if l.idleTTL <= 0 {
		return 0
	}
	now := l.clock.Now()

	l.mu.Lock()
	defer l.mu.Unlock()

	removed := 0
	for key, b := range l.buckets {
		if now.Sub(b.last) > l.idleTTL {
			delete(l.buckets, key)
			removed++
		}
	}
	l.cleaned += uint64(removed)
	return removed
}

// Stats returns a snapshot of the limiter's statistics.
func (l *Limiter) Stats() Stats {
	l.mu.Lock()
	defer l.mu.Unlock()
	return Stats{
		Allowed:    l.allowed,
		Denied:     l.denied,
		ActiveKeys: len(l.buckets),
		Cleaned:    l.cleaned,
	}
}

// bucketLocked returns the bucket for key, creating a full one if the
// key is new. Callers must hold l.mu.
func (l *Limiter) bucketLocked(key string, now time.Time) *bucket {
	b, ok := l.buckets[key]
	if !ok {
		b = &bucket{tokens: l.capacity, last: now}
		l.buckets[key] = b
	}
	return b
}

// refillLocked adds tokens accrued since the last touch, capped at
// capacity. Callers must hold l.mu.
func (l *Limiter) refillLocked(b *bucket, now time.Time) {
	elapsed := now.Sub(b.last)
	if elapsed <= 0 {
		return
	}
	b.tokens += elapsed.Seconds() * l.rate
	if b.tokens > l.capacity {
		b.tokens = l.capacity
	}
	b.last = now
}

// String returns a short human-readable summary, handy for the CLI demo.
func (s Stats) String() string {
	return fmt.Sprintf("allowed=%d denied=%d activeKeys=%d cleaned=%d",
		s.Allowed, s.Denied, s.ActiveKeys, s.Cleaned)
}
