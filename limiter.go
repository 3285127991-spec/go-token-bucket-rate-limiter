// Package ratelimiter implements a concurrent, per-key token bucket rate
// limiter with an injectable clock, idle-key cleanup and usage statistics.
package ratelimiter

import (
	"fmt"
	"math"
	"sync"
	"time"
)

// bucket holds the mutable state of a single key.
type bucket struct {
	tokens     float64   // current token balance, never exceeds capacity
	lastRefill time.Time // last time tokens were lazily refilled
	lastSeen   time.Time // last time the key was accessed, for idle cleanup
}

// Limiter is a per-key token bucket rate limiter.
//
// Each distinct string key owns an independent bucket that starts full.
// Tokens refill continuously at a fixed rate up to the capacity. All methods
// are safe for concurrent use; a single mutex guards the whole limiter, which
// makes AllowN check-and-consume atomic.
type Limiter struct {
	mu           sync.Mutex
	capacity     float64 // max tokens per bucket
	refillPerSec float64 // tokens added per second
	idleTTL      time.Duration
	clock        Clock
	buckets      map[string]*bucket

	allowed     int64 // requests granted
	denied      int64 // requests rejected
	cleanedKeys int64 // buckets removed by CleanupIdle
}

// Option customizes a Limiter.
type Option func(*Limiter)

// WithClock injects a custom Clock. A nil clock is ignored.
func WithClock(c Clock) Option {
	return func(l *Limiter) {
		if c != nil {
			l.clock = c
		}
	}
}

// NewLimiter creates a Limiter.
//
// capacity is the maximum number of tokens each key's bucket can hold and
// must be positive. refillPerSecond is the (fractional) number of tokens
// added per second and must be >= 0; 0 means buckets never refill. idleTTL
// is the idle time after which CleanupIdle removes a key's bucket; a
// non-positive value disables idle cleanup. Invalid capacity or refill rate
// panics, as they indicate a programming error.
func NewLimiter(capacity int64, refillPerSecond float64, idleTTL time.Duration, opts ...Option) *Limiter {
	if capacity <= 0 {
		panic(fmt.Sprintf("ratelimiter: capacity must be positive, got %d", capacity))
	}
	if refillPerSecond < 0 {
		panic(fmt.Sprintf("ratelimiter: refill rate must be >= 0, got %v", refillPerSecond))
	}
	l := &Limiter{
		capacity:     float64(capacity),
		refillPerSec: refillPerSecond,
		idleTTL:      idleTTL,
		clock:        RealClock{},
		buckets:      make(map[string]*bucket),
	}
	for _, opt := range opts {
		opt(l)
	}
	return l
}

// Allow consumes 1 token for key and reports whether the request is allowed.
func (l *Limiter) Allow(key string) bool {
	return l.AllowN(key, 1)
}

// AllowN atomically consumes n tokens for key: either all n tokens are
// consumed and true is returned, or nothing is consumed and false is
// returned. Partial consumption never happens.
//
// n <= 0 is a no-op that reports success and is not counted in statistics.
// n greater than the capacity can never be satisfied and is always denied.
func (l *Limiter) AllowN(key string, n int64) bool {
	if n <= 0 {
		return true
	}
	now := l.clock.Now()
	l.mu.Lock()
	defer l.mu.Unlock()
	b := l.bucketLocked(key, now)
	if float64(n) > b.tokens {
		l.denied++
		return false
	}
	b.tokens -= float64(n)
	l.allowed++
	return true
}

// bucketLocked returns the bucket for key, creating it full on first use and
// lazily refilling it based on the elapsed time. l.mu must be held.
func (l *Limiter) bucketLocked(key string, now time.Time) *bucket {
	b, ok := l.buckets[key]
	if !ok {
		b = &bucket{tokens: l.capacity, lastRefill: now}
		l.buckets[key] = b
	} else if elapsed := now.Sub(b.lastRefill).Seconds(); elapsed > 0 {
		b.tokens = math.Min(l.capacity, b.tokens+elapsed*l.refillPerSec)
		b.lastRefill = now
	}
	b.lastSeen = now
	return b
}

// Reset removes the bucket for key, so the next request for that key starts
// again with a full bucket. Resetting a non-existent key is a no-op.
//
// Reset does not modify the Allowed, Denied or CleanedKeys counters; it only
// reduces ActiveKeys (when the key existed) until the key is used again.
func (l *Limiter) Reset(key string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.buckets, key)
}

// CleanupIdle removes every bucket that has not been accessed for at least
// the configured idle TTL and returns the number of removed buckets. Only
// the removed buckets are affected; the token state of all other keys is
// left untouched. A non-positive idle TTL disables cleanup, in which case
// CleanupIdle always returns 0.
func (l *Limiter) CleanupIdle() int {
	if l.idleTTL <= 0 {
		return 0
	}
	now := l.clock.Now()
	l.mu.Lock()
	defer l.mu.Unlock()
	removed := 0
	for key, b := range l.buckets {
		if now.Sub(b.lastSeen) >= l.idleTTL {
			delete(l.buckets, key)
			removed++
		}
	}
	l.cleanedKeys += int64(removed)
	return removed
}
