package ratelimiter

import (
	"sync"
	"testing"
	"time"
)

func newTestLimiter(t *testing.T, capacity, rate float64, idleTTL time.Duration) (*Limiter, *ManualClock) {
	t.Helper()
	clock := NewManualClock(time.Unix(1_700_000_000, 0))
	l, err := NewLimiter(capacity, rate, idleTTL, WithClock(clock))
	if err != nil {
		t.Fatalf("NewLimiter: %v", err)
	}
	return l, clock
}

func TestNewLimiterValidation(t *testing.T) {
	if _, err := NewLimiter(0, 1, time.Minute); err == nil {
		t.Error("expected error for zero capacity")
	}
	if _, err := NewLimiter(1, 0, time.Minute); err == nil {
		t.Error("expected error for zero rate")
	}
	if _, err := NewLimiter(1, 1, -time.Second); err == nil {
		t.Error("expected error for negative idle TTL")
	}
	if _, err := NewLimiter(1, 1, time.Minute, WithClock(nil)); err == nil {
		t.Error("expected error for nil clock")
	}
}

func TestFirstRequestStartsFull(t *testing.T) {
	l, _ := newTestLimiter(t, 3, 1, 0)
	for i := 0; i < 3; i++ {
		if !l.Allow("k") {
			t.Fatalf("request %d should be allowed: bucket starts full", i+1)
		}
	}
}

func TestTokensExhausted(t *testing.T) {
	l, _ := newTestLimiter(t, 2, 1, 0)
	if !l.Allow("k") || !l.Allow("k") {
		t.Fatal("first two requests should be allowed")
	}
	if l.Allow("k") {
		t.Fatal("third request should be denied: bucket exhausted")
	}
}

func TestRefillOverTime(t *testing.T) {
	l, clock := newTestLimiter(t, 5, 2, 0) // 2 tokens/second
	if !l.AllowN("k", 5) {
		t.Fatal("draining the bucket should succeed")
	}
	if l.Allow("k") {
		t.Fatal("no tokens should be available right after draining")
	}
	clock.Advance(1500 * time.Millisecond) // +3 tokens
	for i := 0; i < 3; i++ {
		if !l.Allow("k") {
			t.Fatalf("request %d after refill should be allowed", i+1)
		}
	}
	if l.Allow("k") {
		t.Fatal("only 3 tokens should have been refilled")
	}
}

func TestRefillCappedAtCapacity(t *testing.T) {
	l, clock := newTestLimiter(t, 3, 10, 0)
	if !l.Allow("k") {
		t.Fatal("first request should be allowed")
	}
	clock.Advance(time.Hour) // way more than enough to refill
	for i := 0; i < 3; i++ {
		if !l.Allow("k") {
			t.Fatalf("request %d should be allowed", i+1)
		}
	}
	if l.Allow("k") {
		t.Fatal("tokens must be capped at capacity")
	}
}

func TestAllowNAtomic(t *testing.T) {
	l, _ := newTestLimiter(t, 5, 1, 0)
	if !l.AllowN("k", 3) {
		t.Fatal("AllowN(3) on a full bucket of 5 should succeed")
	}
	// 2 tokens remain; asking for 3 must fail without partial deduction.
	if l.AllowN("k", 3) {
		t.Fatal("AllowN(3) with only 2 tokens should be denied")
	}
	if !l.AllowN("k", 2) {
		t.Fatal("AllowN(2) should succeed: denied call must not consume tokens")
	}
	if l.Allow("k") {
		t.Fatal("bucket should be empty now")
	}
}

func TestAllowNInvalidN(t *testing.T) {
	l, _ := newTestLimiter(t, 5, 1, 0)
	for _, n := range []int{0, -1, 6} {
		if l.AllowN("k", n) {
			t.Errorf("AllowN(%d) should be rejected", n)
		}
	}
	// Invalid calls must not touch state or statistics.
	if got := l.Stats(); got.Allowed != 0 || got.Denied != 0 || got.ActiveKeys != 0 {
		t.Errorf("invalid AllowN changed state: %v", got)
	}
	if !l.AllowN("k", 5) {
		t.Error("bucket should still be full after invalid calls")
	}
}

func TestKeysAreIndependent(t *testing.T) {
	l, _ := newTestLimiter(t, 1, 1, 0)
	if !l.Allow("a") {
		t.Fatal("first request for key a should be allowed")
	}
	if l.Allow("a") {
		t.Fatal("key a should be exhausted")
	}
	if !l.Allow("b") {
		t.Fatal("key b must have its own full bucket")
	}
}

func TestReset(t *testing.T) {
	l, _ := newTestLimiter(t, 2, 1, 0)
	l.Allow("k")
	l.Allow("k")
	if l.Allow("k") {
		t.Fatal("bucket should be exhausted")
	}
	l.Reset("k")
	if !l.AllowN("k", 2) {
		t.Fatal("after Reset the bucket should be full again")
	}
	l.Reset("unknown") // must be a no-op
}

func TestResetDoesNotTouchStats(t *testing.T) {
	l, _ := newTestLimiter(t, 2, 1, 0)
	l.Allow("a")
	l.Allow("b")
	l.Allow("b") // denied
	before := l.Stats()
	l.Reset("a")
	after := l.Stats()
	if after.Allowed != before.Allowed || after.Denied != before.Denied || after.Cleaned != before.Cleaned {
		t.Errorf("Reset changed counters: before=%v after=%v", before, after)
	}
	if after.ActiveKeys != before.ActiveKeys-1 {
		t.Errorf("Reset should only reduce ActiveKeys: before=%v after=%v", before, after)
	}
}

func TestCleanupIdle(t *testing.T) {
	// Slow refill (0.01/s) keeps refilled amounts negligible so the test
	// can verify the surviving key's token state is preserved.
	l, clock := newTestLimiter(t, 2, 0.01, time.Minute)
	l.Allow("old")
	clock.Advance(30 * time.Second)
	l.Allow("fresh")
	clock.Advance(31 * time.Second) // "old" idle for 61s, "fresh" for 31s

	if removed := l.CleanupIdle(); removed != 1 {
		t.Fatalf("CleanupIdle removed %d keys, want 1", removed)
	}
	stats := l.Stats()
	if stats.ActiveKeys != 1 || stats.Cleaned != 1 {
		t.Fatalf("unexpected stats after cleanup: %v", stats)
	}

	// The surviving key must keep its token state (1 token left).
	if !l.Allow("fresh") {
		t.Fatal("surviving key should still have its remaining token")
	}
	if l.Allow("fresh") {
		t.Fatal("surviving key should be exhausted now")
	}

	// The cleaned key starts over with a full bucket.
	if !l.AllowN("old", 2) {
		t.Fatal("cleaned key should restart with a full bucket")
	}
}

func TestCleanupIdleDisabled(t *testing.T) {
	l, clock := newTestLimiter(t, 1, 1, 0) // idleTTL = 0 disables cleanup
	l.Allow("k")
	clock.Advance(time.Hour)
	if removed := l.CleanupIdle(); removed != 0 {
		t.Fatalf("cleanup with disabled TTL removed %d keys", removed)
	}
	if got := l.Stats().ActiveKeys; got != 1 {
		t.Fatalf("ActiveKeys = %d, want 1", got)
	}
}

func TestConcurrentNoOversell(t *testing.T) {
	const capacity = 100
	l, _ := newTestLimiter(t, capacity, 1, 0) // manual clock never advances

	var wg sync.WaitGroup
	for g := 0; g < 50; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 10; i++ {
				l.Allow("shared")
			}
		}()
	}
	wg.Wait()

	stats := l.Stats()
	if stats.Allowed != capacity {
		t.Errorf("allowed = %d, want exactly %d (no oversell)", stats.Allowed, capacity)
	}
	if stats.Denied != 500-capacity {
		t.Errorf("denied = %d, want %d", stats.Denied, 500-capacity)
	}
}

func TestConcurrentIndependentKeys(t *testing.T) {
	l, _ := newTestLimiter(t, 10, 1, 0)
	keys := []string{"a", "b", "c", "d"}
	var wg sync.WaitGroup
	for _, key := range keys {
		for g := 0; g < 5; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < 4; i++ {
					l.Allow(key)
				}
			}()
		}
	}
	wg.Wait()
	stats := l.Stats()
	if stats.Allowed != 40 { // 4 keys * 10 tokens
		t.Errorf("allowed = %d, want 40", stats.Allowed)
	}
	if stats.ActiveKeys != len(keys) {
		t.Errorf("activeKeys = %d, want %d", stats.ActiveKeys, len(keys))
	}
}

func TestStats(t *testing.T) {
	l, clock := newTestLimiter(t, 2, 1, time.Minute)
	l.Allow("a")     // allowed: 1
	l.AllowN("a", 2) // denied (only 1 left)
	l.Allow("a")     // allowed: 2 total
	l.Allow("a")     // denied
	l.Allow("b")     // allowed: 3 total
	clock.Advance(2 * time.Minute)
	l.CleanupIdle() // removes a and b

	stats := l.Stats()
	want := Stats{Allowed: 3, Denied: 2, ActiveKeys: 0, Cleaned: 2}
	if stats != want {
		t.Errorf("stats = %+v, want %+v", stats, want)
	}
}
