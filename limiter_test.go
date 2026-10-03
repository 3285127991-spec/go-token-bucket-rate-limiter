package ratelimiter

import (
	"sync"
	"testing"
	"time"
)

func TestFirstRequestHasFullCapacity(t *testing.T) {
	l := NewLimiter(3, 1, time.Minute, WithClock(NewFakeClock(time.Now())))
	if !l.AllowN("k", 3) {
		t.Fatal("first request should be able to consume the full capacity")
	}
	if l.Allow("k") {
		t.Fatal("bucket should be empty after consuming the full capacity")
	}
}

func TestTokenExhaustion(t *testing.T) {
	l := NewLimiter(3, 0, time.Minute, WithClock(NewFakeClock(time.Now())))
	for i := 0; i < 3; i++ {
		if !l.Allow("k") {
			t.Fatalf("request %d should be allowed", i+1)
		}
	}
	if l.Allow("k") {
		t.Fatal("request beyond capacity should be denied")
	}
}

func TestRefillOverTime(t *testing.T) {
	clock := NewFakeClock(time.Now())
	l := NewLimiter(4, 2, time.Minute, WithClock(clock)) // 2 tokens/second
	if !l.AllowN("k", 4) {
		t.Fatal("should start full")
	}
	clock.Advance(time.Second) // +2 tokens
	if !l.Allow("k") || !l.Allow("k") {
		t.Fatal("2 tokens should have refilled after 1s")
	}
	if l.Allow("k") {
		t.Fatal("only 2 tokens should have refilled")
	}
	clock.Advance(1500 * time.Millisecond) // +3 tokens
	if !l.AllowN("k", 3) {
		t.Fatal("3 tokens should have refilled after another 1.5s")
	}
}

func TestRefillCappedAtCapacity(t *testing.T) {
	clock := NewFakeClock(time.Now())
	l := NewLimiter(3, 10, time.Minute, WithClock(clock))
	if !l.AllowN("k", 3) {
		t.Fatal("should start full")
	}
	clock.Advance(time.Hour) // would add 36000 tokens without a cap
	if !l.AllowN("k", 3) {
		t.Fatal("bucket should be full again after a long wait")
	}
	if l.Allow("k") {
		t.Fatal("tokens must be capped at capacity")
	}
}

func TestAllowNAtomicNoPartialConsumption(t *testing.T) {
	l := NewLimiter(5, 0, time.Minute, WithClock(NewFakeClock(time.Now())))
	if !l.AllowN("k", 3) {
		t.Fatal("AllowN(3) on a full bucket of 5 should succeed")
	}
	if l.AllowN("k", 3) {
		t.Fatal("AllowN(3) with only 2 tokens left must be denied")
	}
	// The failed AllowN must not have consumed anything.
	if !l.AllowN("k", 2) {
		t.Fatal("the 2 remaining tokens must still be available")
	}
	if l.Allow("k") {
		t.Fatal("bucket should now be empty")
	}
}

func TestAllowNGreaterThanCapacity(t *testing.T) {
	clock := NewFakeClock(time.Now())
	l := NewLimiter(2, 100, time.Minute, WithClock(clock))
	if l.AllowN("k", 3) {
		t.Fatal("AllowN above capacity can never be satisfied")
	}
	clock.Advance(time.Hour)
	if l.AllowN("k", 3) {
		t.Fatal("AllowN above capacity stays impossible even after refilling")
	}
}

func TestAllowNNonPositiveIsNoOp(t *testing.T) {
	l := NewLimiter(1, 0, time.Minute, WithClock(NewFakeClock(time.Now())))
	if !l.AllowN("k", 0) || !l.AllowN("k", -5) {
		t.Fatal("n <= 0 should report success")
	}
	if got := l.Stats(); got.Allowed != 0 || got.Denied != 0 || got.ActiveKeys != 0 {
		t.Fatalf("n <= 0 must not touch state or stats, got %+v", got)
	}
}

func TestKeyIsolation(t *testing.T) {
	l := NewLimiter(2, 0, time.Minute, WithClock(NewFakeClock(time.Now())))
	if !l.Allow("a") || !l.Allow("a") || l.Allow("a") {
		t.Fatal("key a should be limited to its capacity")
	}
	if !l.Allow("b") || !l.Allow("b") || l.Allow("b") {
		t.Fatal("key b must have its own independent bucket")
	}
	if got := l.Stats().ActiveKeys; got != 2 {
		t.Fatalf("expected 2 active keys, got %d", got)
	}
}

func TestReset(t *testing.T) {
	l := NewLimiter(2, 0, time.Minute, WithClock(NewFakeClock(time.Now())))
	l.Allow("k")
	l.Allow("k")
	if l.Allow("k") {
		t.Fatal("bucket should be exhausted")
	}
	l.Reset("k")
	if !l.Allow("k") {
		t.Fatal("bucket should be full again after Reset")
	}
	l.Reset("unknown") // must be a no-op
	if got := l.Stats().ActiveKeys; got != 1 {
		t.Fatalf("resetting an unknown key must not create state, got %d active keys", got)
	}
}

func TestResetDoesNotAffectCounters(t *testing.T) {
	l := NewLimiter(2, 0, time.Minute, WithClock(NewFakeClock(time.Now())))
	l.Allow("k")
	l.Allow("k")
	l.Allow("k") // denied
	l.Reset("k")
	got := l.Stats()
	if got.Allowed != 2 || got.Denied != 1 || got.CleanedKeys != 0 {
		t.Fatalf("Reset must not change counters, got %+v", got)
	}
	if got.ActiveKeys != 0 {
		t.Fatalf("Reset removes the bucket, expected 0 active keys, got %d", got.ActiveKeys)
	}
}

func TestCleanupIdle(t *testing.T) {
	clock := NewFakeClock(time.Now())
	l := NewLimiter(2, 0, time.Minute, WithClock(clock))
	l.Allow("a")
	clock.Advance(30 * time.Second)
	l.Allow("b")
	clock.Advance(31 * time.Second) // a idle for 61s, b idle for 31s

	if removed := l.CleanupIdle(); removed != 1 {
		t.Fatalf("expected 1 removed bucket, got %d", removed)
	}
	got := l.Stats()
	if got.ActiveKeys != 1 || got.CleanedKeys != 1 {
		t.Fatalf("expected 1 active and 1 cleaned key, got %+v", got)
	}

	// The surviving key keeps its exact token state (1 of 2 tokens used).
	if !l.Allow("b") || l.Allow("b") {
		t.Fatal("cleanup must not disturb the token state of other keys")
	}
	// The cleaned key starts over with a full bucket.
	if !l.AllowN("a", 2) {
		t.Fatal("a cleaned key should restart with a full bucket")
	}
}

func TestCleanupIdleBoundary(t *testing.T) {
	clock := NewFakeClock(time.Now())
	l := NewLimiter(1, 0, time.Minute, WithClock(clock))
	l.Allow("k")
	clock.Advance(time.Minute - time.Nanosecond)
	if removed := l.CleanupIdle(); removed != 0 {
		t.Fatalf("key idle for less than the TTL must survive, removed %d", removed)
	}
	clock.Advance(time.Nanosecond) // idle for exactly the TTL
	if removed := l.CleanupIdle(); removed != 1 {
		t.Fatalf("key idle for exactly the TTL should be removed, removed %d", removed)
	}
}

func TestCleanupIdleDisabled(t *testing.T) {
	clock := NewFakeClock(time.Now())
	l := NewLimiter(1, 0, 0, WithClock(clock)) // non-positive TTL disables cleanup
	l.Allow("k")
	clock.Advance(24 * time.Hour)
	if removed := l.CleanupIdle(); removed != 0 {
		t.Fatalf("cleanup is disabled, removed %d", removed)
	}
	if got := l.Stats(); got.ActiveKeys != 1 || got.CleanedKeys != 0 {
		t.Fatalf("unexpected stats %+v", got)
	}
}

func TestStats(t *testing.T) {
	clock := NewFakeClock(time.Now())
	l := NewLimiter(2, 0, time.Minute, WithClock(clock))
	l.Allow("a")     // allowed
	l.Allow("a")     // allowed
	l.Allow("a")     // denied
	l.AllowN("b", 2) // allowed
	l.AllowN("b", 1) // denied
	l.AllowN("c", 0) // no-op, not counted
	if got := l.Stats(); got.Allowed != 3 || got.Denied != 2 || got.ActiveKeys != 2 || got.CleanedKeys != 0 {
		t.Fatalf("unexpected stats %+v", got)
	}
	clock.Advance(2 * time.Minute)
	l.CleanupIdle()
	if got := l.Stats(); got.ActiveKeys != 0 || got.CleanedKeys != 2 || got.Allowed != 3 || got.Denied != 2 {
		t.Fatalf("unexpected stats after cleanup %+v", got)
	}
}

func TestConcurrentAllow(t *testing.T) {
	const capacity = 1000
	const goroutines = 100
	const perGoroutine = 100
	l := NewLimiter(capacity, 0, time.Minute) // refill 0: exactly capacity grants

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				l.Allow("shared")
			}
		}()
	}
	wg.Wait()

	got := l.Stats()
	if got.Allowed != capacity {
		t.Fatalf("over-issue detected: allowed=%d, want %d", got.Allowed, capacity)
	}
	if got.Denied != goroutines*perGoroutine-capacity {
		t.Fatalf("denied=%d, want %d", got.Denied, goroutines*perGoroutine-capacity)
	}
}

func TestConcurrentAllowNAtomic(t *testing.T) {
	const capacity = 100
	const goroutines = 50
	l := NewLimiter(capacity, 0, time.Minute)

	var wg sync.WaitGroup
	var granted int64
	var mu sync.Mutex
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if l.AllowN("shared", 7) {
				mu.Lock()
				granted++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if granted != 14 { // floor(100/7)
		t.Fatalf("granted=%d, want 14", granted)
	}
	// 14*7 = 98 tokens consumed, exactly 2 must remain.
	if !l.AllowN("shared", 2) {
		t.Fatal("exactly 2 tokens should remain")
	}
	if l.Allow("shared") {
		t.Fatal("bucket should be empty now")
	}
}

func TestConcurrentIndependentKeys(t *testing.T) {
	const capacity = 50
	const goroutines = 20
	const perGoroutine = 10
	l := NewLimiter(capacity, 0, time.Minute)

	var wg sync.WaitGroup
	for _, key := range []string{"a", "b"} {
		for g := 0; g < goroutines; g++ {
			wg.Add(1)
			go func(key string) {
				defer wg.Done()
				for i := 0; i < perGoroutine; i++ {
					l.Allow(key)
				}
			}(key)
		}
	}
	wg.Wait()

	if got := l.Stats(); got.Allowed != 2*capacity {
		t.Fatalf("allowed=%d, want %d (each key independent)", got.Allowed, 2*capacity)
	}
}

func TestConcurrentMixedOperations(t *testing.T) {
	clock := NewFakeClock(time.Now())
	l := NewLimiter(10, 5, time.Millisecond, WithClock(clock))

	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			key := string(rune('a' + g%3))
			for i := 0; i < 200; i++ {
				l.Allow(key)
				l.AllowN(key, 2)
				l.Stats()
				if i%10 == 0 {
					l.Reset(key)
					clock.Advance(time.Millisecond)
					l.CleanupIdle()
				}
			}
		}(g)
	}
	wg.Wait()
}
