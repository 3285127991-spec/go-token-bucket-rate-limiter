package ratelimiter

import (
	"testing"
	"time"
)

func TestFakeClock(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := NewFakeClock(start)
	if got := clock.Now(); !got.Equal(start) {
		t.Fatalf("Now() = %v, want %v", got, start)
	}
	clock.Advance(90 * time.Second)
	if got := clock.Now(); !got.Equal(start.Add(90 * time.Second)) {
		t.Fatalf("Now() after Advance = %v, want %v", got, start.Add(90*time.Second))
	}
}

func TestRealClock(t *testing.T) {
	before := time.Now()
	got := RealClock{}.Now()
	after := time.Now()
	if got.Before(before) || got.After(after) {
		t.Fatalf("RealClock.Now() = %v, want between %v and %v", got, before, after)
	}
}
