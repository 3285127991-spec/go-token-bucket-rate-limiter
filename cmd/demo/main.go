// Command demo is a small CLI walkthrough of the ratelimiter library.
package main

import (
	"fmt"
	"time"

	"ratelimiter"
)

func verdict(ok bool) string {
	if ok {
		return "allowed"
	}
	return "denied"
}

func printStats(l *ratelimiter.Limiter) {
	s := l.Stats()
	fmt.Printf("stats: allowed=%d denied=%d activeKeys=%d cleanedKeys=%d\n",
		s.Allowed, s.Denied, s.ActiveKeys, s.CleanedKeys)
}

func main() {
	// Capacity 5, refill 2 tokens/second, keys idle for 30s can be cleaned.
	limiter := ratelimiter.NewLimiter(5, 2, 30*time.Second)

	fmt.Println("== burst of 7 requests for user-1 (capacity 5) ==")
	for i := 1; i <= 7; i++ {
		fmt.Printf("request %d: %s\n", i, verdict(limiter.Allow("user-1")))
	}

	fmt.Println("\n== user-2 owns an independent bucket ==")
	fmt.Println("user-2 first request:", verdict(limiter.Allow("user-2")))

	fmt.Println("\n== AllowN consumes atomically ==")
	fmt.Println("AllowN(user-3, 3):", verdict(limiter.AllowN("user-3", 3)))
	fmt.Println("AllowN(user-3, 3) again (only 2 left):", verdict(limiter.AllowN("user-3", 3)))

	fmt.Println("\n== sleep 1s: user-1 refills 2 tokens ==")
	time.Sleep(1 * time.Second)
	fmt.Println("user-1:", verdict(limiter.Allow("user-1")))
	fmt.Println("user-1:", verdict(limiter.Allow("user-1")))
	fmt.Println("user-1:", verdict(limiter.Allow("user-1")))

	fmt.Println("\n== Reset(user-1) restores a full bucket ==")
	limiter.Reset("user-1")
	fmt.Println("user-1 after reset:", verdict(limiter.Allow("user-1")))

	fmt.Println("\n== idle cleanup (separate limiter with 200ms idle TTL) ==")
	idleDemo := ratelimiter.NewLimiter(2, 1, 200*time.Millisecond)
	idleDemo.Allow("stale-key")
	idleDemo.Allow("fresh-key")
	time.Sleep(300 * time.Millisecond)
	idleDemo.Allow("fresh-key") // keep fresh-key active
	fmt.Println("removed buckets:", idleDemo.CleanupIdle())
	printStats(idleDemo)

	fmt.Println("\n== final stats of the main limiter ==")
	printStats(limiter)
}
