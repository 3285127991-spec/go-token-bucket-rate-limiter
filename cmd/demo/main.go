// Command demo is a small CLI demonstration of the ratelimiter package.
package main

import (
	"fmt"
	"time"

	"ratelimiter"
)

func main() {
	// Capacity 3, refills 1 token/second, keys idle for over 2s can be
	// cleaned up.
	l, err := ratelimiter.NewLimiter(3, 1, 2*time.Second)
	if err != nil {
		panic(err)
	}

	fmt.Println("== burst of 5 requests for key alice (capacity 3) ==")
	for i := 1; i <= 5; i++ {
		fmt.Printf("request %d: %s", i, verdict(l.Allow("alice")))
		fmt.Println()
	}

	fmt.Println()
	fmt.Println("== waiting 1.5s for refill (1 token/s) ==")
	time.Sleep(1500 * time.Millisecond)
	fmt.Println("request after refill:", verdict(l.Allow("alice")))

	fmt.Println()
	fmt.Println("== key bob has an independent bucket ==")
	fmt.Println("bob's first request:", verdict(l.Allow("bob")))

	fmt.Println()
	fmt.Println("== AllowN(2) is atomic ==")
	fmt.Println("bob AllowN(2):", verdict(l.AllowN("bob", 2)))
	fmt.Println("bob AllowN(2) again (only 1 token left):", verdict(l.AllowN("bob", 2)))

	fmt.Println()
	fmt.Println("== reset alice ==")
	l.Reset("alice")
	fmt.Println("alice AllowN(3) after reset:", verdict(l.AllowN("alice", 3)))

	fmt.Println()
	fmt.Println("== idle for 3s, then CleanupIdle ==")
	time.Sleep(3 * time.Second)
	fmt.Printf("removed %d idle key(s)", l.CleanupIdle())
	fmt.Println()

	fmt.Println()
	fmt.Println("final stats:", l.Stats())
}

func verdict(ok bool) string {
	if ok {
		return "ALLOWED"
	}
	return "DENIED"
}
