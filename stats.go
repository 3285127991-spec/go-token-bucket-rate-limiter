package ratelimiter

// Stats is a point-in-time snapshot of a Limiter's counters.
type Stats struct {
	// Allowed is the total number of Allow/AllowN calls that were granted.
	Allowed int64
	// Denied is the total number of Allow/AllowN calls that were rejected.
	Denied int64
	// ActiveKeys is the number of keys currently holding a bucket.
	ActiveKeys int
	// CleanedKeys is the cumulative number of buckets removed by CleanupIdle.
	CleanedKeys int64
}

// Stats returns a consistent snapshot of the limiter's counters.
//
// Counters are monotonic: Reset and CleanupIdle never decrease Allowed or
// Denied, and only CleanupIdle increments CleanedKeys.
func (l *Limiter) Stats() Stats {
	l.mu.Lock()
	defer l.mu.Unlock()
	return Stats{
		Allowed:     l.allowed,
		Denied:      l.denied,
		ActiveKeys:  len(l.buckets),
		CleanedKeys: l.cleanedKeys,
	}
}
