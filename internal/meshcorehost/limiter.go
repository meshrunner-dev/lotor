package meshcorehost

import "time"

// RateLimiter is a fixed window of allowed events: the shape the
// reference's RateLimiter has, standing behind every answer a stranger
// can demand so a request flood cannot turn the node into an
// amplifier. Refused work costs the price of its reception, nothing
// more: no packet is built, nothing reaches a queue.
type RateLimiter struct {
	Max    int
	Window time.Duration
	start  time.Time
	count  int
}

// Allow consumes one slot; the window opens on the first event after
// the previous one expired. A budget nobody set grants nothing: these
// are the only defence against being made an amplifier, so the zero
// value refuses rather than permits.
func (r *RateLimiter) Allow(now time.Time) bool {
	if r.Max <= 0 {
		return false
	}
	if now.Before(r.start.Add(r.Window)) {
		r.count++
		return r.count <= r.Max
	}
	r.start, r.count = now, 1
	return true
}
