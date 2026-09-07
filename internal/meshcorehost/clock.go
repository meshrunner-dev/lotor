package meshcorehost

import "time"

// The reference's server-side figures, by name, for every owner that
// answers like one: how many answers a logged-in client may make a
// node emit per window, and the fixed pause before a server-style
// reply — the asker's radio needs a beat to turn around after
// transmitting (SERVER_RESPONSE_DELAY).
const (
	SessionLimit        = 6
	SessionLimitWindow  = time.Minute
	ServerResponseDelay = 300 * time.Millisecond
)

// UniqueClock is the reference's getCurrentTimeUnique: seconds since
// the epoch, strictly increasing within a run, because two things
// stamped alike — two posts to a client's cursor, two requests to a
// replay guard — would be one thing. The zero value is ready.
type UniqueClock struct {
	last uint32
}

// Now stamps at, or one second past the last stamp when at has not
// moved on. The owner supplies at, so an owner that keeps an offset
// from the wall clock — a station following its companion — applies
// it before asking.
func (u *UniqueClock) Now(at time.Time) uint32 {
	now := uint32(at.Unix())
	if now <= u.last {
		now = u.last + 1
	}
	u.last = now
	return now
}

// Observe tells the clock about a stamp minted before this run — a
// post read back from the store — so the next stamp lands after it.
func (u *UniqueClock) Observe(ts uint32) {
	u.last = max(u.last, ts)
}
