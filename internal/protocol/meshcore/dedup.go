package meshcore

import (
	"time"

	"meshrunner.dev/pkg/meshcore"

	"meshrunner.dev/lotor/internal/txn"
)

// seenTable remembers packet hashes long enough to suppress the
// copies a flood sends back, and keeps the transaction that carried
// the first copy so log chains can point at it. Bounded in entries
// and in time; eviction is oldest-first.
type seenTable struct {
	ttl     time.Duration
	max     int
	entries map[[meshcore.MaxHashSize]byte]seenEntry
}

type seenEntry struct {
	txn txn.ID
	at  time.Time
}

func newSeenTable(ttl time.Duration, maxEntries int) *seenTable {
	return &seenTable{
		ttl:     ttl,
		max:     maxEntries,
		entries: make(map[[meshcore.MaxHashSize]byte]seenEntry, maxEntries),
	}
}

// witness records the hash if it is new and returns the transaction
// of the first copy otherwise.
func (t *seenTable) witness(h [meshcore.MaxHashSize]byte, id txn.ID, now time.Time) (txn.ID, bool) {
	if e, ok := t.entries[h]; ok && now.Sub(e.at) < t.ttl {
		return e.txn, true
	}
	t.sweep(now)
	t.entries[h] = seenEntry{txn: id, at: now}
	return txn.ID{}, false
}

func (t *seenTable) sweep(now time.Time) {
	for h, e := range t.entries {
		if now.Sub(e.at) >= t.ttl {
			delete(t.entries, h)
		}
	}
	// Still full of fresh entries: drop the oldest to stay bounded.
	for len(t.entries) >= t.max {
		var oldest [meshcore.MaxHashSize]byte
		first := true
		for h, e := range t.entries {
			if first || e.at.Before(t.entries[oldest].at) {
				oldest, first = h, false
			}
		}
		delete(t.entries, oldest)
	}
}
