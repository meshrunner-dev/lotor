package meshcore

import (
	"time"

	"meshrunner.dev/pkg/meshcore"

	"meshrunner.dev/lotor/internal/txn"
)

// referenceCapacity mirrors the reference's packet-hash ring
// (SimpleMeshTables MAX_PACKET_HASHES): dedup there is bounded by
// count, not by time — a hash lives until 160 newer ones push it out.
const referenceCapacity = 128 + 32

// seenTable suppresses the copies a flood sends back. It is bounded
// the reference's way — a fixed-capacity ring, oldest evicted first —
// with an optional time bound on top for operators who want one
// (DedupTTL: zero means capacity-only, the reference's behaviour).
// Each entry keeps the transaction that carried the first copy so log
// chains can point at it.
type seenTable struct {
	ttl     time.Duration
	max     int
	entries map[[meshcore.MaxHashSize]byte]seenEntry
	// order is the insertion ring: eviction is oldest-first without
	// scanning the map.
	order []([meshcore.MaxHashSize]byte)
	next  int
}

type seenEntry struct {
	txn txn.ID
	at  time.Time
	// slot is this hash's position in the ring, so eviction and
	// re-insertion stay in step.
	slot int
}

func newSeenTable(ttl time.Duration, maxEntries int) *seenTable {
	if maxEntries <= 0 {
		maxEntries = referenceCapacity
	}
	return &seenTable{
		ttl:     ttl,
		max:     maxEntries,
		entries: make(map[[meshcore.MaxHashSize]byte]seenEntry, maxEntries),
		order:   make([]([meshcore.MaxHashSize]byte), maxEntries),
	}
}

// witness records the hash if it is new and returns the transaction of
// the first copy otherwise. A hit does not refresh the entry: the
// reference's ring ages by insertions, so refreshing would let a
// steady echo keep itself alive forever.
func (t *seenTable) witness(h [meshcore.MaxHashSize]byte, id txn.ID, now time.Time) (txn.ID, bool) {
	if e, ok := t.entries[h]; ok {
		if t.ttl <= 0 || now.Sub(e.at) < t.ttl {
			return e.txn, true
		}
		// Expired by the operator's TTL: drop it and record afresh.
		delete(t.entries, h)
	}
	// Claim the next ring slot, evicting whoever holds it.
	slot := t.next
	if old := t.order[slot]; old != ([meshcore.MaxHashSize]byte{}) {
		if e, ok := t.entries[old]; ok && e.slot == slot {
			delete(t.entries, old)
		}
	}
	t.order[slot] = h
	t.next = (slot + 1) % t.max
	t.entries[h] = seenEntry{txn: id, at: now, slot: slot}
	return txn.ID{}, false
}
