package meshcorehost

import "meshrunner.dev/pkg/meshcore"

// SeenCapacity is the reference's packet-hash ring, SimpleMeshTables'
// MAX_PACKET_HASHES: duplicate suppression is bounded by count, not by
// time — a hash lives until this many newer ones have pushed it out.
const SeenCapacity = 128 + 32

// Seen is the count-bounded ring every reference node keeps in front of
// its handlers: a packet whose hash is in it has already acted — a
// flood's other copies, a recording replayed — and acts no more. The
// hash covers type and payload but not the path, so the copies a flood
// sends back through several repeaters all read as one. Owned by its
// owner's goroutine or its owner's lock, like the Table.
type Seen struct {
	hashes [SeenCapacity][meshcore.MaxHashSize]byte
	next   int
}

// Witness records the hash if it is new and reports whether it was
// already there. A hit does not refresh the entry: the ring ages by
// insertions, so refreshing would let a steady echo keep itself alive.
func (s *Seen) Witness(hash [meshcore.MaxHashSize]byte) bool {
	for i := range s.hashes {
		if s.hashes[i] == hash {
			return true
		}
	}
	s.hashes[s.next] = hash
	s.next = (s.next + 1) % len(s.hashes)
	return false
}
