package meshcore

import mesh "meshrunner.dev/pkg/meshcore"

const stationSeenEntries = 160

// packetRing is the reference companion's count-only SimpleMeshTables ring.
// It deliberately has no time expiry: the oldest of 160 packet hashes is the
// one forgotten next.
type packetRing struct {
	hashes [stationSeenEntries][mesh.MaxHashSize]byte
	next   int
}

func (r *packetRing) witness(hash [mesh.MaxHashSize]byte) bool {
	for i := range r.hashes {
		if r.hashes[i] == hash {
			return true
		}
	}
	r.mark(hash)
	return false
}

func (r *packetRing) mark(hash [mesh.MaxHashSize]byte) {
	r.hashes[r.next] = hash
	r.next = (r.next + 1) % len(r.hashes)
}
