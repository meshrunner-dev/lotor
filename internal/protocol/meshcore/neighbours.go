package meshcore

import (
	"sort"
	"sync"
	"time"

	"meshrunner.dev/pkg/meshcore"
)

// maxNeighbours bounds the table; the least-recently-heard entry is
// evicted when a new node arrives full, the reference's shape.
const maxNeighbours = 16

// neighbourTable remembers the direct neighbourhood: who we hear
// without a relay in between, for the GET_NEIGHBOURS answer and the
// operator's own site view. The engine's goroutine writes it and CLI
// sessions read it, so it is held under a mutex.
type neighbourTable struct {
	mu sync.Mutex
	by map[[meshcore.PubKeySize]byte]Neighbour
}

func newNeighbourTable() *neighbourTable {
	return &neighbourTable{by: map[[meshcore.PubKeySize]byte]Neighbour{}}
}

// put records or refreshes a neighbour. When the table is full and
// the node is new, the least-recently-heard entry makes room.
//
// An empty name leaves the one already held. Only an advert carries a
// name; a discovery answer carries none, so a scan of a neighbourhood
// we already know would otherwise strip every node back to its key.
func (nt *neighbourTable) put(
	pubKey [meshcore.PubKeySize]byte, name string, snr float64, at time.Time,
) {
	nt.mu.Lock()
	defer nt.mu.Unlock()
	known, seen := nt.by[pubKey]
	if !seen {
		evictOldest(nt.by, maxNeighbours, func(n Neighbour) time.Time { return n.Heard })
	}
	if name == "" {
		name = known.Name
	}
	nt.by[pubKey] = Neighbour{PubKey: pubKey, Name: name, SNR: snr, Heard: at}
}

// Neighbour is one node heard with no relay in between: the SNR we
// last heard it at, and when.
type Neighbour struct {
	PubKey [meshcore.PubKeySize]byte
	// Name is what the node calls itself, when it has told us — which
	// an advert does and a discovery answer does not.
	Name  string
	SNR   float64
	Heard time.Time
}

// snapshot returns the neighbourhood newest-heard first — any
// goroutine.
func (nt *neighbourTable) snapshot() []Neighbour {
	nt.mu.Lock()
	defer nt.mu.Unlock()
	out := make([]Neighbour, 0, len(nt.by))
	for _, n := range nt.by {
		out = append(out, n)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Heard.After(out[j].Heard) })
	return out
}

// evictOldest makes room in a bounded table by dropping its least
// recently touched entry, dated by at. Both the neighbourhood and the
// session table fill the same way and used to say so twice.
func evictOldest[K comparable, V any](m map[K]V, maximum int, at func(V) time.Time) {
	if len(m) < maximum {
		return
	}
	var oldest K
	var when time.Time
	first := true
	for k, v := range m {
		if t := at(v); first || t.Before(when) {
			oldest, when, first = k, t, false
		}
	}
	delete(m, oldest)
}

// get returns one neighbour as the table now holds it — any goroutine.
func (nt *neighbourTable) get(pubKey [meshcore.PubKeySize]byte) Neighbour {
	nt.mu.Lock()
	defer nt.mu.Unlock()
	return nt.by[pubKey]
}
