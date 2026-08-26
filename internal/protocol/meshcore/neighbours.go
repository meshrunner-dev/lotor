package meshcore

import (
	"sort"
	"sync"
	"time"
)

// maxNeighbours bounds the table; the least-recently-heard entry is
// evicted when a new node arrives full, the reference's shape.
const maxNeighbours = 16

// neighbour is one node heard directly — zero hop — with the SNR we
// last heard it at and when.
type neighbour struct {
	pubKey [32]byte
	snr    float64
	heard  time.Time
}

// neighbourTable remembers the direct neighbourhood: who we hear
// without a relay in between, for the GET_NEIGHBOURS answer and the
// operator's own site view. The engine's goroutine writes it and CLI
// sessions read it, so it is held under a mutex.
type neighbourTable struct {
	mu sync.Mutex
	by map[[32]byte]*neighbour
}

func newNeighbourTable() *neighbourTable {
	return &neighbourTable{by: map[[32]byte]*neighbour{}}
}

// put records or refreshes a neighbour. When the table is full and the
// node is new, the least-recently-heard entry makes room.
func (nt *neighbourTable) put(pubKey [32]byte, snr float64, at time.Time) {
	nt.mu.Lock()
	defer nt.mu.Unlock()
	if n, ok := nt.by[pubKey]; ok {
		n.snr, n.heard = snr, at
		return
	}
	if len(nt.by) >= maxNeighbours {
		var oldest [32]byte
		first := true
		for k, n := range nt.by {
			if first || n.heard.Before(nt.by[oldest].heard) {
				oldest, first = k, false
			}
		}
		delete(nt.by, oldest)
	}
	nt.by[pubKey] = &neighbour{pubKey: pubKey, snr: snr, heard: at}
}

// Neighbour is one row of the neighbourhood, as a reader sees it.
type Neighbour struct {
	PubKey [32]byte
	SNR    float64
	Heard  time.Time
}

// snapshot returns the neighbourhood newest-heard first — any
// goroutine.
func (nt *neighbourTable) snapshot() []Neighbour {
	nt.mu.Lock()
	defer nt.mu.Unlock()
	out := make([]Neighbour, 0, len(nt.by))
	for _, n := range nt.by {
		out = append(out, Neighbour{PubKey: n.pubKey, SNR: n.snr, Heard: n.heard})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Heard.After(out[j].Heard) })
	return out
}
