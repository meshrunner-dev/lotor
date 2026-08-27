package meshcore

import (
	"testing"
	"time"

	"meshrunner.dev/pkg/meshcore"
)

func TestNeighbourOrderings(t *testing.T) {
	// The four the query may ask for, and the one it may not.
	now := time.Now()
	// As the table hands them over: newest heard first.
	base := []Neighbour{
		{SNR: -2, Heard: now},                  // newest, weakest
		{SNR: 3, Heard: now.Add(-time.Minute)}, // middle both ways
		{SNR: 9, Heard: now.Add(-time.Hour)},   // oldest, strongest
	}
	for _, c := range []struct {
		order byte
		first float64 // the SNR the first row should carry
		what  string
	}{
		{meshcore.NeighboursNewestFirst, -2, "newest heard"},
		{meshcore.NeighboursOldestFirst, 9, "oldest heard"},
		{meshcore.NeighboursStrongestFirst, 9, "strongest"},
		{meshcore.NeighboursWeakestFirst, -2, "weakest"},
		{200, -2, "an order nobody defined leaves what the table gave"},
	} {
		all := append([]Neighbour(nil), base...)
		orderNeighbours(all, c.order)
		if all[0].SNR != c.first {
			t.Errorf("order %d (%s): first is %v dB, want %v", c.order, c.what, all[0].SNR, c.first)
		}
	}
}
