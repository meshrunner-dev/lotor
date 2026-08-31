package meshcore

import (
	"testing"
	"time"

	"meshrunner.dev/pkg/meshcore"

	"meshrunner.dev/lotor/internal/radio"
)

// The status a companion reads must answer with a measurement, not
// with the zero of a frame a peer beside us handed over.
func TestHandedOverFrameLeavesTheLastHeardAlone(t *testing.T) {
	pkt, err := meshcore.BuildAck([]byte{1, 2, 3, 4})
	if err != nil {
		t.Fatal(err)
	}
	var s Stats

	s.countHeard(pkt, radio.Frame{RSSI: -87, SNR: 5.5, Airtime: time.Second}, false)
	if s.LastRSSI != -87 || s.LastSNR != 5.5 {
		t.Fatalf("a demodulated frame left %v/%v, want -87/5.5", s.LastRSSI, s.LastSNR)
	}

	s.countHeard(pkt, radio.Frame{Binding: "station:alice"}, false)
	if s.LastRSSI != -87 || s.LastSNR != 5.5 {
		t.Fatalf("a handed-over frame moved the last heard to %v/%v", s.LastRSSI, s.LastSNR)
	}
	// Counted as receptions all the same: an ACK travels flooded.
	if s.RecvFlood != 2 {
		t.Fatalf("RecvFlood = %d, want both receptions counted", s.RecvFlood)
	}
}
