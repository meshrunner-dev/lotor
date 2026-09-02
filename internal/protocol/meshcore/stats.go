package meshcore

import (
	"sync"
	"time"

	"meshrunner.dev/pkg/meshcore"

	"meshrunner.dev/lotor/internal/radio"
)

// Stats is a repeater's own reception and transmission tally — the
// figures the GET_STATUS request returns and the console shows. The
// engine's goroutine writes it; readers take a copy under the lock.
type Stats struct {
	StatsSnapshot

	mu sync.Mutex
}

// StatsSnapshot is the tally itself, copied out for any goroutine to
// read. Stats embeds it rather than shadowing every field: eleven
// counters written down three times is three chances to forget one.
type StatsSnapshot struct {
	// RecvTotal counts frames the radio handed us, whatever became of
	// them; the route counters split what parsed, and the duplicate
	// counters run alongside rather than instead.
	RecvTotal             uint32
	RecvFlood, RecvDirect uint32
	SentFlood, SentDirect uint32
	FloodDups, DirectDups uint32
	RecvErrors            uint32
	RxAirtime, TxAirtime  time.Duration
	LastRSSI, LastSNR     float64
}

// countHeard records one reception: its route, its airtime, and the
// RSSI and SNR the radio measured — the "last heard" the status
// reports.
//
// A frame handed over by a binding beside us on the controller was
// never demodulated. It is a reception and counts as one, but its
// measurements are the zero value: taking them would answer a
// companion's status query with a reading nobody took, and every
// advert our own station composes would blank the last one.
func (s *Stats) countHeard(pkt *meshcore.Packet, frame radio.Frame, dup bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if frame.HasRFMeasurements() {
		s.LastRSSI, s.LastSNR = frame.RSSI, frame.SNR
		s.RxAirtime += frame.Airtime
	}
	// The reference counts a reception by its route whether or not a
	// copy came before it, and keeps the duplicate tallies alongside
	// rather than instead: a companion reads the dup ratio from both.
	flood := pkt.IsRouteFlood()
	if flood {
		s.RecvFlood++
	} else {
		s.RecvDirect++
	}
	if dup && flood {
		s.FloodDups++
	} else if dup {
		s.DirectDups++
	}
}

// countFrame tallies a frame the radio handed us, before anything is
// made of it — the raw reception count the reference reads off its
// driver, malformed frames included.
func (s *Stats) countFrame() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.RecvTotal++
}

// countCorrupt records a reception the radio could not decode.
func (s *Stats) countCorrupt() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.RecvErrors++
}

// countSent records one emission by its route.
func (s *Stats) countSent(flood bool, airtime time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.TxAirtime += airtime
	if flood {
		s.SentFlood++
	} else {
		s.SentDirect++
	}
}

// Snapshot copies the tally out for any goroutine — the observers'
// periodic heartbeat, notably.
func (s *Stats) Snapshot() StatsSnapshot { return s.snapshot() }

// snapshot copies the counters out.
func (s *Stats) snapshot() StatsSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.StatsSnapshot
}
