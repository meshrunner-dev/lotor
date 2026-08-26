package meshcore

import (
	"sync"
	"time"

	"meshrunner.dev/pkg/meshcore"
)

// Stats is a repeater's own reception and transmission tally — the
// figures the GET_STATUS request returns and the console shows. The
// engine's goroutine writes it; readers take a copy under the lock.
type Stats struct {
	mu sync.Mutex

	recvTotal             uint32
	recvFlood, recvDirect uint32
	sentFlood, sentDirect uint32
	floodDups, directDups uint32
	recvErrors            uint32
	rxAirtime, txAirtime  time.Duration
	lastRSSI, lastSNR     float64
}

// StatsSnapshot is a copy readable from any goroutine.
type StatsSnapshot struct {
	RecvTotal             uint32
	RecvFlood, RecvDirect uint32
	SentFlood, SentDirect uint32
	FloodDups, DirectDups uint32
	RecvErrors            uint32
	RxAirtime, TxAirtime  time.Duration
	LastRSSI, LastSNR     float64
}

// countHeard records one reception: its route, airtime, and the RSSI
// and SNR the radio measured — the "last heard" the status reports.
func (s *Stats) countHeard(pkt *meshcore.Packet, rssi, snr float64, airtime time.Duration, dup bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastRSSI, s.lastSNR = rssi, snr
	s.rxAirtime += airtime
	// The reference counts a reception by its route whether or not a
	// copy came before it, and keeps the duplicate tallies alongside
	// rather than instead: a companion reads the dup ratio from both.
	flood := pkt.IsRouteFlood()
	if flood {
		s.recvFlood++
	} else {
		s.recvDirect++
	}
	if dup && flood {
		s.floodDups++
	} else if dup {
		s.directDups++
	}
}

// countFrame tallies a frame the radio handed us, before anything is
// made of it — the raw reception count the reference reads off its
// driver, malformed frames included.
func (s *Stats) countFrame() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.recvTotal++
}

// countCorrupt records a reception the radio could not decode.
func (s *Stats) countCorrupt() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.recvErrors++
}

// countSent records one emission by its route.
func (s *Stats) countSent(flood bool, airtime time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.txAirtime += airtime
	if flood {
		s.sentFlood++
	} else {
		s.sentDirect++
	}
}

// snapshot copies the counters out.
func (s *Stats) snapshot() StatsSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return StatsSnapshot{
		RecvTotal: s.recvTotal,
		RecvFlood: s.recvFlood, RecvDirect: s.recvDirect,
		SentFlood: s.sentFlood, SentDirect: s.sentDirect,
		FloodDups: s.floodDups, DirectDups: s.directDups,
		RecvErrors: s.recvErrors,
		RxAirtime:  s.rxAirtime, TxAirtime: s.txAirtime,
		LastRSSI: s.lastRSSI, LastSNR: s.lastSNR,
	}
}
