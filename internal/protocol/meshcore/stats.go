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

	recvFlood, recvDirect uint32
	sentFlood, sentDirect uint32
	floodDups, directDups uint32
	recvErrors            uint32
	rxAirtime, txAirtime  time.Duration
	lastRSSI, lastSNR     float64
}

// StatsSnapshot is a copy readable from any goroutine.
type StatsSnapshot struct {
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
	flood := pkt.IsRouteFlood()
	switch {
	case dup && flood:
		s.floodDups++
	case dup:
		s.directDups++
	case flood:
		s.recvFlood++
	default:
		s.recvDirect++
	}
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
		RecvFlood: s.recvFlood, RecvDirect: s.recvDirect,
		SentFlood: s.sentFlood, SentDirect: s.sentDirect,
		FloodDups: s.floodDups, DirectDups: s.directDups,
		RecvErrors: s.recvErrors,
		RxAirtime:  s.rxAirtime, TxAirtime: s.txAirtime,
		LastRSSI: s.lastRSSI, LastSNR: s.lastSNR,
	}
}
