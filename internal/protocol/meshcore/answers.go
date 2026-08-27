package meshcore

// The bodies of the answers an authenticated client asks for. Each is
// a wire format the reference defines and a companion parses, so they
// live together, apart from the session machinery that decides who is
// allowed to ask.

import (
	"sort"
	"time"

	"meshrunner.dev/pkg/meshcore"
)

// statusBody packs the tally a companion's status page reads. Two
// fields this node cannot answer honestly are sent as zero: it has no
// battery to measure, and it keeps no error bitfield.
func (e *engine) statusBody() []byte {
	s := e.stats.snapshot()
	nf := int16(0)
	if e.floor != nil {
		if f, ok := e.floor(); ok {
			nf = int16(f.DBm)
		}
	}
	return meshcore.RepeaterStats{
		TxQueueLen:    uint16(e.queueLen()),
		NoiseFloor:    nf,
		LastRSSI:      int16(s.LastRSSI),
		PacketsRecv:   s.RecvTotal,
		PacketsSent:   s.SentFlood + s.SentDirect,
		TxAirtimeSecs: uint32(s.TxAirtime / time.Second),
		UptimeSecs:    uint32(time.Since(e.started) / time.Second),
		SentFlood:     s.SentFlood,
		SentDirect:    s.SentDirect,
		RecvFlood:     s.RecvFlood,
		RecvDirect:    s.RecvDirect,
		LastSNR:       s.LastSNR,
		DirectDups:    uint16(s.DirectDups),
		FloodDups:     uint16(s.FloodDups),
		RxAirtimeSecs: uint32(s.RxAirtime / time.Second),
		RecvErrors:    s.RecvErrors,
	}.AppendTo(nil)
}

// telemetryBody reports what this node can honestly measure about
// itself, in the Cayenne encoding companions expect.
func (e *engine) telemetryBody() []byte {
	enc := meshcore.NewLPPEncoder()
	if c, ok := hostTemperature(); ok {
		_ = enc.Add(meshcore.LPPReading{
			Channel: telemChannelSelf, Type: meshcore.LPPTemperature, Value: c,
		})
	}
	return enc.Bytes()
}

// orderNeighbours sorts a neighbourhood the way the query asked. An
// order nobody defined leaves the newest-heard first, which is what
// the table already gives — stated here rather than left to the
// reader to discover in another file.
func orderNeighbours(all []Neighbour, orderBy byte) {
	switch orderBy {
	case meshcore.NeighboursOldestFirst:
		sort.SliceStable(all, func(i, j int) bool { return all[i].Heard.Before(all[j].Heard) })
	case meshcore.NeighboursStrongestFirst:
		sort.SliceStable(all, func(i, j int) bool { return all[i].SNR > all[j].SNR })
	case meshcore.NeighboursWeakestFirst:
		sort.SliceStable(all, func(i, j int) bool { return all[i].SNR < all[j].SNR })
	case meshcore.NeighboursNewestFirst:
	default: // an order we do not know is the order we already have
	}
}

// neighboursBody answers the neighbourhood query: the total known, how
// many are returned, then each one's key prefix, how long ago it was
// heard, and the SNR it was heard at.
func (e *engine) neighboursBody(args []byte) []byte {
	q, err := meshcore.ParseNeighboursQuery(args)
	if err != nil {
		return nil
	}
	all := e.neighbours.snapshot() // newest heard first
	orderNeighbours(all, q.OrderBy)

	now := time.Now()
	rows := make([]meshcore.NeighbourEntry, 0, q.Count)
	for i := int(q.Offset); i < len(all) && len(rows) < int(q.Count); i++ {
		n := all[i]
		rows = append(rows, meshcore.NeighbourEntry{
			PubKeyPrefix: n.PubKey[:q.PrefixLen],
			HeardSecsAgo: uint32(now.Sub(n.Heard) / time.Second),
			SNR:          n.SNR,
		})
	}
	return meshcore.FrameNeighbours(len(all), rows)
}
