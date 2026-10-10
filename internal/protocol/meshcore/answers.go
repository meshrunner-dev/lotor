package meshcore

// The bodies of the answers an authenticated client asks for. Each is
// a wire format the reference defines and a companion parses, so they
// live together, apart from the session machinery that decides who is
// allowed to ask.

import (
	"errors"
	"go.uber.org/zap"

	"math"
	"sort"
	"time"

	"meshrunner.dev/pkg/meshcore"
)

// statusBody packs the tally a companion's status page reads. Its
// battery is the supply the telemetry leads with, zero when nothing
// measures one. The error bitfield is sent as zero: this node keeps
// none.
func (e *engine) statusBody() []byte {
	s := e.stats.snapshot()
	nf := int16(0)
	if e.floor != nil {
		if f, ok := e.floor(); ok {
			nf = int16(f.DBm)
		}
	}
	return meshcore.RepeaterStats{
		BattMilliVolts: milliVolts(e.supplyVolts()),
		TxQueueLen:     uint16(e.queueLen()),
		NoiseFloor:     nf,
		LastRSSI:       int16(s.LastRSSI),
		PacketsRecv:    s.RecvTotal,
		PacketsSent:    s.SentFlood + s.SentDirect,
		TxAirtimeSecs:  uint32(s.TxAirtime / time.Second),
		UptimeSecs:     uint32(time.Since(e.started) / time.Second),
		SentFlood:      s.SentFlood,
		SentDirect:     s.SentDirect,
		RecvFlood:      s.RecvFlood,
		RecvDirect:     s.RecvDirect,
		LastSNR:        s.LastSNR,
		DirectDups:     uint16(s.DirectDups),
		FloodDups:      uint16(s.FloodDups),
		RxAirtimeSecs:  uint32(s.RxAirtime / time.Second),
		RecvErrors:     s.RecvErrors,
	}.AppendTo(nil)
}

// supplyVolts is what this node runs on, the one reading the status
// page and the telemetry both carry as its battery. Zero when nothing
// measures the supply: parsable, and honestly empty.
func (e *engine) supplyVolts() float64 {
	if e.supply != nil {
		if v, ok := e.supply(); ok {
			return v
		}
	}
	return 0
}

// milliVolts is a supply reading in the status field's unit. The field
// is sixteen bits wide, and Go leaves a float converted beyond that
// range undefined, so a reading outside it pins to the edge instead of
// coming out as some plausible voltage.
func milliVolts(v float64) uint16 {
	return uint16(math.Round(max(0, min(v*1000, math.MaxUint16))))
}

// telemetryBody reports what this node measures about itself, in the
// Cayenne encoding companions expect.
//
// The voltage leads and is always present: companion apps show no
// telemetry at all without it, and every emitter in the reference
// sends it first and unconditionally, so no companion has met a
// channel-1 payload lacking one. It is the measured supply, the same
// one statusBody reports, and zero stands in where nothing measures
// it — transparently not measured.
func (e *engine) telemetryBody(permMask byte, budget int) []byte {
	return e.telemetryBodyLogged(e.log, permMask, budget)
}

// telemetryBodyLogged is the production path's form: the caller hands
// down the correlation-enriched logger of the request being answered.
// The plain wrapper above keeps the codec independently testable.
func (e *engine) telemetryBodyLogged(log *zap.Logger, permMask byte, budget int) []byte {
	// Bounded to the route this answer will travel, before anything
	// is encoded: the sensors hook was the one variable producer left
	// composing blind, and a reading list that outgrew the packet was
	// refused after the asker's replay guard was already spent. The
	// encoder refuses whole records, so running out of room reads as
	// the end of the list — never as a buffer cut mid-record, which
	// LPP cannot survive.
	enc := meshcore.NewLPPEncoderWithin(budget)
	// The voltage leads, as the reference's does: a companion reads
	// this field as the node's battery, and a payload without it is
	// one such a client will not show at all.
	if err := enc.Add(meshcore.LPPReading{
		Channel: TelemChannelSelf, Type: meshcore.LPPVoltage, Value: e.supplyVolts(),
	}); err != nil {
		// A rejected reading leaves its header in the buffer, so the
		// payload would decode as truncated rather than short.
		log.Warn("telemetry voltage refused by the encoder", zap.Error(err))
		return nil
	}
	// Then the sensors', under the permission mask — the reference
	// queries its SensorManager here. The hook always runs and the
	// mask always travels: which sensor a mask admits is the sensors'
	// own judgement, not this file's.
	if e.telemetry != nil {
		if err := e.telemetry(permMask, enc); err != nil && !errors.Is(err, meshcore.ErrLPPFull) {
			log.Warn("sensor telemetry refused", zap.Error(err))
			return nil
		}
	}
	// The host's own thermometer comes last, deliberately: the
	// reference calls it the default "overridden by external sensors
	// (if any)", and a client honouring that precedence needs the
	// real one to arrive first. Coming last also makes it the first
	// thing a tight budget drops, which is the right one to lose.
	if c, ok := hostTemperature(); ok {
		if err := enc.Add(meshcore.LPPReading{
			Channel: TelemChannelSelf, Type: meshcore.LPPTemperature, Value: c,
		}); err != nil && !errors.Is(err, meshcore.ErrLPPFull) {
			log.Warn("telemetry temperature refused by the encoder", zap.Error(err))
			return nil
		}
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
func (e *engine) neighboursBody(args []byte, budget int) []byte {
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
	// Dropped one whole neighbour at a time until the frame fits the
	// route it will travel. The codec decides what a row costs; this
	// only decides how many there is room for, which is the honest
	// division of labour when a layout is not this file's to know.
	for {
		body := meshcore.FrameNeighbours(len(all), rows)
		if len(body) <= budget || len(rows) == 0 {
			return body
		}
		rows = rows[:len(rows)-1]
	}
}
