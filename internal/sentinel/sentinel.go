// Package sentinel is the observation and archival instantiation: it
// consumes the event bus and journals what the daemon hears and
// decides. It is optional by design — a deployment may run none, and
// nothing elsewhere depends on one existing. The bus makes that free.
package sentinel

import (
	"context"
	"errors"
	"time"

	"go.uber.org/zap"

	"meshrunner.dev/lotor/internal/bus"
)

const pruneEvery = time.Hour

// Sentinel journals bus traffic into its store.
type Sentinel struct {
	store       *store
	bus         *bus.Bus
	sub         *bus.Subscription
	log         *zap.Logger
	retention   time.Duration
	maxFrames   int
	journalPath string
	reported    uint64
	// txWindows tracks each relay's sliding-hour airtime, feeding the
	// tx_airtime series. Consumer-goroutine state, like the store.
	txWindows map[string][]txStamp
}

// txStamp is one emission the airtime window remembers.
type txStamp struct {
	at  time.Time
	air time.Duration
}

// txWindow records one emission and returns the relay's spent airtime
// over the sliding hour ending at that instant.
func (s *Sentinel) txWindow(relay string, at time.Time, air time.Duration) time.Duration {
	if s.txWindows == nil {
		s.txWindows = map[string][]txStamp{}
	}
	s.txWindows[relay] = append(s.txWindows[relay], txStamp{at: at, air: air})
	w := s.txWindows[relay]
	cut := at.Add(-time.Hour)
	i := 0
	for i < len(w) && w[i].at.Before(cut) {
		i++
	}
	w = w[i:]
	s.txWindows[relay] = w
	var sum time.Duration
	for _, t := range w {
		sum += t.air
	}
	return sum
}

// Open prepares the journal. The path may be MemoryJournal for hosts
// whose storage dislikes continuous writes.
func Open(ctx context.Context, journalPath string, retention time.Duration,
	maxFrames int, b *bus.Bus, log *zap.Logger,
) (*Sentinel, error) {
	st, err := openStore(ctx, journalPath)
	if err != nil {
		return nil, err
	}
	// Subscribing here — not in Run — means the journal misses nothing
	// published between construction and the consumer goroutine's first
	// breath, the daemon's opening relay states included.
	sen := &Sentinel{
		store: st, bus: b, sub: b.Subscribe(256),
		log: log, retention: retention, maxFrames: maxFrames,
		journalPath: journalPath,
	}
	sen.seedTxWindows(ctx)
	return sen, nil
}

// seedTxWindows resumes each relay's sliding-hour airtime from the
// journal. The window is RAM state, but the hour it describes already
// happened and the ledger remembers it: starting empty would carve a
// false trough into the archived series at every restart.
func (s *Sentinel) seedTxWindows(ctx context.Context) {
	rows, err := s.store.TxSince(ctx, time.Now().Add(-time.Hour))
	if err != nil {
		s.log.Warn("could not resume the transmit airtime window", zap.Error(err))
		return
	}
	s.txWindows = map[string][]txStamp{}
	for _, r := range rows {
		s.txWindows[r.Relay] = append(s.txWindows[r.Relay], txStamp{at: r.At, air: r.Airtime})
	}
	if len(rows) > 0 {
		s.log.Info("transmit airtime window resumed", zap.Int("emissions", len(rows)))
	}
}

// RecentFrames exposes the journal to future consumers (CLI, web),
// filtered in SQL.
func (s *Sentinel) RecentFrames(ctx context.Context, fq FrameQuery) ([]Frame, error) {
	return s.store.RecentFrames(ctx, fq)
}

// CountFrames says how many rows match, the cap notwithstanding.
func (s *Sentinel) CountFrames(ctx context.Context, fq FrameQuery) (int, error) {
	return s.store.CountFrames(ctx, fq)
}

// Nodes lists the directory the mesh writes about itself.
func (s *Sentinel) Nodes(ctx context.Context) ([]Node, error) { return s.store.Nodes(ctx) }

// Chain returns a transaction and everything duplicate-linked to it.
func (s *Sentinel) Chain(ctx context.Context, txnPrefix string) ([]Frame, error) {
	return s.store.Chain(ctx, txnPrefix)
}

// VerdictCounts sums a relay's judgements by verdict.
func (s *Sentinel) VerdictCounts(ctx context.Context, relay string) (map[string]int, error) {
	return s.store.VerdictCounts(ctx, relay)
}

// FrameVocabulary is what the journal holds in its filterable
// columns, for a reader choosing what to filter on.
func (s *Sentinel) FrameVocabulary(ctx context.Context) (types, verdicts []string, err error) {
	return s.store.FrameVocabulary(ctx)
}

// FrameCount is the journal's current size.
func (s *Sentinel) FrameCount(ctx context.Context) (int, error) {
	return s.store.FrameCount(ctx)
}

// Noise reports each relay's corrupt receptions — traffic the radio
// heard but could not decode. A jammed site shows here first.
func (s *Sentinel) Noise(ctx context.Context) ([]Noise, error) {
	return s.store.Noise(ctx)
}

// KnownRelays lists every relay the journal has records for —
// including relays no longer configured, whose archive outlives them.
func (s *Sentinel) KnownRelays(ctx context.Context) ([]string, error) {
	return s.store.Relays(ctx)
}

// NoiseHistory returns a relay's consolidated noise-floor history
// since the given instant, oldest first.
func (s *Sentinel) NoiseHistory(ctx context.Context, relay string, since time.Time) ([]MetricBucket, error) {
	return s.store.MetricHistory(ctx, "noise_floor", relay, since)
}

// NoiseSpreadHistory returns the companion impulsiveness series: how
// far the 90th percentile sat above the median, bucket by bucket.
func (s *Sentinel) NoiseSpreadHistory(ctx context.Context, relay string, since time.Time) ([]MetricBucket, error) {
	return s.store.MetricHistory(ctx, "noise_spread", relay, since)
}

// NoiseStarvedHistory returns the abandoned-batch counts: when the
// channel was too busy for the floor to be measured at all.
func (s *Sentinel) NoiseStarvedHistory(ctx context.Context, relay string, since time.Time) ([]MetricBucket, error) {
	return s.store.MetricHistory(ctx, "noise_starved", relay, since)
}

// SpentAirtime lists one relay's emissions of the sliding hour —
// what a restarting duty ledger must still count.
func (s *Sentinel) SpentAirtime(ctx context.Context, relay string) ([]Sent, error) {
	rows, err := s.store.TxSince(ctx, time.Now().Add(-time.Hour))
	if err != nil {
		return nil, err
	}
	out := rows[:0]
	for _, r := range rows {
		if r.Relay == relay {
			out = append(out, r)
		}
	}
	return out, nil
}

// TxAirtimeHistory returns a relay's consolidated transmit airtime:
// how many seconds of the sliding hour each bucket was spending.
func (s *Sentinel) TxAirtimeHistory(ctx context.Context, relay string, since time.Time) ([]MetricBucket, error) {
	return s.store.MetricHistory(ctx, "tx_airtime", relay, since)
}

// SentFor lists the emissions answering one transaction, oldest first.
func (s *Sentinel) SentFor(ctx context.Context, txn string) ([]Sent, error) {
	return s.store.SentFor(ctx, txn)
}

// TxDrops lists the emission-refusal tallies, most recent first.
func (s *Sentinel) TxDrops(ctx context.Context) ([]TxDrop, error) {
	return s.store.TxDrops(ctx)
}

// Journal reports where the archive lives and how long it reaches.
func (s *Sentinel) Journal() (path string, retention time.Duration) {
	return s.journalPath, s.retention
}

// Run consumes the bus until the context ends, then drains what the
// subscription still buffers before closing the store — the last
// frames of a session belong in the journal. Journal errors are
// logged, never fatal: an ailing sentinel must not take relays down.
func (s *Sentinel) Run(ctx context.Context) {
	defer s.sub.Close()
	defer func() { _ = s.store.Close() }()

	s.pruneNow(ctx)
	ticker := time.NewTicker(pruneEvery)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// The daemon's context is done; the drain writes must not
			// be — hence the detached context.
			s.drain(context.WithoutCancel(ctx))
			return
		case ev, ok := <-s.sub.C:
			if !ok {
				return
			}
			s.Process(ctx, ev)
			s.reportDrops()
		case <-ticker.C:
			s.pruneNow(ctx)
			s.reportDrops()
		}
	}
}

// drain journals everything still buffered. The caller sequences the
// shutdown so publishers are already stopped.
func (s *Sentinel) drain(ctx context.Context) {
	for {
		select {
		case ev := <-s.sub.C:
			s.Process(ctx, ev)
		default:
			s.reportDrops()
			return
		}
	}
}

// reportDrops warns as soon as the subscription lost events, not an
// hour later: silent degradation is a bug by definition.
func (s *Sentinel) reportDrops() {
	if d := s.sub.Dropped(); d > s.reported {
		s.log.Warn("journal fell behind the bus",
			zap.Uint64("events_dropped", d-s.reported))
		s.reported = d
	}
}

// Process journals one event synchronously. Run feeds it from the
// bus; tests and tools may feed it directly.
func (s *Sentinel) Process(ctx context.Context, ev bus.Event) {
	var err error
	switch e := ev.(type) {
	case bus.FrameHeard:
		err = s.store.insertHeard(ctx, Frame{
			Txn: e.Txn.String(), Relay: e.Relay, At: e.At,
			Bytes: e.Bytes, RSSI: e.RSSI, SNR: e.SNR,
			SignalRSSI: e.SignalRSSI, FreqErrHz: e.FreqErrHz,
			Airtime: e.Airtime,
		})
	case bus.FrameJudged:
		err = s.store.applyJudgement(ctx, e.Txn.String(), e.Relay, Frame{
			Type: e.Type, Route: e.Route, Scope: e.Scope, PathLen: e.PathLen,
			Verdict: e.Verdict, DuplicateOf: e.DuplicateOf,
			Node: e.Node, PubKey: e.PubKey, Detail: e.Detail,
		})
	case bus.FrameCorrupt:
		err = s.store.recordCorrupt(ctx, e.At, e.Relay, e.Err)
	case bus.NoiseFloor:
		// Three writes on purpose: the last value for O(1) status
		// reads, and one raw point per series — the floor and the
		// site's impulsiveness age through the tiers independently.
		if err = s.store.upsertNoiseFloor(ctx, e.At, e.Relay, e.DBm, e.SpreadDB); err == nil {
			if err = s.store.insertMetric(ctx, "noise_floor", e.Relay, e.At, e.DBm); err == nil {
				err = s.store.insertMetric(ctx, "noise_spread", e.Relay, e.At, e.SpreadDB)
			}
		}
	case bus.NoiseStarved:
		err = s.store.insertMetric(ctx, "noise_starved", e.Relay, e.At, float64(e.Aborted))
	case bus.FrameSent:
		// The ledger row, then the derived series: the sliding hour's
		// spent airtime in seconds, one point per emission.
		if err = s.store.insertSent(ctx, e.At, e.Relay, e.Txn.String(), e.Kind,
			e.Airtime, e.PowerDBm, e.Shadow); err == nil {
			err = s.store.insertMetric(ctx, "tx_airtime", e.Relay, e.At,
				s.txWindow(e.Relay, e.At, e.Airtime).Seconds())
		}
	case bus.TxDropped:
		err = s.store.recordTxDrop(ctx, e.At, e.Relay, e.Reason)
	case bus.RelayState:
		err = s.store.insertRelayState(ctx, e.At, e.Relay, e.State, e.Err)
	default:
		return
	}
	switch {
	case errors.Is(err, errJudgementOrphan):
		s.log.Warn("journal recovered an orphan judgement — its heard event was dropped")
	case err != nil:
		s.log.Warn("journal write failed", zap.Error(err))
	}
}

func (s *Sentinel) pruneNow(ctx context.Context) {
	if err := s.store.prune(ctx, time.Now(), s.retention, s.maxFrames); err != nil {
		s.log.Warn("journal prune failed", zap.Error(err))
	}
}
