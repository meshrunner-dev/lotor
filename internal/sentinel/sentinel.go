// Package sentinel is the observation and archival instantiation: it
// consumes the event bus and journals what the daemon hears and
// decides. It is optional by design — a deployment may run none, and
// nothing elsewhere depends on one existing. The bus makes that free.
package sentinel

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"meshrunner.dev/lotor/internal/bus"
)

const pruneEvery = time.Hour

// Sentinel journals bus traffic into its store.
type Sentinel struct {
	store            *store
	bus              *bus.Bus
	sub              *bus.Subscription
	log              *zap.Logger
	retention        time.Duration
	metricsRetention time.Duration
	maxFrames        int
	journalPath      string
	reported         uint64
	dropWarnAt       time.Time
	dropOpen         bool
	// The journal's health, kept where any goroutine may read it: the
	// consumer writes, status and heartbeats read. warnedAt paces the
	// failure logs on the consumer goroutine alone.
	writes   atomic.Uint64
	failures atomic.Uint64
	degraded atomic.Bool
	lastErr  atomic.Value // string
	lastFail atomic.Int64 // unix ms
	warnedAt time.Time
	// txWindows tracks each relay's sliding-hour airtime, feeding the
	// tx_airtime series. Consumer-goroutine state, like the store.
	txWindows map[string][]txStamp
}

// txStamp is one emission the airtime window remembers.
type txStamp struct {
	at  time.Time
	air time.Duration
}

// nextTxWindow computes the relay's spent airtime over the sliding
// hour ending at this emission without changing RAM. The caller only
// installs the returned window after the ledger and its metric commit;
// a failed SQLite write must not leave memory one emission ahead.
func (s *Sentinel) nextTxWindow(relay string, at time.Time, air time.Duration) (time.Duration, []txStamp) {
	current := s.txWindows[relay]
	w := make([]txStamp, len(current), len(current)+1)
	copy(w, current)
	w = append(w, txStamp{at: at, air: air})
	cut := at.Add(-time.Hour)
	i := 0
	for i < len(w) && w[i].at.Before(cut) {
		i++
	}
	w = w[i:]
	var sum time.Duration
	for _, t := range w {
		sum += t.air
	}
	return sum, w
}

// Open prepares the journal. The path may be MemoryJournal for hosts
// whose storage dislikes continuous writes.
func Open(ctx context.Context, journalPath string, retention, metricsRetention time.Duration,
	maxFrames int, b *bus.Bus, log *zap.Logger,
) (*Sentinel, error) {
	st, err := openStore(ctx, journalPath)
	if err != nil {
		return nil, err
	}
	// Subscribing here — not in Run — means the journal misses nothing
	// published between construction and the consumer goroutine's first
	// breath, the daemon's opening relay states included.
	if metricsRetention <= 0 {
		// The default resolves HERE, so MetricsRetention() answers
		// the depth actually enforced, never a zero that means two
		// years somewhere deeper.
		metricsRetention = metricDailyKeep
	}
	sen := &Sentinel{
		store: st, bus: b,
		log: log, retention: retention, metricsRetention: metricsRetention,
		maxFrames: maxFrames, journalPath: journalPath,
	}
	// Maintenance belongs before subscription: Open itself precedes
	// every relay at assembly, whereas pruning at the head of Run left
	// a subscribed-but-unread buffer for producers to overflow.
	sen.pruneNow(ctx)
	sen.seedTxWindows(ctx)
	sen.sub = b.Subscribe(256)
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

// Chain returns a correlation and everything duplicate-linked to it.
func (s *Sentinel) Chain(ctx context.Context, correlationPrefix string) ([]Frame, error) {
	return s.store.Chain(ctx, correlationPrefix)
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

// SentFor lists the emissions answering one correlation, oldest first.
func (s *Sentinel) SentFor(ctx context.Context, correlationPrefix string) ([]Sent, error) {
	return s.store.SentFor(ctx, correlationPrefix)
}

// TxDrops lists the emission-refusal tallies, most recent first.
func (s *Sentinel) TxDrops(ctx context.Context) ([]TxDrop, error) {
	return s.store.TxDrops(ctx)
}

// DropsFor lists the refusals recorded under one correlation prefix —
// the missing half of the heard → judged → sent chain.
func (s *Sentinel) DropsFor(ctx context.Context, correlationPrefix string) ([]TxDropEvent, error) {
	return s.store.DropsFor(ctx, correlationPrefix)
}

// StateHistory interleaves recent relay and observer lifecycle
// transitions, newest first.
func (s *Sentinel) StateHistory(ctx context.Context, since time.Time, limit int) ([]StateTransition, error) {
	return s.store.StateHistory(ctx, since, limit)
}

// Journal reports where the archive lives and how long it reaches —
// the detailed depth. The consolidated metric tiers reach further, by
// design; MetricsRetention names that depth honestly.
func (s *Sentinel) Journal() (path string, retention time.Duration) {
	return s.journalPath, s.retention
}

// MetricsRetention is how long the consolidated hourly/daily metric
// tiers reach — the long game the detailed retention does not bound.
func (s *Sentinel) MetricsRetention() time.Duration { return s.metricsRetention }

// Run consumes the bus until the context ends, then drains what the
// subscription still buffers before closing the store — the last
// frames of a session belong in the journal. Journal errors are
// logged, never fatal: an ailing sentinel must not take relays down.
func (s *Sentinel) Run(ctx context.Context) {
	defer s.sub.Close()
	defer func() { _ = s.store.Close() }()

	ticker := time.NewTicker(pruneEvery)
	defer ticker.Stop()
	healthTicker := time.NewTicker(healthLogEvery)
	defer healthTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			// The daemon's context is done; the drain writes must not
			// be — detached, but BOUNDED: a filesystem that hangs must
			// not hold the whole shutdown hostage to save at most a
			// buffer's worth of events.
			dctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), drainBudget)
			s.drain(dctx)
			cancel()
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
		case <-healthTicker.C:
			// A completely quiet bus after an overload still closes
			// the loss episode on time; recovery cannot depend on the
			// next frame arriving or wait for the hourly prune.
			s.reportDrops()
		}
	}
}

// drainBudget bounds the shutdown drain.
const drainBudget = 5 * time.Second

// drain journals everything still buffered. The caller sequences the
// shutdown so publishers are already stopped.
func (s *Sentinel) drain(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			s.reportDrops()
			return
		}
		select {
		case ev := <-s.sub.C:
			s.Process(ctx, ev)
		default:
			s.reportDrops()
			return
		}
	}
}

// reportDrops warns when the subscription lost events — the FIRST
// loss at once, the rest folded into paced deltas, the same
// discipline the write failures follow: under sustained saturation
// every consumed event can free a slot two publications refill and
// overflow, and a WARN per cycle is log volume proportional to the
// very traffic that caused it. The cumulative count stays readable
// in Health whatever the pacing.
func (s *Sentinel) reportDrops() {
	d := s.sub.Dropped()
	if d <= s.reported {
		// No new losses. An episode quiet for a whole pacing window
		// is over, and its end is said once — silence after a WARN
		// otherwise reads as "still losing".
		if s.dropOpen && time.Since(s.dropWarnAt) >= healthLogEvery {
			s.dropOpen = false
			s.log.Info("journal caught up with the bus",
				zap.Uint64("events_dropped_total", d))
		}
		return
	}
	first := s.reported == 0
	if !first && time.Since(s.dropWarnAt) < healthLogEvery {
		return
	}
	s.dropWarnAt = time.Now()
	s.dropOpen = true
	s.log.Warn("journal fell behind the bus",
		zap.Uint64("events_dropped", d-s.reported),
		zap.Uint64("events_dropped_total", d),
		zap.Bool("first", first))
	s.reported = d
}

// Process journals one event synchronously. Run feeds it from the
// bus; tests and tools may feed it directly.
func (s *Sentinel) Process(ctx context.Context, ev bus.Event) {
	var err error
	switch e := ev.(type) {
	case bus.FrameJudged:
		// The one archive event: it carries the reception AND the
		// verdict, so a backpressure drop loses a whole frame or
		// nothing — never a row stranded halfway. FrameHeard stays on
		// the bus for the live consumers and is not journalled.
		err = s.store.insertObserved(ctx, Frame{
			Correlation: e.Correlation.String(), Relay: e.Relay, At: e.At,
			Bytes: e.Bytes, RSSI: e.RSSI, SNR: e.SNR,
			SignalRSSI: e.SignalRSSI, FreqErrHz: e.FreqErrHz,
			Airtime: e.Airtime,
			Type:    e.Type, Route: e.Route, Scope: e.Scope, PathLen: e.PathLen,
			Verdict: e.Verdict, DuplicateOf: e.DuplicateOf,
			Node: e.Node, PubKey: e.PubKey, Detail: e.Detail,
		})
	case bus.FrameCorrupt:
		err = s.store.recordCorrupt(ctx, e.At, e.Relay, e.Err)
	case bus.NoiseFloor:
		err = s.store.recordNoiseFloor(ctx, e.At, e.Relay, e.DBm, e.SpreadDB)
	case bus.NoiseStarved:
		err = s.store.insertMetric(ctx, "noise_starved", e.Relay, e.At, float64(e.Aborted))
	case bus.FrameSent:
		window, next := s.nextTxWindow(e.Relay, e.At, e.Airtime)
		if err = s.store.recordSent(ctx, e.At, e.Relay, e.Correlation.String(), e.Kind,
			e.Airtime, e.PowerDBm, e.Shadow, window); err == nil {
			if s.txWindows == nil {
				s.txWindows = map[string][]txStamp{}
			}
			s.txWindows[e.Relay] = next
		}
	case bus.TxDropped:
		err = s.store.recordTxDrop(ctx, e.At, e.Relay, e.Correlation.String(), e.Reason, e.Kind)
	case bus.RelayState:
		err = s.store.insertRelayState(ctx, e.At, e.Relay, e.State, e.Err)
	case bus.ObserverState:
		err = s.store.insertObserverState(ctx, e.At, e.Observer, e.State, e.Cause)
	default:
		return
	}
	s.recordOutcome(ev, err)
}

func (s *Sentinel) pruneNow(ctx context.Context) {
	if err := s.store.prune(ctx, time.Now(), s.retention, s.metricsRetention, s.maxFrames); err != nil {
		s.log.Warn("journal prune failed", zap.Error(err))
	}
}

// healthLogEvery paces the degraded journal's own noise: a disk that
// refuses every write under RF traffic must not earn a WARN per
// frame — that log volume is itself disk pressure, feeding the very
// saturation it reports.
const healthLogEvery = 30 * time.Second

// recordOutcome tallies one write and keeps the failure story honest:
// the FIRST failure logs at once, the rest fold into paced deltas,
// and the recovery is announced — a journal that fell sick and got
// better must say both, or a log rotation erases the whole episode.
func (s *Sentinel) recordOutcome(ev bus.Event, err error) {
	if err == nil {
		s.writes.Add(1)
		if s.degraded.CompareAndSwap(true, false) {
			s.log.Info("journal recovered",
				zap.Uint64("writes_failed", s.failures.Load()))
		}
		return
	}
	s.failures.Add(1)
	s.lastErr.Store(err.Error())
	s.lastFail.Store(time.Now().UnixMilli())
	first := s.degraded.CompareAndSwap(false, true)
	if first || time.Since(s.warnedAt) >= healthLogEvery {
		s.warnedAt = time.Now()
		fields := make([]zap.Field, 0, 6)
		fields = append(fields,
			zap.String("event", fmt.Sprintf("%T", ev)),
			zap.Uint64("failures", s.failures.Load()),
			zap.Bool("first", first),
			zap.Error(err),
		)
		fields = append(fields, frameCorrelation(ev)...)
		s.log.Warn("journal write failed", fields...)
	}
}

// frameCorrelation keeps a failed journal projection attached to the
// frame whose story it could not store. Non-frame events deliberately
// carry no invented correlation.
func frameCorrelation(ev bus.Event) []zap.Field {
	var relay, id string
	switch e := ev.(type) {
	case bus.FrameJudged:
		relay, id = e.Relay, e.Correlation.Short()
	case bus.FrameCorrupt:
		relay, id = e.Relay, e.Correlation.Short()
	case bus.FrameSent:
		relay, id = e.Relay, e.Correlation.Short()
	case bus.TxDropped:
		relay, id = e.Relay, e.Correlation.Short()
	default:
		return nil
	}
	return []zap.Field{zap.String("relay", relay), zap.String("corr", id)}
}

// Health is the journal's condition as outside readers see it.
type Health struct {
	Healthy  bool
	Writes   uint64
	Failures uint64
	// BusDropped counts the events the journal never even received —
	// backpressure at the subscription.
	BusDropped uint64
	LastErr    string
	LastFailAt time.Time
}

// Health reports the journal's condition — any goroutine.
func (s *Sentinel) Health() Health {
	h := Health{
		Healthy:    !s.degraded.Load(),
		Writes:     s.writes.Load(),
		Failures:   s.failures.Load(),
		BusDropped: s.sub.Dropped(),
	}
	if e, ok := s.lastErr.Load().(string); ok {
		h.LastErr = e
	}
	if ms := s.lastFail.Load(); ms != 0 {
		h.LastFailAt = time.UnixMilli(ms)
	}
	return h
}
