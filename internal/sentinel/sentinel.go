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
	return &Sentinel{
		store: st, bus: b, sub: b.Subscribe(256),
		log: log, retention: retention, maxFrames: maxFrames,
		journalPath: journalPath,
	}, nil
}

// RecentFrames exposes the journal to future consumers (CLI, web),
// filtered in SQL.
func (s *Sentinel) RecentFrames(ctx context.Context, fq FrameQuery) ([]Frame, error) {
	return s.store.RecentFrames(ctx, fq)
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
			Bytes: e.Bytes, RSSI: e.RSSI, SNR: e.SNR, Airtime: e.Airtime,
		})
	case bus.FrameJudged:
		err = s.store.applyJudgement(ctx, e.Txn.String(), e.Relay, Frame{
			Type: e.Type, Route: e.Route, PathLen: e.PathLen,
			Verdict: e.Verdict, DuplicateOf: e.DuplicateOf,
			Node: e.Node, PubKey: e.PubKey, Detail: e.Detail,
		})
	case bus.FrameCorrupt:
		err = s.store.recordCorrupt(ctx, e.At, e.Relay, e.Err)
	case bus.NoiseFloor:
		// Twice on purpose: the last value for O(1) status reads, a raw
		// point for the consolidated history.
		if err = s.store.upsertNoiseFloor(ctx, e.At, e.Relay, e.DBm); err == nil {
			err = s.store.insertMetric(ctx, "noise_floor", e.Relay, e.At, e.DBm)
		}
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
