// Package sentinel is the observation and archival instantiation: it
// consumes the event bus and journals what the daemon hears and
// decides. It is optional by design — a deployment may run none, and
// nothing elsewhere depends on one existing. The bus makes that free.
package sentinel

import (
	"context"
	"time"

	"go.uber.org/zap"

	"meshrunner.dev/lotor/internal/bus"
)

const pruneEvery = time.Hour

// Sentinel journals bus traffic into its store.
type Sentinel struct {
	store     *store
	bus       *bus.Bus
	log       *zap.Logger
	retention time.Duration
}

// Open prepares the journal. The path may be MemoryJournal for hosts
// whose storage dislikes continuous writes.
func Open(journalPath string, retention time.Duration, b *bus.Bus, log *zap.Logger) (*Sentinel, error) {
	st, err := openStore(journalPath)
	if err != nil {
		return nil, err
	}
	return &Sentinel{store: st, bus: b, log: log, retention: retention}, nil
}

// RecentFrames exposes the journal to future consumers (CLI, web). A
// txn prefix — the short displayed form — filters to its transaction.
func (s *Sentinel) RecentFrames(ctx context.Context, txnPrefix string, limit int) ([]Frame, error) {
	return s.store.RecentFrames(ctx, txnPrefix, limit)
}

// Run consumes the bus until the context ends. Journal errors are
// logged, never fatal: an ailing sentinel must not take relays down.
func (s *Sentinel) Run(ctx context.Context) {
	sub := s.bus.Subscribe(256)
	defer sub.Close()
	defer func() { _ = s.store.Close() }()

	s.pruneNow(ctx)
	ticker := time.NewTicker(pruneEvery)
	defer ticker.Stop()

	var reportedDrops uint64
	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-sub.C:
			if !ok {
				return
			}
			s.process(ctx, ev)
		case <-ticker.C:
			s.pruneNow(ctx)
			if d := sub.Dropped(); d > reportedDrops {
				s.log.Warn("journal fell behind the bus",
					zap.Uint64("events_dropped", d-reportedDrops))
				reportedDrops = d
			}
		}
	}
}

func (s *Sentinel) process(ctx context.Context, ev bus.Event) {
	var err error
	switch e := ev.(type) {
	case bus.FrameHeard:
		err = s.store.insertHeard(ctx, Frame{
			Txn: e.Txn.String(), Relay: e.Relay, At: e.At,
			Bytes: e.Bytes, RSSI: e.RSSI, SNR: e.SNR, Airtime: e.Airtime,
		})
	case bus.FrameJudged:
		err = s.store.applyJudgement(ctx, e.Txn.String(), Frame{
			Type: e.Type, Route: e.Route, PathLen: e.PathLen,
			Verdict: e.Verdict, DuplicateOf: e.DuplicateOf,
			Node: e.Node, PubKey: e.PubKey, Detail: e.Detail,
		})
	case bus.RelayState:
		err = s.store.insertRelayState(ctx, time.Now(), e.Relay, e.State, e.Err)
	default:
		return
	}
	if err != nil {
		s.log.Warn("journal write failed", zap.Error(err))
	}
}

func (s *Sentinel) pruneNow(ctx context.Context) {
	if err := s.store.prune(ctx, time.Now().Add(-s.retention)); err != nil {
		s.log.Warn("journal prune failed", zap.Error(err))
	}
}
