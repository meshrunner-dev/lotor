// Package origin is the origination pipeline every non-relay producer
// shares — a station, an application: the bounded (priority, not-before)
// queue the reference companion dispatches from, the atomic duty
// reservation taken before the channel is assessed and committed after
// the frame is on the air, the listen-before-talk ladder with its
// bounded retries, and the gate that keys the radio, journals a shadow,
// or refuses. An emission is bytes, a priority and an instant: nothing
// here knows what the bytes say. An owner labels what it composed —
// what the journal calls it, how it travels, when it stops being worth
// keying — and the pipeline carries those labels without ever reading
// the frame.
//
// The relay keeps its own transmit queue: its ladder is the reference
// repeater's, with forwarding semantics origination has no business in.
package origin

import (
	"context"
	"errors"
	rand "math/rand/v2"
	"time"

	"go.uber.org/zap"

	"meshrunner.dev/lotor/internal/bus"
	"meshrunner.dev/lotor/internal/config"
	"meshrunner.dev/lotor/internal/correlation"
	"meshrunner.dev/lotor/internal/logging"
	"meshrunner.dev/lotor/internal/radio"
)

// The reference companion's clocks: how long a frame may wait for the
// channel before the exhausted policy applies, the pacing between
// assessments, and how long a frame may wait for duty to free up.
const (
	DefaultLBTBound = 4 * time.Second
	DefaultLBTRetry = 200 * time.Millisecond
	DefaultDutyWait = 10 * time.Minute
)

// Route is how a frame travels — flooded to whoever hears it, or down
// a path to one node. The pipeline never decides it and never reads
// the frame to find it: the owner classifies what it composed, and
// gets the label back to keep its own tally.
type Route uint8

// The routes an owner may declare; the zero value is the one an owner
// that keeps no such tally leaves behind.
const (
	RouteUnclassified Route = iota
	RouteFlood
	RouteDirect
)

// Emission is one frame waiting to go on the air.
type Emission struct {
	// Frame is the packet as it will be keyed, marshalled once at
	// submission so the queue holds what the air will carry.
	Frame       []byte
	Correlation correlation.ID
	// Kind is what the journal calls this emission.
	Kind      string
	Priority  uint8
	NotBefore time.Time
	// BusySince starts the continuous busy spell a reception in
	// progress opened; zero until the first refusal.
	BusySince time.Time
	// Expires is when this frame stops being worth keying: the instant
	// its moment passes — an answer the asker has stopped waiting for,
	// an advert whose signed timestamp has gone stale. Zero never
	// expires. Past it the frame is dropped as "expired" rather than
	// held in front of what is behind it, which is what a bounded
	// queue with a ten-minute duty patience would otherwise do.
	Expires time.Time
	// Route is how the owner composed this frame to travel, carried
	// untouched for the tally the owner keeps about what it emitted.
	Route Route
}

// Policy is the origination gate and channel politeness, the
// protocol-neutral half of a producer's tx: block.
type Policy struct {
	// Mode is dry, shadow or on-air; empty reads as dry.
	Mode           string
	LBTThresholdDB float64
	// LBTExhausted is what a channel busy past the bound earns:
	// transmit anyway, or drop.
	LBTExhausted string
	// CAD gates the hardware's activity detection before keying.
	CAD bool
}

// Config names the producer to the bus and the log, and paces it.
type Config struct {
	// SourceKind and Source identify the producer on every event —
	// bus.SourceStation and its name, bus.SourceApplication and its.
	SourceKind string
	Source     string
	Bus        *bus.Bus
	Log        *zap.Logger
	// The clocks, the reference's when zero.
	LBTBound time.Duration
	LBTRetry time.Duration
	DutyWait time.Duration
}

// Pipeline is one producer's outbound door: its queue, and the emission
// of what the queue yields.
type Pipeline struct {
	Queue *Queue
	cfg   Config
}

// New makes a pipeline over a queue of the given depth.
func New(cfg Config, queueDepth int) *Pipeline {
	if cfg.Log == nil {
		cfg.Log = zap.NewNop()
	}
	if cfg.LBTBound <= 0 {
		cfg.LBTBound = DefaultLBTBound
	}
	if cfg.LBTRetry <= 0 {
		cfg.LBTRetry = DefaultLBTRetry
	}
	if cfg.DutyWait <= 0 {
		cfg.DutyWait = DefaultDutyWait
	}
	return &Pipeline{Queue: NewQueue(queueDepth), cfg: cfg}
}

// Outcome is what became of one emission. Exactly one of Sent,
// Requeued and Dropped is meaningful: Sent with its measured facts,
// Requeued when the frame went back to the queue to try again, Dropped
// with the reason the journal recorded.
type Outcome struct {
	Sent     bool
	Shadow   bool
	At       time.Time
	Airtime  time.Duration
	PowerDBm int8
	Requeued bool
	Dropped  string
}

// Emit carries one emission through the pipeline on the owner's turn:
// duty reserved atomically before the channel is assessed, so two
// producers cannot spend the same remaining budget; the LBT ladder,
// bounded; then the gate — shadow commits the estimated airtime and
// never keys, on-air keys and commits what the radio measured. A radio
// that reports airtime radiated even as it errors is accounted and
// announced: the frame was on the air.
func (p *Pipeline) Emit(ctx context.Context, item Emission, dev radio.Device,
	ledger *radio.AirtimeLedger, policy Policy, power int8,
) Outcome {
	if policy.Mode == "" || policy.Mode == config.TXDry {
		// A dry gate reaches no radio: whatever reached this queue under
		// it is refused here, so the contract "empty reads as dry" is
		// enforced where the keying would happen and not merely stated.
		return p.drop(item, "dry")
	}
	if dev == nil || ledger == nil {
		return p.drop(item, "radio-down")
	}
	if item.expired(time.Now()) {
		return p.drop(item, "expired")
	}
	airtime := dev.Airtime(len(item.Frame))
	reservation, outcome := p.reserveDuty(ctx, ledger, airtime, item)
	if reservation == nil {
		return outcome
	}
	defer reservation.Cancel()
	if outcome, proceed := p.clearChannel(ctx, dev, policy, item); !proceed {
		return outcome
	}
	shadow := policy.Mode == config.TXShadow
	at, actualAir, actualPower := time.Now(), airtime, power
	var txErr error
	if !shadow {
		txBase := correlation.WithContext(context.WithoutCancel(ctx), item.Correlation)
		txCtx, cancel := context.WithTimeout(txBase, 2*airtime+time.Second)
		report, err := dev.Transmit(txCtx, item.Frame, power)
		cancel()
		txErr = err
		if report.Airtime > 0 {
			at, actualAir, actualPower = report.At, report.Airtime, report.PowerDBm
		}
		if err != nil && report.Airtime == 0 {
			return p.drop(item, "tx-failed")
		}
	}
	reservation.Commit(at, actualAir)
	if p.cfg.Bus != nil {
		p.cfg.Bus.Publish(bus.FrameSent{SourceKind: p.cfg.SourceKind, Source: p.cfg.Source,
			Correlation: item.Correlation, At: at,
			Airtime: actualAir, PowerDBm: actualPower, Kind: item.Kind, Shadow: shadow, Raw: item.Frame})
	}
	p.cfg.Log.Debug("frame sent", zap.String("corr", item.Correlation.Short()),
		zap.String("kind", item.Kind), zap.Uint8("priority", item.Priority),
		zap.Bool("shadow", shadow), zap.Error(txErr))
	logging.Trace(p.cfg.Log, "tx emission accounted", zap.String("corr", item.Correlation.Short()),
		zap.Uint8("priority", item.Priority), zap.Duration("airtime", actualAir),
		zap.Int8("power_dbm", actualPower), zap.Bool("shadow", shadow))
	return Outcome{Sent: true, Shadow: shadow, At: at, Airtime: actualAir, PowerDBm: actualPower}
}

// reserveDuty waits for the shared ledger to admit the airtime, up to
// the configured patience or the emission's own expiry, whichever
// comes first; a budget that will never free, or not in
// time, drops the frame as duty, and a wait the owner cancelled drops
// it as cancelled — each counted under its own name, so a shutdown
// never reads as a saturated ledger.
func (p *Pipeline) reserveDuty(ctx context.Context, ledger *radio.AirtimeLedger,
	airtime time.Duration, item Emission,
) (*radio.AirtimeReservation, Outcome) {
	deadline, exhausted := time.Now().Add(p.cfg.DutyWait), "duty"
	if !item.Expires.IsZero() && item.Expires.Before(deadline) {
		// Its own moment ends before the pipeline's patience does: the
		// wait is cut there, and the drop is named for what actually
		// ended it.
		deadline, exhausted = item.Expires, "expired"
	}
	for {
		now := time.Now()
		reservation, freeAt, never := ledger.Reserve(now, airtime)
		if reservation != nil {
			return reservation, Outcome{}
		}
		if never || freeAt.After(deadline) {
			return nil, p.drop(item, exhausted)
		}
		select {
		case <-ctx.Done():
			return nil, p.drop(item, "cancelled")
		case <-time.After(max(0, freeAt.Sub(now))):
		}
	}
}

// clearChannel is the LBT ladder. A reception in progress sends the
// frame back to the queue for a paced retry — the channel is busy
// with something we can hear, and the queue's not-before is the
// waiting position — until the busy spell outlasts the bound and the
// exhausted policy decides. A busy verdict retries in place; past the
// bound it transmits anyway, the mesh's convention, unless the site
// chose to drop.
func (p *Pipeline) clearChannel(ctx context.Context, dev radio.Device, policy Policy,
	item Emission,
) (Outcome, bool) {
	if !policy.CAD {
		return Outcome{}, true
	}
	deadline := time.Now().Add(p.cfg.LBTBound)
	for {
		attemptCtx, cancel := context.WithDeadline(ctx, deadline)
		busy, err := dev.AssessChannel(attemptCtx, policy.LBTThresholdDB)
		cancel()
		if errors.Is(err, radio.ErrBusyReceiving) {
			now := time.Now()
			if item.BusySince.IsZero() {
				item.BusySince = now
			}
			busyFor := now.Sub(item.BusySince)
			if busyFor >= p.cfg.LBTBound && policy.LBTExhausted == config.LBTDrop {
				return p.drop(item, "lbt"), false
			}
			retry := p.cfg.LBTRetry/2 + rand.N(p.cfg.LBTRetry) //nolint:gosec // timing jitter, not security
			item.NotBefore = now.Add(retry)
			logging.Trace(p.cfg.Log, "tx requeued for reception",
				zap.String("corr", item.Correlation.Short()), zap.Duration("retry_in", retry),
				zap.Duration("busy_for", busyFor))
			return p.Requeue(item), false
		}
		if err != nil {
			return p.drop(item, "lbt-failed"), false
		}
		if !busy {
			return Outcome{}, true
		}
		if time.Now().After(deadline) {
			if policy.LBTExhausted == config.LBTDrop {
				return p.drop(item, "lbt"), false
			}
			p.cfg.Log.Warn("channel busy past the LBT bound, transmitting anyway",
				zap.String("corr", item.Correlation.Short()))
			return Outcome{}, true
		}
		retry := p.cfg.LBTRetry/2 + rand.N(p.cfg.LBTRetry) //nolint:gosec // timing jitter, not security
		select {
		case <-ctx.Done():
			return Outcome{}, false
		case <-time.After(retry):
		}
	}
}

// Requeue puts an emission back for a later turn; a frame whose next
// turn falls past its expiry, and a full queue, drop it — counted.
func (p *Pipeline) Requeue(item Emission) Outcome {
	if item.expired(item.NotBefore) {
		return p.drop(item, "expired")
	}
	if !p.Queue.Offer(item) {
		return p.drop(item, "queue-full")
	}
	return Outcome{Requeued: true}
}

// Drop refuses one emission for a reason the journal records — what an
// owner calls for the frames it gives up on itself, a queue drained
// by a reset.
func (p *Pipeline) Drop(item Emission, reason string) Outcome { return p.drop(item, reason) }

func (p *Pipeline) drop(item Emission, reason string) Outcome {
	p.cfg.Log.Debug("frame dropped", zap.String("corr", item.Correlation.Short()),
		zap.String("kind", item.Kind), zap.Uint8("priority", item.Priority), zap.String("reason", reason))
	if p.cfg.Bus != nil {
		p.cfg.Bus.Publish(bus.TxDropped{SourceKind: p.cfg.SourceKind, Source: p.cfg.Source,
			Correlation: item.Correlation, At: time.Now(), Reason: reason, Kind: item.Kind})
	}
	return Outcome{Dropped: reason}
}

// expired reports whether this emission's moment has passed at the
// given instant. An emission with no expiry keeps its turn forever.
func (e Emission) expired(at time.Time) bool {
	return !e.Expires.IsZero() && !at.Before(e.Expires)
}
