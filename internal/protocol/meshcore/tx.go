package meshcore

import (
	"context"
	"errors"
	"math/rand/v2"
	"time"

	"go.uber.org/zap"

	"meshrunner.dev/pkg/meshcore"

	"meshrunner.dev/lotor/internal/bus"
	"meshrunner.dev/lotor/internal/protocol"
	"meshrunner.dev/lotor/internal/radio"
	"meshrunner.dev/lotor/internal/txn"
)

// The transmit pipeline's fixed policy, matching the reference
// repeater's active defaults.
const (
	// Retransmission jitter desynchronises repeaters that heard the
	// same frame: a random delay in [0, 5×airtime×factor), the
	// reference's mechanism and factors. (Its score-based rx-delay
	// exists too but ships disabled there; parity keeps it out.)
	floodDelayFactor  = 0.5
	directDelayFactor = 0.3

	// LBT retry pacing: the reference's 200 ms nominal, jittered so
	// two repeaters backing off together do not re-collide, bounded to
	// its 4 s maximum before the exhausted policy applies.
	lbtRetryNominal = 200 * time.Millisecond
	lbtMaxWait      = 4 * time.Second

	// The first flood advert waits out a short random settling delay,
	// so a site rebooting a fleet does not open with a chorus.
	advertFirstMin = 30 * time.Second
	advertFirstMax = 60 * time.Second
)

// Priorities: direct traffic answers someone and goes first.
const (
	prioDirect = 0
	prioFlood  = 1
)

// txEntry is one scheduled emission.
type txEntry struct {
	pkt       *meshcore.Packet
	kind      string
	origin    txn.ID
	priority  int
	notBefore time.Time
}

// txQueue is the bounded outbound queue: entries wait out notBefore,
// then leave in priority order — the reference's queueOutbound shape.
type txQueue struct {
	entries []txEntry
	depth   int
}

// push refuses when full; nothing is evicted silently.
func (q *txQueue) push(e txEntry) bool {
	if len(q.entries) >= q.depth {
		return false
	}
	q.entries = append(q.entries, e)
	return true
}

// pop removes and returns the best due entry: lowest priority number
// first, earliest schedule within it.
func (q *txQueue) pop(now time.Time) (txEntry, bool) {
	best := -1
	for i, e := range q.entries {
		if now.Before(e.notBefore) {
			continue
		}
		if best < 0 || e.priority < q.entries[best].priority ||
			(e.priority == q.entries[best].priority && e.notBefore.Before(q.entries[best].notBefore)) {
			best = i
		}
	}
	if best < 0 {
		return txEntry{}, false
	}
	e := q.entries[best]
	q.entries = append(q.entries[:best], q.entries[best+1:]...)
	return e, true
}

// nextDue reports how long until something is due; false when empty.
func (q *txQueue) nextDue(now time.Time) (time.Duration, bool) {
	var soonest time.Time
	for _, e := range q.entries {
		if soonest.IsZero() || e.notBefore.Before(soonest) {
			soonest = e.notBefore
		}
	}
	if soonest.IsZero() {
		return 0, false
	}
	return max(0, soonest.Sub(now)), true
}

// Arm hands the engine its transmit policy; refusing here makes the
// relay stillborn instead of silently dry.
func (e *engine) Arm(p protocol.TXPolicy) error {
	if e.id == nil {
		return errors.New("tx: relaying needs a node identity — the path carries our hash")
	}
	e.policy = p
	e.queue = &txQueue{depth: p.QueueDepth}
	return nil
}

// txEnabled reports whether the pipeline runs at all.
func (e *engine) txEnabled() bool {
	return e.policy.Mode == "shadow" || e.policy.Mode == "on-air"
}

// enqueue schedules one emission, publishing the drop when the queue
// refuses. jitterFactor scales the desynchronisation delay.
func (e *engine) enqueue(dev radio.Device, pkt *meshcore.Packet, kind string,
	origin txn.ID, priority int, jitterFactor float64,
) {
	delay := time.Duration(0)
	if jitterFactor > 0 {
		air := dev.Airtime(pkt.RawLength())
		span := max(time.Duration(5*jitterFactor*float64(air)), time.Millisecond)
		delay = rand.N(span) //nolint:gosec // desync jitter, not security
	}
	entry := txEntry{
		pkt: pkt, kind: kind, origin: origin,
		priority: priority, notBefore: time.Now().Add(delay),
	}
	if !e.queue.push(entry) {
		e.log.Warn("tx queue full, dropping", zap.String("kind", kind),
			zap.String("txn", origin.Short()))
		e.bus.Publish(bus.TxDropped{
			Relay: e.relay, Txn: origin, At: time.Now(), Reason: "queue-full",
		})
	}
}

// relayFor turns a judged reception into its scheduled retransmission.
// The packet is copied — the transforms must not write through the
// received frame — and the transform follows the verdict.
func (e *engine) relayFor(dev radio.Device, pkt *meshcore.Packet, verdict string,
	origin txn.ID, snr float64,
) {
	cp := *pkt
	switch verdict {
	case verdictRelayFlood:
		if err := cp.AppendPathHash(e.selfHash(cp.PathHashSize())); err != nil {
			e.log.Warn("flood relay path append failed", zap.Error(err))
			return
		}
		e.enqueue(dev, &cp, "relay-flood", origin, prioFlood, floodDelayFactor)
	case verdictRelayDirect:
		if _, err := cp.ConsumeNextHop(); err != nil {
			e.log.Warn("direct relay hop consume failed", zap.Error(err))
			return
		}
		e.enqueue(dev, &cp, "relay-direct", origin, prioDirect, directDelayFactor)
	case verdictTraceTransit:
		// The trace transit appends our SNR reading — quarter-dB, one
		// raw byte — to the walked path (Mesh::onRecvPacket).
		grown := make([]byte, 0, len(cp.Path)+1)
		grown = append(grown, cp.Path...)
		grown = append(grown, byte(int8(snr*4)))
		cp.Path = grown
		cp.PathLen++ // TRACE paths count raw bytes, no size bits
		e.enqueue(dev, &cp, "relay-trace", origin, prioDirect, directDelayFactor)
	}
}

// selfHash is this node's path identity at the packet's hash size.
func (e *engine) selfHash(size int) []byte { return e.id.PubKey[:size] }

// txWait reports how long Receive may block before the pipeline needs
// the radio: the earliest of the queue's schedule and the advert
// clocks. ok is false when nothing is ever due.
func (e *engine) txWait(now time.Time) (time.Duration, bool) {
	var waits []time.Duration
	if d, ok := e.queue.nextDue(now); ok {
		waits = append(waits, d)
	}
	if !e.nextFloodAdvert.IsZero() {
		waits = append(waits, max(0, e.nextFloodAdvert.Sub(now)))
	}
	if !e.nextLocalAdvert.IsZero() {
		waits = append(waits, max(0, e.nextLocalAdvert.Sub(now)))
	}
	if len(waits) == 0 {
		return 0, false
	}
	m := waits[0]
	for _, w := range waits[1:] {
		m = min(m, w)
	}
	return m, true
}

// scheduleAdverts starts the advert clocks when the pipeline comes up.
func (e *engine) scheduleAdverts(now time.Time) {
	if e.p.AdvertFloodInterval > 0 {
		e.nextFloodAdvert = now.Add(advertFirstMin +
			rand.N(advertFirstMax-advertFirstMin)) //nolint:gosec // settling jitter, not security
	}
	if e.p.AdvertLocalInterval > 0 {
		e.nextLocalAdvert = now.Add(e.p.AdvertLocalInterval)
	}
}

// dueAdverts builds and enqueues any advert whose clock has struck,
// and winds that clock forward.
func (e *engine) dueAdverts(dev radio.Device, now time.Time) {
	if !e.nextFloodAdvert.IsZero() && !now.Before(e.nextFloodAdvert) {
		e.nextFloodAdvert = now.Add(e.p.AdvertFloodInterval)
		e.advert(dev, now, "advert-flood", prioFlood, false)
	}
	if !e.nextLocalAdvert.IsZero() && !now.Before(e.nextLocalAdvert) {
		e.nextLocalAdvert = now.Add(e.p.AdvertLocalInterval)
		e.advert(dev, now, "advert-local", prioFlood, true)
	}
}

// advert builds this node's signed announcement. Local adverts are
// zero-hop: direct route, empty path — the form the band rules allow
// first. Our own hash is witnessed so the echo judges as a duplicate.
func (e *engine) advert(dev radio.Device, now time.Time, kind string, priority int, local bool) {
	pkt, err := meshcore.BuildAdvert(e.id, now, &meshcore.AdvertData{
		Type: meshcore.AdvTypeRepeater,
		Name: e.p.NodeName,
	})
	if err != nil {
		e.log.Warn("advert build failed", zap.Error(err))
		return
	}
	if local {
		pkt.Header = meshcore.MakeHeader(meshcore.RouteDirect,
			meshcore.PayloadTypeAdvert, meshcore.PayloadVer1)
	}
	id := txn.New()
	e.seen.witness(pkt.Hash(), id, now)
	e.enqueue(dev, pkt, kind, id, priority, 0)
}

// txPhase serves one due emission: LBT first, then the gate decides
// whether the radio is keyed or the emission is journalled as shadow.
// Radio faults bubble — a sick radio restarts the session, the same
// contract as reception.
func (e *engine) txPhase(ctx context.Context, dev radio.Device) error {
	e.dueAdverts(dev, time.Now())
	entry, ok := e.queue.pop(time.Now())
	if !ok {
		return nil
	}
	log := e.log.With(zap.String("txn", entry.origin.Short()), zap.String("kind", entry.kind))
	if proceed, err := e.clearChannel(ctx, dev, log); err != nil || !proceed {
		return err
	}
	raw, err := entry.pkt.MarshalBinary()
	if err != nil {
		log.Warn("tx marshal failed", zap.Error(err))
		return nil
	}
	sent := bus.FrameSent{
		Relay: e.relay, Txn: entry.origin, Kind: entry.kind,
		PowerDBm: e.policy.PowerDBm, Shadow: e.policy.Mode == "shadow",
	}
	if sent.Shadow {
		sent.At, sent.Airtime = time.Now(), dev.Airtime(len(raw))
	} else {
		report, err := dev.Transmit(ctx, raw, e.policy.PowerDBm)
		if err != nil {
			return err
		}
		sent.At, sent.Airtime, sent.PowerDBm = report.At, report.Airtime, report.PowerDBm
	}
	log.Info("frame sent", zap.Bool("shadow", sent.Shadow),
		zap.Duration("airtime", sent.Airtime), zap.Int8("power_dbm", sent.PowerDBm))
	e.bus.Publish(sent)
	return nil
}

// clearChannel is the LBT wait: bounded retries while the channel is
// busy, then the site's exhausted policy — transmit anyway (the
// mesh's convention) or a counted drop.
func (e *engine) clearChannel(ctx context.Context, dev radio.Device, log *zap.Logger) (bool, error) {
	deadline := time.Now().Add(lbtMaxWait)
	for {
		busy, err := dev.AssessChannel(ctx, e.policy.LBTThresholdDB)
		if err != nil {
			return false, err
		}
		if !busy {
			return true, nil
		}
		if time.Now().After(deadline) {
			if e.policy.LBTExhausted == "drop" {
				log.Warn("channel busy past the LBT bound, dropping")
				e.bus.Publish(bus.TxDropped{
					Relay: e.relay, At: time.Now(), Reason: "lbt",
				})
				return false, nil
			}
			log.Warn("channel busy past the LBT bound, transmitting anyway")
			return true, nil
		}
		retry := lbtRetryNominal/2 + rand.N(lbtRetryNominal) //nolint:gosec // backoff jitter, not security
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-time.After(retry):
		}
	}
}
