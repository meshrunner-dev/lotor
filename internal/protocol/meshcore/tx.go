package meshcore

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"sync"
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

	// bootAdvertDelay paces the boot announcement: one zero-hop advert
	// shortly after the pipeline comes up, the reference's exact
	// gesture (its main sends one 16 s after boot, unconditionally).
	// A reboot is news to the direct neighbourhood, never to the whole
	// mesh: the first flood advert waits out its full interval.
	bootAdvertDelay = 16 * time.Second

	// clockRetry is how long the advert clocks defer while the wall
	// clock is implausible.
	clockRetry = time.Minute
)

// clockEpoch bounds plausibility: an advert stamps the wall clock into
// a signed payload, and neighbours keep only the newest timestamp per
// node — so announcing from a host that has not found the network yet
// (a Pi without an RTC boots into the past) would waste the emission
// or, worse, plant a timestamp the real clock must later climb over.
var clockEpoch = time.Date(2025, time.January, 1, 0, 0, 0, 0, time.UTC)

// reasonRateLimited names every limiter refusal in the drop tally.
const reasonRateLimited = "rate-limited"

// Priorities, the reference's ladder: routed traffic and zero-hop
// sends go first (0); flood relays carry their hop count after the
// append — closer sources beat distant ones; a node's own flood
// adverts are deliberately de-prioritised (3); trace transits sit at
// the back (5). Lower serves first, ties by earliest schedule.
const (
	prioDirect      = 0
	prioFloodReply  = 1
	prioPathReturn  = 2
	prioFloodAdvert = 3
	prioTrace       = 5
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
	if e.p.NodeName == "" {
		return errors.New("tx: announcing needs node_name — the advert carries it, " +
			"and a config slug is not a name")
	}
	if e.p.DutyCyclePct <= 0 {
		return fmt.Errorf(
			"tx: mode %s needs duty_cycle_pct on this relay's band — set the lawful ceiling, "+
				"or 100 to state the band has none", p.Mode)
	}
	e.policy = p
	e.queue = &txQueue{depth: p.QueueDepth}
	// The sliding hour did not restart with the process: what the
	// journal remembers being spent is spent, or a crash-loop could
	// launder the budget.
	dl := &dutyLedger{budget: time.Duration(float64(time.Hour) * e.p.DutyCyclePct / 100)}
	// The window prunes as a time-ordered prefix: feed it in order,
	// whatever order the journal rows arrived in.
	spent := slices.Clone(p.Spent)
	slices.SortFunc(spent, func(a, b protocol.Spent) int { return a.At.Compare(b.At) })
	for _, sp := range spent {
		dl.record(sp.At, sp.Airtime)
	}
	e.duty = dl
	e.started = time.Now()
	e.advertAsk = make(chan string, 1)
	// What "changed since" means for us: this process's pipeline came
	// up — the durable equivalent of the reference's mod timestamp.
	e.discoverySince = time.Now()
	return nil
}

// txEnabled reports whether the pipeline runs at all.
func (e *engine) txEnabled() bool {
	switch e.policy.Mode {
	case "shadow", "on-air-zero-hop", "on-air":
		return true
	}
	return false
}

// paperOnly decides whether one emission is journalled rather than
// keyed. Shadow keys nothing; the zero-hop rung keys only what stays
// with the direct neighbourhood — a local advert, a discovery answer —
// while everything routable continues on paper, so the parity audit
// runs on while the node becomes discoverable.
func (e *engine) paperOnly(pkt *meshcore.Packet) bool {
	switch e.policy.Mode {
	case "shadow":
		return true
	case "on-air-zero-hop":
		return !pkt.IsRouteDirect() || pkt.PathHashCount() != 0
	}
	return false
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
	e.enqueueAfter(pkt, kind, origin, priority, delay)
}

// enqueueAfter schedules one emission a fixed delay out, publishing
// the drop when the queue refuses.
func (e *engine) enqueueAfter(pkt *meshcore.Packet, kind string, origin txn.ID,
	priority int, delay time.Duration,
) {
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
func (e *engine) relayFor(dev radio.Device, rx *reception, verdict string) {
	pkt, origin, snr := rx.pkt, rx.id, rx.frame.SNR
	cp := *pkt
	switch verdict {
	case verdictRelayFlood:
		if err := cp.AppendPathHash(e.selfHash(cp.PathHashSize())); err != nil {
			e.abandon(origin, "malformed", "flood relay path append failed", err)
			return
		}
		// Priority = distance: the hop count with our hash appended.
		e.enqueue(dev, &cp, "relay-flood", origin, cp.PathHashCount(), floodDelayFactor)
	case verdictRelayDirect:
		if cp.PayloadType() == meshcore.PayloadTypeMultipart {
			e.forwardMultipart(&cp, origin)
			return
		}
		if _, err := cp.ConsumeNextHop(); err != nil {
			e.abandon(origin, "malformed", "direct relay hop consume failed", err)
			return
		}
		// The reference forwards ACKs with no delay at all: they are
		// the confirmation a sender is timing out on, and every hop's
		// jitter is latency it pays (Mesh::routeDirectRecvAcks).
		jitter := directDelayFactor
		if cp.PayloadType() == meshcore.PayloadTypeAck {
			jitter = 0
		}
		e.enqueue(dev, &cp, "relay-direct", origin, prioDirect, jitter)
	case verdictDiscover:
		e.respondDiscover(dev, pkt, origin, snr)
	case verdictAnon:
		e.respondAnon(rx, origin)
	case verdictRequest:
		e.respondRequest(rx, origin)
	case verdictRelayTrace:
		// A trace whose next target hop is us walks on: our SNR
		// reading — quarter-dB, one raw byte — joins the walked path
		// (Mesh::onRecvPacket).
		grown := make([]byte, 0, len(cp.Path)+1)
		grown = append(grown, cp.Path...)
		grown = append(grown, byte(int8(snr*4)))
		cp.Path = grown
		cp.PathLen++ // TRACE paths count raw bytes, no size bits
		e.enqueue(dev, &cp, "relay-trace", origin, prioTrace, directDelayFactor)
	}
}

// abandon reports an emission the pipeline gave up on before it ever
// reached the queue: the audit trail must show the refusal, not a
// judgement that quietly led nowhere.
func (e *engine) abandon(origin txn.ID, reason, msg string, err error) {
	e.log.Warn(msg, zap.String("txn", origin.Short()), zap.Error(err))
	e.bus.Publish(bus.TxDropped{
		Relay: e.relay, Txn: origin, At: time.Now(), Reason: reason,
	})
}

// dropOnFault counts an emission lost to a radio fault. A daemon
// shutting down is not a fault: that entry goes unsent like the rest
// of the queue, and counting it would blame the radio for the stop.
func (e *engine) dropOnFault(ctx context.Context, origin txn.ID, err error) {
	if ctx.Err() != nil {
		return
	}
	e.abandon(origin, "tx-failed", "emission lost to a radio fault", err)
}

// forwardMultipart unwraps a direct MULTIPART into the plain ACK it
// wraps and forwards that instead — the reference's
// forwardMultipartDirect. A sender emitting redundant ACK copies
// stamps each with a different remaining-count nibble, so the copies
// all hash differently; deduplicating on the unwrapped form collapses
// them to one forward instead of N. The forward waits out the rest of
// the burst — (remaining+1) × 300 ms, the copies' own spacing — and no
// extra copies of our own are added (the reference's extra-ACK count
// ships at zero).
func (e *engine) forwardMultipart(cp *meshcore.Packet, origin txn.ID) {
	if len(cp.Payload) < 5 || meshcore.PayloadType(cp.Payload[0]&0x0F) != meshcore.PayloadTypeAck {
		e.abandon(origin, "malformed", "multipart wraps no ack", nil)
		return
	}
	remaining := int(cp.Payload[0] >> 4)
	// Dedup on the unwrapped shape, exactly as the reference hashes it:
	// the multipart header over the stripped payload.
	stripped := *cp
	stripped.Payload = cp.Payload[1:]
	if _, dup := e.seen.witness(stripped.Hash(), origin, time.Now()); dup {
		e.bus.Publish(bus.TxDropped{
			Relay: e.relay, Txn: origin, At: time.Now(), Reason: "duplicate",
		})
		return
	}
	if _, err := cp.ConsumeNextHop(); err != nil {
		e.abandon(origin, "malformed", "multipart hop consume failed", err)
		return
	}
	ack, err := meshcore.BuildAck(cp.Payload[1:5])
	if err != nil {
		e.abandon(origin, "malformed", "multipart ack rebuild failed", err)
		return
	}
	ack.Header = meshcore.MakeHeader(meshcore.RouteDirect,
		meshcore.PayloadTypeAck, meshcore.PayloadVer1)
	ack.Path = append([]byte(nil), cp.Path...)
	ack.PathLen = cp.PathLen
	e.enqueueAfter(ack, "relay-ack", origin, prioDirect,
		time.Duration(remaining+1)*300*time.Millisecond)
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

// scheduleAdverts seeds the advert clocks the first time the pipeline
// comes up. The clocks live on the engine, which outlives a radio
// session: re-seeding them on every Run would turn a 47 h flood
// advert into one per session.
//
// The local clock always seeds — the boot announcement goes out even
// when recurring local adverts are disabled, the reference's
// unconditional gesture — and the flood clock waits its whole
// interval: rebooting is not a reason to address the entire mesh.
func (e *engine) scheduleAdverts(now time.Time) {
	if e.p.AdvertFloodInterval > 0 && e.nextFloodAdvert.IsZero() {
		e.nextFloodAdvert = now.Add(e.p.AdvertFloodInterval)
	}
	if e.nextLocalAdvert.IsZero() {
		first := bootAdvertDelay
		if e.p.AdvertLocalInterval > 0 && e.p.AdvertLocalInterval < first {
			first = e.p.AdvertLocalInterval
		}
		e.nextLocalAdvert = now.Add(first)
	}
}

// dueAdverts builds and enqueues any advert whose clock has struck,
// and winds that clock forward.
func (e *engine) dueAdverts(dev radio.Device, now time.Time) {
	if e.advertDue(now) && now.Before(clockEpoch) {
		// The host has not found the network yet: hold the
		// announcements, try again shortly. Everything else — relays,
		// discovery answers — carries no wall time and flows normally.
		if !e.clockWarned {
			e.log.Warn("wall clock implausible, holding adverts",
				zap.Time("clock", now), zap.Duration("retry", clockRetry))
			e.clockWarned = true
		}
		if !e.nextFloodAdvert.IsZero() && !now.Before(e.nextFloodAdvert) {
			e.nextFloodAdvert = now.Add(clockRetry)
		}
		if !e.nextLocalAdvert.IsZero() && !now.Before(e.nextLocalAdvert) {
			e.nextLocalAdvert = now.Add(clockRetry)
		}
		return
	}
	switch {
	case !e.nextFloodAdvert.IsZero() && !now.Before(e.nextFloodAdvert):
		e.nextFloodAdvert = now.Add(e.p.AdvertFloodInterval)
		// Both adverts in one pass would carry the same second in
		// their timestamp, hence the same packet hash: a neighbour
		// hearing the zero-hop copy first would judge the routable one
		// a duplicate and never re-flood it. The reference winds both
		// clocks for the same reason (MyMesh: "so they don't overlap").
		if !e.nextLocalAdvert.IsZero() {
			e.windLocalAdvert(now)
		}
		e.advert(dev, now, "advert-flood", false)
	case !e.nextLocalAdvert.IsZero() && !now.Before(e.nextLocalAdvert):
		e.windLocalAdvert(now)
		e.advert(dev, now, "advert-local", true)
	}
}

// RequestAdvert queues one operator-triggered announcement: zero-hop
// by default, routable when flood. Safe from any goroutine. It has no
// limiter of its own — an explicit operator order answers to the duty
// budget alone, like every emission.
func (e *engine) RequestAdvert(flood bool) error {
	if !e.txEnabled() {
		return errors.New("the transmit gate is dry — nothing may be sent")
	}
	if time.Now().Before(clockEpoch) {
		return errors.New("wall clock implausible — neighbours keep the newest advert timestamp, " +
			"and this one would poison ours")
	}
	kind := "advert-local"
	if flood {
		kind = "advert-flood"
	}
	select {
	case e.advertAsk <- kind:
	default:
		return errors.New("an advert is already pending")
	}
	e.wakeMu.Lock()
	if e.wakeRx != nil {
		e.wakeRx() // close the receive window: serve the order now
	}
	e.wakeMu.Unlock()
	return nil
}

// drainAdvertAsk serves an operator-triggered announcement. The
// clocks wind as if the scheduler itself had fired: the mesh just
// heard from us, and a scheduled advert moments later would be a
// byte-identical duplicate a neighbour would dedup away.
func (e *engine) drainAdvertAsk(dev radio.Device, now time.Time) {
	select {
	case kind := <-e.advertAsk:
		if kind == "advert-flood" {
			if e.p.AdvertFloodInterval > 0 {
				e.nextFloodAdvert = now.Add(e.p.AdvertFloodInterval)
			}
			e.windLocalAdvert(now)
			e.advert(dev, now, kind, false)
			return
		}
		e.windLocalAdvert(now)
		e.advert(dev, now, kind, true)
	default:
	}
}

// windLocalAdvert schedules the next local advert, or stops the clock
// when the boot announcement was the only one asked for.
func (e *engine) windLocalAdvert(now time.Time) {
	if e.p.AdvertLocalInterval > 0 {
		e.nextLocalAdvert = now.Add(e.p.AdvertLocalInterval)
	} else {
		e.nextLocalAdvert = time.Time{}
	}
}

// advertDue reports whether either advert clock has struck.
func (e *engine) advertDue(now time.Time) bool {
	return (!e.nextFloodAdvert.IsZero() && !now.Before(e.nextFloodAdvert)) ||
		(!e.nextLocalAdvert.IsZero() && !now.Before(e.nextLocalAdvert))
}

// advert builds this node's signed announcement. Local adverts are
// zero-hop: direct route, empty path — the form the band rules allow
// first. Our own hash is witnessed so the echo judges as a duplicate.
func (e *engine) advert(dev radio.Device, now time.Time, kind string, local bool) {
	data := &meshcore.AdvertData{
		Type: meshcore.AdvTypeRepeater,
		Name: e.p.NodeName,
	}
	if e.p.NodeLat != 0 || e.p.NodeLon != 0 {
		data.HasLoc = true
		data.LatE6 = int32(e.p.NodeLat * 1e6)
		data.LonE6 = int32(e.p.NodeLon * 1e6)
	}
	pkt, err := meshcore.BuildAdvert(e.id, now, data)
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
	prio := prioFloodAdvert
	if local {
		prio = prioDirect // zero-hop sends go out at the front
	}
	e.enqueue(dev, pkt, kind, id, prio, 0)
}

// lbtOutcome is what the channel assessment decided about one entry.
type lbtOutcome int

const (
	lbtGo      lbtOutcome = iota // the channel is clear enough
	lbtRequeue                   // the radio has reception work: collect first
	lbtDrop                      // busy past the bound, and the site drops
)

// txPhase serves one due emission: LBT first, then the gate decides
// whether the radio is keyed or the emission is journalled as shadow.
// Radio faults bubble — a sick radio restarts the session, the same
// contract as reception — but a radio busy receiving is not a fault:
// the entry goes back in the queue and the loop collects the frame.
func (e *engine) txPhase(ctx context.Context, dev radio.Device) error {
	e.drainAdvertAsk(dev, time.Now())
	e.dueAdverts(dev, time.Now())
	entry, ok := e.queue.pop(time.Now())
	if !ok {
		return nil
	}
	if !e.admitDuty(dev, entry) {
		return nil
	}
	log := e.log.With(zap.String("txn", entry.origin.Short()), zap.String("kind", entry.kind))
	outcome, err := e.clearChannel(ctx, dev, log, entry.origin)
	switch {
	case err != nil:
		e.dropOnFault(ctx, entry.origin, err)
		return err
	case outcome == lbtRequeue:
		e.requeue(entry)
		return nil
	case outcome == lbtDrop:
		return nil // clearChannel published the refusal
	}
	raw, err := entry.pkt.MarshalBinary()
	if err != nil {
		log.Warn("tx marshal failed", zap.Error(err))
		e.bus.Publish(bus.TxDropped{
			Relay: e.relay, Txn: entry.origin, At: time.Now(), Reason: "malformed",
		})
		return nil
	}
	sent := bus.FrameSent{
		Relay: e.relay, Txn: entry.origin, Kind: entry.kind,
		PowerDBm: e.policy.PowerDBm, Shadow: e.paperOnly(entry.pkt),
	}
	if sent.Shadow {
		sent.At, sent.Airtime = time.Now(), dev.Airtime(len(raw))
	} else if requeued, err := e.keyAndFill(ctx, dev, raw, entry, &sent, log); requeued || err != nil {
		if err != nil && sent.Airtime == 0 {
			// Nothing was radiated and nothing will be: the entry was
			// already popped, so the session restart's queue purge
			// cannot count it — this is its only witness.
			e.dropOnFault(ctx, entry.origin, err)
		}
		return err
	}
	if !sent.Shadow {
		// The reference marks every emission seen as it sends it: a
		// flood we originate comes back from our neighbours, and
		// without this we would judge our own words a stranger's and
		// flood them again.
		e.seen.witness(entry.pkt.Hash(), entry.origin, time.Now())
	}
	e.duty.record(sent.At, sent.Airtime)
	if !sent.Shadow {
		// The radio's own tally: paper never counts here.
		e.stats.countSent(entry.pkt.IsRouteFlood(), sent.Airtime)
	}
	log.Info("frame sent", zap.Bool("shadow", sent.Shadow),
		zap.Duration("airtime", sent.Airtime), zap.Int8("power_dbm", sent.PowerDBm))
	e.bus.Publish(sent)
	return nil
}

// keyAndFill keys the radio and fills the emission's own account of
// itself. requeued reports that the entry went back in the queue —
// the caller is done either way.
//
// An error alongside a real airtime means the frame reached the air
// and the trouble came after: charge and journal it before the
// session goes down, or the ledger loses airtime the regulator would
// still count.
func (e *engine) keyAndFill(ctx context.Context, dev radio.Device, raw []byte,
	entry txEntry, sent *bus.FrameSent, log *zap.Logger,
) (requeued bool, err error) {
	report, err := e.key(ctx, dev, raw)
	if errors.Is(err, radio.ErrBusyReceiving) {
		e.requeue(entry) // a frame landed between assessment and keying
		return true, nil
	}
	sent.At, sent.Airtime, sent.PowerDBm = report.At, report.Airtime, report.PowerDBm
	if err == nil {
		return false, nil
	}
	if report.Airtime > 0 {
		e.duty.record(report.At, report.Airtime)
		e.bus.Publish(*sent)
		log.Warn("frame sent, then the radio faulted",
			zap.Duration("airtime", report.Airtime), zap.Error(err))
	}
	return false, err
}

// key transmits under a deadline of its own. Once the radio is
// committed, a cancelled daemon must not cut the frame short: a
// truncated emission is garbage on the air and its airtime never
// reaches the ledger. The detached deadline is generous enough for
// the frame plus the chip's own timeout, and no longer.
func (e *engine) key(ctx context.Context, dev radio.Device, raw []byte) (radio.TxReport, error) {
	budget := 2*dev.Airtime(len(raw)) + time.Second
	txCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), budget)
	defer cancel()
	return dev.Transmit(txCtx, raw, e.policy.PowerDBm)
}

// requeue puts an entry back for the next pass; a queue that filled
// meanwhile refuses it, counted like any other refusal.
func (e *engine) requeue(entry txEntry) {
	entry.notBefore = time.Now()
	if !e.queue.push(entry) {
		e.bus.Publish(bus.TxDropped{
			Relay: e.relay, Txn: entry.origin, At: time.Now(), Reason: "queue-full",
		})
	}
}

// clearChannel is the LBT wait: bounded retries while the channel is
// busy, then the site's exhausted policy — transmit anyway (the
// mesh's convention) or a counted drop. A refusal because the radio
// is receiving ends the wait early: the frame in hand comes first.
func (e *engine) clearChannel(ctx context.Context, dev radio.Device, log *zap.Logger,
	origin txn.ID,
) (lbtOutcome, error) {
	deadline := time.Now().Add(lbtMaxWait)
	for {
		busy, err := dev.AssessChannel(ctx, e.policy.LBTThresholdDB)
		switch {
		case errors.Is(err, radio.ErrBusyReceiving):
			return lbtRequeue, nil
		case err != nil:
			return lbtDrop, err
		case !busy:
			return lbtGo, nil
		}
		if time.Now().After(deadline) {
			if e.policy.LBTExhausted == "drop" {
				log.Warn("channel busy past the LBT bound, dropping")
				e.bus.Publish(bus.TxDropped{
					Relay: e.relay, Txn: origin, At: time.Now(), Reason: "lbt",
				})
				return lbtDrop, nil
			}
			log.Warn("channel busy past the LBT bound, transmitting anyway")
			return lbtGo, nil
		}
		retry := lbtRetryNominal/2 + rand.N(lbtRetryNominal) //nolint:gosec // backoff jitter, not security
		select {
		case <-ctx.Done():
			return lbtDrop, ctx.Err()
		case <-time.After(retry):
		}
	}
}

// dropQueued empties the queue, counting what it refuses. A radio
// session that ended took its moment with it: the frames it was about
// to relay are stale news by the time the backoff expires, and the
// mesh has long since carried them past us.
func (e *engine) dropQueued(reason string) {
	for _, entry := range e.queue.entries {
		e.bus.Publish(bus.TxDropped{
			Relay: e.relay, Txn: entry.origin, At: time.Now(), Reason: reason,
		})
	}
	if n := len(e.queue.entries); n > 0 {
		e.log.Info("outbound queue cleared", zap.Int("dropped", n), zap.String("reason", reason))
	}
	e.queue.entries = e.queue.entries[:0]
}

// dutyMaxWait bounds how long an emission may wait for the duty budget
// to free before it is dropped: past this, the frame is stale news.
const dutyMaxWait = 10 * time.Minute

// dutyStamp is one emission the sliding window remembers.
type dutyStamp struct {
	at  time.Time
	air time.Duration
}

// dutyLedger enforces the band's airtime budget over a sliding hour.
// Shadow emissions are recorded like real ones — the audit must show
// what on-air would have cost. The engine's goroutine writes it and
// operator sessions read it, so the window is held under a mutex:
// a mirror refreshed only on the write path would report a burst's
// usage long after the hour that held it had passed.
type dutyLedger struct {
	mu     sync.Mutex
	budget time.Duration // per sliding hour; zero = unbudgeted
	window []dutyStamp
}

// prune drops stamps older than the window; the caller holds mu.
func (d *dutyLedger) prune(now time.Time) time.Duration {
	cut := now.Add(-time.Hour)
	i := 0
	for i < len(d.window) && d.window[i].at.Before(cut) {
		i++
	}
	d.window = d.window[i:]
	var sum time.Duration
	for _, s := range d.window {
		sum += s.air
	}
	return sum
}

// usage is the sliding hour's spent airtime as of now, pruned first
// so an idle relay reports what it is actually using: nothing.
func (d *dutyLedger) usage(now time.Time) time.Duration {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.prune(now)
}

// admit answers whether an emission of the given airtime fits the
// budget now; when it does not, freeAt is the earliest instant enough
// window expires — and never reports an airtime no budget ever fits.
func (d *dutyLedger) admit(now time.Time, air time.Duration) (ok bool, freeAt time.Time, never bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.budget <= 0 {
		return true, time.Time{}, false
	}
	if air > d.budget {
		return false, time.Time{}, true
	}
	used := d.prune(now)
	if used+air <= d.budget {
		return true, time.Time{}, false
	}
	// Walk the oldest stamps: each expiry frees its airtime an hour
	// after it was spent.
	need := used + air - d.budget
	var freed time.Duration
	for _, s := range d.window {
		freed += s.air
		if freed >= need {
			return false, s.at.Add(time.Hour), false
		}
	}
	return false, now.Add(time.Hour), false
}

// record spends airtime from the budget.
func (d *dutyLedger) record(now time.Time, air time.Duration) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.window = append(d.window, dutyStamp{at: now, air: air})
	d.prune(now)
}

// Duty reports the sliding-hour airtime spent and the budget; ok is
// false when the pipeline is off or the band unbudgeted. Any
// goroutine.
func (e *engine) Duty() (used, budget time.Duration, ok bool) {
	if !e.txEnabled() || e.duty == nil || e.duty.budget <= 0 {
		return 0, 0, false
	}
	return e.duty.usage(time.Now()), e.duty.budget, true
}

// admitDuty applies the budget to one popped entry: requeued for the
// budget's freeing when that is near, dropped when it is not.
func (e *engine) admitDuty(dev radio.Device, entry txEntry) bool {
	if e.duty.budget <= 0 {
		return true
	}
	raw := entry.pkt.RawLength()
	ok, freeAt, never := e.duty.admit(time.Now(), dev.Airtime(raw))
	if ok {
		return true
	}
	if never || time.Until(freeAt) > dutyMaxWait {
		e.log.Warn("duty budget refuses the emission, dropping",
			zap.String("kind", entry.kind), zap.String("txn", entry.origin.Short()))
		e.bus.Publish(bus.TxDropped{
			Relay: e.relay, Txn: entry.origin, At: time.Now(), Reason: "duty",
		})
		return false
	}
	entry.notBefore = freeAt
	if !e.queue.push(entry) {
		e.bus.Publish(bus.TxDropped{
			Relay: e.relay, Txn: entry.origin, At: time.Now(), Reason: "duty",
		})
	}
	return false
}
