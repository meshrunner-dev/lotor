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
	"meshrunner.dev/lotor/internal/logging"
	"meshrunner.dev/lotor/internal/protocol"
	"meshrunner.dev/lotor/internal/radio"
	"meshrunner.dev/lotor/internal/txn"
)

// The transmit pipeline's fixed policy, matching the reference
// repeater's active defaults.
const (
	// Retransmission jitter desynchronises repeaters that heard the
	// same frame: a random delay in [0, 5×airtime×factor), the
	// reference's mechanism, and these its shipped factors — what
	// tx_delay_factor and direct_tx_delay_factor override.
	defaultTxDelayFactor       = 0.5
	defaultDirectTxDelayFactor = 0.3

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

// smallestDutyCyclePct is the least percentage the sliding-hour
// ledger can still hold as one nanosecond of airtime. Below it a
// positive ceiling would round away entirely, and the ledger reads a
// zero budget as no ceiling at all.
const smallestDutyCyclePct = 100.0 / float64(time.Hour)

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
// A dry run never arms the pipeline, so the queue may not exist at
// all: nothing queued and nothing to queue answer the same way.
func (q *txQueue) nextDue(now time.Time) (time.Duration, bool) {
	if q == nil {
		return 0, false
	}
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
	if !validDutyCyclePct(e.p.DutyCyclePct) || e.p.DutyCyclePct <= 0 {
		return fmt.Errorf(
			"tx: mode %s needs duty_cycle_pct in (0, 100] on this relay's band — "+
				"set the lawful ceiling, or 100 to state the band has none", p.Mode)
	}
	// The ledger reads a budget of zero as "unbudgeted", so a
	// percentage that rounds to zero nanoseconds would turn the most
	// restrictive ceiling an operator can write into no ceiling at
	// all. A budget smaller than any frame is honest — every emission
	// is then refused as one no budget ever fits — but a budget that
	// disappears is not.
	budget := time.Duration(float64(time.Hour) * e.p.DutyCyclePct / 100)
	if budget <= 0 {
		return fmt.Errorf(
			"tx: duty_cycle_pct %g is positive but rounds to no airtime at all — "+
				"the smallest budget this ledger can hold is %g%%",
			e.p.DutyCyclePct, smallestDutyCyclePct)
	}
	e.policy = p
	e.queue = &txQueue{depth: p.QueueDepth}
	// The sliding hour did not restart with the process: what the
	// journal remembers being spent is spent, or a crash-loop could
	// launder the budget.
	dl := &dutyLedger{budget: budget}
	// The window prunes as a time-ordered prefix: feed it in order,
	// whatever order the journal rows arrived in.
	spent := slices.Clone(p.Spent)
	slices.SortFunc(spent, func(a, b protocol.Spent) int { return a.At.Compare(b.At) })
	for _, sp := range spent {
		dl.record(sp.At, sp.Airtime)
	}
	e.duty = dl
	e.started = time.Now()
	e.advertAsk = make(chan *advertOrder, 1)
	e.scopeAsk = make(chan *scopeQuery, 1)
	e.sweepAsk = make(chan *sweep, 1)
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
// refuses. jitterFactor scales the desynchronisation delay. It reports
// whether the entry was taken, for the callers that owe somebody an
// answer about it.
func (e *engine) enqueue(dev radio.Device, pkt *meshcore.Packet, kind string,
	origin txn.ID, priority int, jitterFactor float64,
) bool {
	delay := time.Duration(0)
	if jitterFactor > 0 {
		air := dev.Airtime(pkt.RawLength())
		span := max(time.Duration(5*jitterFactor*float64(air)), time.Millisecond)
		delay = rand.N(span) //nolint:gosec // desync jitter, not security
	}
	return e.enqueueAfter(pkt, kind, origin, priority, delay)
}

// enqueueAfter schedules one emission a fixed delay out, publishing
// the drop when the queue refuses.
func (e *engine) enqueueAfter(pkt *meshcore.Packet, kind string, origin txn.ID,
	priority int, delay time.Duration,
) bool {
	entry := txEntry{
		pkt: pkt, kind: kind, origin: origin,
		priority: priority, notBefore: time.Now().Add(delay),
	}
	if !e.queue.push(entry) {
		e.log.Warn("tx queue full, dropping", zap.String("kind", kind),
			zap.String("txn", origin.Short()))
		e.bus.Publish(bus.TxDropped{
			Relay: e.relay, Txn: origin, At: time.Now(), Reason: "queue-full", Kind: kind,
		})
		return false
	}
	return true
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
			e.abandonKind(origin, "malformed", "relay-flood", "flood relay path append failed", err)
			return
		}
		// Priority = distance: the hop count with our hash appended.
		e.enqueue(dev, &cp, "relay-flood", origin, cp.PathHashCount(), e.p.txDelayFactor())
	case verdictRelayDirect:
		if cp.PayloadType() == meshcore.PayloadTypeMultipart {
			e.forwardMultipart(&cp, origin)
			return
		}
		if _, err := cp.ConsumeNextHop(); err != nil {
			e.abandonKind(origin, "malformed", "relay-direct", "direct relay hop consume failed", err)
			return
		}
		// The reference forwards ACKs with no delay at all: they are
		// the confirmation a sender is timing out on, and every hop's
		// jitter is latency it pays (Mesh::routeDirectRecvAcks).
		jitter := e.p.directTxDelayFactor()
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
	case verdictCommand:
		e.runCommand(rx, origin)
	case verdictRelayTrace:
		// A trace whose next target hop is us walks on: our SNR
		// reading — quarter-dB, one raw byte — joins the walked path
		// (Mesh::onRecvPacket).
		if err := cp.AppendTraceHop(snr); err != nil {
			e.abandonKind(origin, "malformed", "relay-trace", "trace path could not grow", err)
			return
		}
		e.enqueue(dev, &cp, "relay-trace", origin, prioTrace, e.p.directTxDelayFactor())
	}
}

// abandonKind reports an emission the pipeline gave up on before it
// ever reached the queue, with the kind its caller was composing: the
// audit trail must show the refusal, not a judgement that quietly led
// nowhere.
func (e *engine) abandonKind(origin txn.ID, reason, kind, msg string, err error) {
	e.log.Warn(msg, zap.String("txn", origin.Short()), zap.Error(err))
	e.bus.Publish(bus.TxDropped{
		Relay: e.relay, Txn: origin, At: time.Now(), Reason: reason, Kind: kind,
	})
}

// dropOnFault counts an emission lost to a radio fault. A daemon
// shutting down is not a fault: that entry goes unsent like the rest
// of the queue, and counting it would blame the radio for the stop.
func (e *engine) dropOnFault(ctx context.Context, origin txn.ID, kind string, err error) {
	if ctx.Err() != nil {
		return
	}
	e.abandonKind(origin, "tx-failed", kind, "emission lost to a radio fault", err)
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
	mp, stripped, err := cp.UnwrapMultipart()
	if err != nil || mp.Inner != meshcore.PayloadTypeAck || len(mp.Data) < 4 {
		e.abandonKind(origin, "malformed", "relay-direct", "multipart wraps no ack", err)
		return
	}
	// Dedup on the unwrapped shape, exactly as the reference hashes it:
	// the multipart header over the stripped payload.
	if _, dup := e.seen.witness(stripped.Hash(), origin, time.Now()); dup {
		e.bus.Publish(bus.TxDropped{
			Relay: e.relay, Txn: origin, At: time.Now(), Reason: "duplicate", Kind: "relay-direct",
		})
		return
	}
	if _, err := stripped.ConsumeNextHop(); err != nil {
		e.abandonKind(origin, "malformed", "relay-direct", "multipart hop consume failed", err)
		return
	}
	ack, err := meshcore.BuildAck(mp.Data[:4])
	if err != nil {
		e.abandonKind(origin, "malformed", "relay-direct", "multipart ack rebuild failed", err)
		return
	}
	// The unwrapped ACK travels the way its multipart did, scope
	// included: dropping the scope mid-relay would strand the answer
	// outside the mesh that asked for it.
	ack.InheritRouting(stripped)
	e.enqueueAfter(ack, "relay-ack", origin, prioDirect,
		time.Duration(mp.Remaining+1)*300*time.Millisecond)
}

// selfHash is this node's path identity at the packet's hash size.
func (e *engine) selfHash(size int) []byte { return e.id.PubKey[:size] }

// txWait reports how long Receive may block before the pipeline needs
// the radio: the earliest of the queue's schedule, the advert clocks
// and a held flood's release. ok is false when nothing is ever due.
func (e *engine) txWait(now time.Time) (time.Duration, bool) {
	var waits []time.Duration
	if d, ok := e.queue.nextDue(now); ok {
		waits = append(waits, d)
	}
	if d, ok := e.heldWait(now); ok {
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
	// The clocks wind only once the announcement is queued. Winding
	// first cost a refused emission its whole interval — up to
	// forty-seven hours of silence for one full queue.
	switch {
	case e.floodAdvertDue(now):
		if !e.advert(dev, now, "advert-flood", false) {
			return // the clock still stands: the next turn tries again
		}
		e.nextFloodAdvert = now.Add(e.p.AdvertFloodInterval)
		// Both adverts in one pass would carry the same second in
		// their timestamp, hence the same packet hash: a neighbour
		// hearing the zero-hop copy first would judge the routable one
		// a duplicate and never re-flood it. The reference winds both
		// clocks for the same reason (MyMesh: "so they don't overlap").
		if !e.nextLocalAdvert.IsZero() {
			e.windLocalAdvert(now)
		}
	case !e.nextLocalAdvert.IsZero() && !now.Before(e.nextLocalAdvert):
		// The same arbitration on the scheduled side: a flood due
		// inside this second wins it, and the local one waits for the
		// next — the two must never share a signature.
		if !e.advert(dev, now, "advert-local", true) {
			return
		}
		e.windLocalAdvert(now)
	}
}

// RequestAdvert queues one operator-triggered announcement: zero-hop
// by default, routable when flood. Safe from any goroutine.
//
// The guard against ordering them faster than anyone means to lives
// here rather than in the console, because the console is not the only
// caller: a web button, and later a question arriving over the air,
// come through this same door, and a rule written at one of those
// doors is a rule the others never learn.
func (e *engine) RequestAdvert(flood bool) error {
	if !e.txEnabled() {
		return errors.New("the transmit gate is dry — nothing may be sent")
	}
	if time.Now().Before(clockEpoch) {
		return errors.New("wall clock implausible — neighbours keep the newest advert timestamp, " +
			"and this one would poison ours")
	}
	o := &advertOrder{kind: "advert-local", started: newAck()}
	if flood {
		o.kind = "advert-flood"
	}
	select {
	case e.advertAsk <- o:
	default:
		return errors.New("an advert is already pending")
	}
	e.wakeReceiver()
	return o.started.wait("advert")
}

// advertOrder is one announcement an operator asked for, and the
// answer waiting to hear whether it went out.
type advertOrder struct {
	kind    string
	started *ack
}

// advertAskGap is the least time between two ordered announcements.
// Short enough that no deliberate act is blocked — a flood advert
// costs well under a second of air, and the duty ledger is what
// actually bounds the spending — and long enough that a stuck key or
// a double-clicked button does not become a burst the neighbourhood
// has to dedup its way out of.
const advertAskGap = 10 * time.Second

// advertAskDue refuses an order that crowds the last one, saying how
// long is left rather than only that the answer is no. It runs on the
// pipeline's goroutine, beside the scheduled adverts' clocks, so the
// spacing needs no lock to be right — and it only reads: the gap is
// spent when the announcement is actually queued, never on an order
// the queue then refuses.
//
// Those scheduled announcements do not come through here. They keep
// their configured cadence, and an operator's order is not evidence
// about when the next one is due.
func (e *engine) advertAskDue(now time.Time) error {
	if wait := advertAskGap - now.Sub(e.lastAskedAdvert); wait > 0 && !e.lastAskedAdvert.IsZero() {
		return fmt.Errorf("an advert was ordered %s ago — %s to wait",
			now.Sub(e.lastAskedAdvert).Round(time.Second), wait.Round(time.Second))
	}
	return nil
}

// drainAdvertAsk serves an operator-triggered announcement. Nothing is
// spent — not the gap, not the clocks, not a place in the duplicate
// table — until the announcement is actually in the queue: an order
// answered "sent" whose packet was dropped for a full queue would
// have wound the flood clock hours forward for an emission nobody
// ever heard.
//
// The clocks then wind as if the scheduler itself had fired: the mesh
// just heard from us, and a scheduled advert moments later would be a
// byte-identical duplicate a neighbour would dedup away.
func (e *engine) drainAdvertAsk(dev radio.Device, now time.Time) {
	select {
	case o := <-e.advertAsk:
		if !o.started.claim() {
			return // the operator gave up; this must not go out now
		}
		if err := e.advertAskDue(now); err != nil {
			o.started.refused(err)
			return
		}
		// A due flood outranks an ordered local one: both would carry
		// this second in the same signed payload, hash alike, and the
		// neighbour hearing the zero-hop copy first would judge the
		// routable one a duplicate and never re-flood it. The flood is
		// the one the mesh cannot get again for hours.
		if o.kind == "advert-local" && e.floodAdvertDue(now) {
			o.started.refused(errors.New(
				"a flooded advert is due this second — it carries the same announcement further"))
			return
		}
		local := o.kind != "advert-flood"
		if !e.advert(dev, now, o.kind, local) {
			o.started.refused(errors.New("the outbound queue is full — the advert never left"))
			return
		}
		e.lastAskedAdvert = now
		if !local && e.p.AdvertFloodInterval > 0 {
			e.nextFloodAdvert = now.Add(e.p.AdvertFloodInterval)
		}
		e.windLocalAdvert(now)
		o.started.taken()
	default:
	}
}

// floodAdvertDue reports that the routable announcement's own clock
// has struck, or will strike inside the very second this one would be
// signed in.
//
// The wire counts in seconds — BuildAdvert signs now.Unix() — while
// the clocks are nanosecond instants, and comparing the instants let
// a flood five hundred milliseconds away count as "not yet". It would
// then carry the identical timestamp, hence the identical packet
// hash, as the local advert going out now: the neighbour hears the
// zero-hop copy first and judges the routable one a duplicate it need
// not relay. The arbitration has to happen in the units the signature
// actually uses.
func (e *engine) floodAdvertDue(now time.Time) bool {
	return !e.nextFloodAdvert.IsZero() && e.nextFloodAdvert.Unix() <= now.Unix()
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

// advert builds this node's signed announcement and reports whether
// it reached the queue. Local adverts are zero-hop: direct route,
// empty path — the form the band rules allow first. Our own hash is
// witnessed so the echo judges as a duplicate, and only once the
// announcement is really scheduled: a witness for a frame that was
// dropped would silence the next honest copy of it.
func (e *engine) advert(dev radio.Device, now time.Time, kind string, local bool) bool {
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
		return false
	}
	if local {
		pkt.Header = meshcore.MakeHeader(meshcore.RouteDirect,
			meshcore.PayloadTypeAdvert, meshcore.PayloadVer1)
	} else {
		// Our own flood declares the hash width every relayer will
		// append at — path_hash_mode, the origination half of the
		// width story (relays mirror whatever arrives).
		pkt.SetPathHashSizeAndCount(e.p.pathHashWidth(), 0)
		// A routable announcement travels in the scope this relay
		// speaks, so the mesh that carries it is the one that agreed
		// to. The zero-hop one stays plain, as the reference's own
		// zero-hop send does: whoever hears it directly hears it
		// whatever they carry.
		e.regions.speak.Scope(pkt)
	}
	id := txn.New()
	prio := prioFloodAdvert
	if local {
		prio = prioDirect // zero-hop sends go out at the front
	}
	if !e.enqueue(dev, pkt, kind, id, prio, 0) {
		return false
	}
	e.seen.witness(pkt.Hash(), id, now)
	return true
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
	e.drainScopeAsk(dev, time.Now())
	e.drainSweepAsk(dev, time.Now())
	e.dueAdverts(dev, time.Now())
	entry, ok := e.queue.pop(time.Now())
	if !ok {
		return nil
	}
	if !e.admitDuty(dev, entry) {
		return nil
	}
	log := e.log.With(zap.String("txn", entry.origin.Short()), zap.String("kind", entry.kind))
	outcome, err := e.clearChannel(ctx, dev, log, entry.origin, entry.kind)
	switch {
	case err != nil:
		e.dropOnFault(ctx, entry.origin, entry.kind, err)
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
			Relay: e.relay, Txn: entry.origin, At: time.Now(), Reason: "malformed", Kind: entry.kind,
		})
		return nil
	}
	sent := bus.FrameSent{
		Relay: e.relay, Txn: entry.origin, Kind: entry.kind,
		PowerDBm: e.policy.PowerDBm, Shadow: e.paperOnly(entry.pkt),
		Raw: append([]byte(nil), raw...),
	}
	var faulted error
	if sent.Shadow {
		sent.At, sent.Airtime = time.Now(), dev.Airtime(len(raw))
	} else {
		requeued, radiated, err := e.keyAndFill(ctx, dev, raw, entry, &sent)
		switch {
		case requeued:
			return nil
		case !radiated:
			if err != nil {
				// Nothing was radiated and nothing will be: the entry
				// was already popped, so the session restart's queue
				// purge cannot count it — this is its only witness.
				e.dropOnFault(ctx, entry.origin, entry.kind, err)
			}
			return err
		}
		faulted = err
	}
	// One place for every consequence of a frame that occupied the
	// channel, whether the radio came back healthy afterwards or not.
	e.recordEmission(entry, sent, log, faulted)
	return faulted
}

// recordEmission is everything a frame that reached the air owes the
// rest of the daemon: the duplicate table, so our own words are not
// heard back as a stranger's and flooded again; the duty ledger the
// regulator counts; the traffic tally the status answers with; the
// log and the journal.
//
// It exists as one function because the accounting used to be split
// across two: a frame the chip radiated and then faulted on handback
// reached the ledger and the journal but not the duplicate table or
// the counters, so GET_STATUS and the heartbeat denied an emission
// the budget had already paid for.
func (e *engine) recordEmission(entry txEntry, sent bus.FrameSent, log *zap.Logger, faulted error) {
	if !sent.Shadow {
		e.seen.witness(entry.pkt.Hash(), entry.origin, time.Now())
	}
	e.duty.record(sent.At, sent.Airtime)
	if !sent.Shadow {
		// The radio's own tally: paper never counts here.
		e.stats.countSent(entry.pkt.IsRouteFlood(), sent.Airtime)
	}
	if faulted != nil {
		log.Warn("frame sent, then the radio faulted",
			zap.Duration("airtime", sent.Airtime), zap.Error(faulted))
	} else {
		log.Info("frame sent", zap.Bool("shadow", sent.Shadow),
			zap.Duration("airtime", sent.Airtime), zap.Int8("power_dbm", sent.PowerDBm))
	}
	e.bus.Publish(sent)
}

// keyAndFill keys the radio and fills the emission's own account of
// itself. requeued reports that the entry went back in the queue;
// radiated that the frame reached the air, which the caller must
// account for whatever the error beside it.
//
// An error alongside a real airtime means the frame was radiated and
// the trouble came after — a chip that signalled TxDone and then
// failed to go back to listening. As far as the channel and the
// regulator are concerned that emission happened.
func (e *engine) keyAndFill(ctx context.Context, dev radio.Device, raw []byte,
	entry txEntry, sent *bus.FrameSent,
) (requeued, radiated bool, err error) {
	report, err := e.key(ctx, dev, raw)
	if errors.Is(err, radio.ErrBusyReceiving) {
		e.requeue(entry) // a frame landed between assessment and keying
		return true, false, nil
	}
	sent.At, sent.Airtime, sent.PowerDBm = report.At, report.Airtime, report.PowerDBm
	return false, err == nil || report.Airtime > 0, err
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
	now := time.Now()
	if e.busySince.IsZero() {
		e.busySince = now
	}
	if now.Sub(e.busySince) > lbtMaxWait && e.policy.LBTExhausted == "drop" {
		// Only drop is honourable here: the radio refuses to key over
		// a reception whatever the policy says, so a site that asked
		// to transmit anyway waits for the air instead.
		e.log.Warn("radio busy receiving past the LBT bound, dropping",
			zap.String("txn", entry.origin.Short()), zap.String("kind", entry.kind))
		e.bus.Publish(bus.TxDropped{
			Relay: e.relay, Txn: entry.origin, At: now, Reason: "lbt", Kind: entry.kind,
		})
		return
	}
	// The channel is as occupied as a CAD-busy channel, so the retry
	// keeps the same pacing instead of spinning SPI polls against a
	// chip mid-demodulation. Our own reception completing wakes the
	// receive loop on its edge either way; only the retry is paced.
	entry.notBefore = now.Add(
		lbtRetryNominal/2 + rand.N(lbtRetryNominal)) //nolint:gosec // backoff jitter, not security
	if !e.queue.push(entry) {
		e.bus.Publish(bus.TxDropped{
			Relay: e.relay, Txn: entry.origin, At: now, Reason: "queue-full", Kind: entry.kind,
		})
	}
}

// clearChannel is the LBT wait: bounded retries while the channel is
// busy, then the site's exhausted policy — transmit anyway (the
// mesh's convention) or a counted drop. A refusal because the radio
// is receiving ends the wait early: the frame in hand comes first.
func (e *engine) clearChannel(ctx context.Context, dev radio.Device, log *zap.Logger,
	origin txn.ID, kind string,
) (lbtOutcome, error) {
	if !e.policy.CAD {
		// The reference's own default posture: key and let the mesh's
		// dedup sort out the collisions. The driver still refuses to
		// key over a reception actually in progress, which is a
		// hardware guard rather than a politeness.
		return lbtGo, nil
	}
	deadline := time.Now().Add(lbtMaxWait)
	for {
		busy, err := dev.AssessChannel(ctx, e.policy.LBTThresholdDB)
		switch {
		case errors.Is(err, radio.ErrBusyReceiving):
			return lbtRequeue, nil
		case err != nil:
			return lbtDrop, err
		case !busy:
			e.busySince = time.Time{} // the air is free; the spell ended
			return lbtGo, nil
		}
		if time.Now().After(deadline) {
			if e.policy.LBTExhausted == "drop" {
				log.Warn("channel busy past the LBT bound, dropping")
				e.bus.Publish(bus.TxDropped{
					Relay: e.relay, Txn: origin, At: time.Now(), Reason: "lbt", Kind: kind,
				})
				return lbtDrop, nil
			}
			log.Warn("channel busy past the LBT bound, transmitting anyway")
			return lbtGo, nil
		}
		retry := lbtRetryNominal/2 + rand.N(lbtRetryNominal) //nolint:gosec // backoff jitter, not security
		logging.Trace(log, "lbt channel busy — backing off",
			zap.Duration("retry_in", retry), zap.Duration("bound_left", time.Until(deadline)))
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
			Relay: e.relay, Txn: entry.origin, At: time.Now(), Reason: reason, Kind: entry.kind,
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
			Relay: e.relay, Txn: entry.origin, At: time.Now(), Reason: "duty", Kind: entry.kind,
		})
		return false
	}
	entry.notBefore = freeAt
	if !e.queue.push(entry) {
		e.bus.Publish(bus.TxDropped{
			Relay: e.relay, Txn: entry.origin, At: time.Now(), Reason: "duty", Kind: entry.kind,
		})
	}
	return false
}
