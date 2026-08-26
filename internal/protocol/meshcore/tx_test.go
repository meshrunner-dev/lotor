package meshcore

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"testing"
	"time"

	"go.uber.org/zap"

	"meshrunner.dev/pkg/meshcore"

	"meshrunner.dev/lotor/internal/bus"
	"meshrunner.dev/lotor/internal/protocol"
	"meshrunner.dev/lotor/internal/radio"
	"meshrunner.dev/lotor/internal/txn"
)

// fakeDevice scripts receptions and records transmissions. Frames
// arrive on a channel; an empty channel blocks like a quiet band.
type fakeDevice struct {
	frames chan radio.Frame
	sent   chan []byte
	busy   int // AssessChannel says busy this many times
	// pending makes AssessChannel refuse as "receiving" this many
	// times — the library's guard on a destructive operation.
	pending int
	// assessErr makes AssessChannel fail hard — a real radio fault.
	assessErr error
	// txEntered fires when a transmission starts, and txCtxErr carries
	// what its context said after the caller had a chance to cancel.
	txEntered chan struct{}
	txCtxErr  chan error
}

func newFakeDevice() *fakeDevice {
	return &fakeDevice{
		frames: make(chan radio.Frame, 8),
		sent:   make(chan []byte, 8),
	}
}

func (d *fakeDevice) Envelope() radio.Envelope             { return radio.Envelope{MaxTxPowerDBm: 5} }
func (d *fakeDevice) Configure(radio.Waveform) error       { return nil }
func (d *fakeDevice) StartReceive() error                  { return nil }
func (d *fakeDevice) NoiseFloor() (radio.NoiseFloor, bool) { return radio.NoiseFloor{}, false }
func (d *fakeDevice) NoiseStarved() uint64                 { return 0 }
func (d *fakeDevice) ChipStats() (radio.ChipStats, bool)   { return radio.ChipStats{}, false }
func (d *fakeDevice) Close() error                         { return nil }
func (d *fakeDevice) Airtime(int) time.Duration            { return time.Millisecond }

func (d *fakeDevice) Receive(ctx context.Context) (radio.Frame, error) {
	select {
	case <-ctx.Done():
		return radio.Frame{}, ctx.Err()
	case f := <-d.frames:
		return f, nil
	}
}

func (d *fakeDevice) AssessChannel(context.Context, float64) (bool, error) {
	if d.assessErr != nil {
		return false, d.assessErr
	}
	if d.pending > 0 {
		d.pending--
		return false, fmt.Errorf("%w: fake", radio.ErrBusyReceiving)
	}
	if d.busy > 0 {
		d.busy--
		return true, nil
	}
	return false, nil
}

func (d *fakeDevice) Transmit(ctx context.Context, payload []byte, powerDBm int8) (radio.TxReport, error) {
	if d.txEntered != nil {
		close(d.txEntered)
		time.Sleep(50 * time.Millisecond) // the caller cancels meanwhile
		d.txCtxErr <- ctx.Err()
	}
	d.sent <- payload
	return radio.TxReport{
		At: time.Now(), Airtime: time.Millisecond, PowerDBm: powerDBm,
	}, nil
}

// txRig builds an armed engine on a bus with a subscription, and a
// foreign identity whose adverts exercise the flood-relay path.
func txRig(t *testing.T, mode string) (*engine, *fakeDevice, *bus.Subscription, *meshcore.LocalIdentity) {
	t.Helper()
	seed := make([]byte, meshcore.SeedSize)
	seed[0] = 1
	self, err := meshcore.LocalIdentityFromSeed(seed)
	if err != nil {
		t.Fatal(err)
	}
	seed[0] = 2
	peer, err := meshcore.LocalIdentityFromSeed(seed)
	if err != nil {
		t.Fatal(err)
	}
	b := bus.New()
	sub := b.Subscribe(32)
	t.Cleanup(sub.Close)
	e := &engine{
		relay: "test-868",
		p:     params{NodeName: "test", DutyCyclePct: 100},
		id:    self,
		bus:   b,
		log:   zap.NewNop(),
		seen:  newSeenTable(0, referenceCapacity),
	}
	if err := e.Arm(protocol.TXPolicy{
		Mode: mode, LBTExhausted: "transmit", QueueDepth: 2, PowerDBm: -5,
	}); err != nil {
		t.Fatal(err)
	}
	return e, newFakeDevice(), sub, peer
}

// peerAdvert marshals a routable advert from the peer identity: a
// frame the engine judges would-relay-flood.
func peerAdvert(t *testing.T, peer *meshcore.LocalIdentity, at time.Time) radio.Frame {
	t.Helper()
	pkt, err := meshcore.BuildAdvert(peer, at, &meshcore.AdvertData{
		Type: meshcore.AdvTypeChat, Name: "peer",
	})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := pkt.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	return radio.Frame{Payload: raw, At: at, SNR: 8, RSSI: -80}
}

// drainSent collects bus events until a FrameSent arrives or the
// timeout runs out.
func awaitSent(t *testing.T, sub *bus.Subscription) bus.FrameSent {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		select {
		case ev := <-sub.C:
			if fs, ok := ev.(bus.FrameSent); ok {
				return fs
			}
		case <-deadline:
			t.Fatal("no FrameSent on the bus")
		}
	}
}

func runEngine(t *testing.T, e *engine, dev *fakeDevice) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = e.Run(ctx, dev) }()
	t.Cleanup(func() { cancel(); <-done })
}

func TestShadowRelaysOnPaperOnly(t *testing.T) {
	e, dev, sub, peer := txRig(t, "shadow")
	runEngine(t, e, dev)
	dev.frames <- peerAdvert(t, peer, time.Now())

	sent := awaitSent(t, sub)
	if !sent.Shadow || sent.Kind != "relay-flood" || sent.PowerDBm != -5 {
		t.Fatalf("sent = %+v, want a shadow relay-flood at -5 dBm", sent)
	}
	select {
	case raw := <-dev.sent:
		t.Fatalf("shadow keyed the radio: % X", raw)
	default:
	}
}

func TestOnAirRelayAppendsOurHash(t *testing.T) {
	e, dev, sub, peer := txRig(t, "on-air")
	runEngine(t, e, dev)
	dev.frames <- peerAdvert(t, peer, time.Now())

	sent := awaitSent(t, sub)
	if sent.Shadow || sent.Kind != "relay-flood" {
		t.Fatalf("sent = %+v, want a real relay-flood", sent)
	}
	raw := <-dev.sent
	pkt, err := meshcore.ParsePacket(raw)
	if err != nil {
		t.Fatalf("transmitted frame does not parse: %v", err)
	}
	if pkt.PathHashCount() != 1 || pkt.Path[0] != e.id.PubKey[0] {
		t.Fatalf("path = % X (count %d), want our hash appended", pkt.Path, pkt.PathHashCount())
	}
}

func TestLBTDropWhenSiteChoosesIt(t *testing.T) {
	e, dev, sub, peer := txRig(t, "on-air")
	e.policy.LBTExhausted = "drop"
	dev.busy = 1 << 20 // the channel never clears
	runEngine(t, e, dev)
	dev.frames <- peerAdvert(t, peer, time.Now())

	deadline := time.After(8 * time.Second)
	for {
		select {
		case ev := <-sub.C:
			if d, ok := ev.(bus.TxDropped); ok {
				if d.Reason != "lbt" {
					t.Fatalf("dropped for %q, want lbt", d.Reason)
				}
				return
			}
			if fs, ok := ev.(bus.FrameSent); ok {
				t.Fatalf("sent despite drop policy: %+v", fs)
			}
		case <-deadline:
			t.Fatal("no TxDropped on the bus")
		}
	}
}

func TestQueueRefusesTheOverflow(t *testing.T) {
	e, dev, sub, peer := txRig(t, "shadow")
	// No engine loop: enqueue directly, depth is 2.
	for range 3 {
		pkt, _ := meshcore.BuildAdvert(peer, time.Now(), &meshcore.AdvertData{Name: "p"})
		e.enqueue(dev, pkt, "relay-flood", txn.New(), prioFlood, 0)
	}
	deadline := time.After(time.Second)
	for {
		select {
		case ev := <-sub.C:
			if d, ok := ev.(bus.TxDropped); ok {
				if d.Reason != "queue-full" {
					t.Fatalf("dropped for %q, want queue-full", d.Reason)
				}
				return
			}
		case <-deadline:
			t.Fatal("no queue-full drop on the bus")
		}
	}
}

func TestLocalAdvertIsZeroHop(t *testing.T) {
	e, dev, sub, _ := txRig(t, "on-air")
	e.p.AdvertLocalInterval = 20 * time.Millisecond
	e.p.AdvertFloodInterval = time.Hour
	runEngine(t, e, dev)

	sent := awaitSent(t, sub)
	if sent.Kind != "advert-local" {
		t.Fatalf("sent = %+v, want advert-local", sent)
	}
	raw := <-dev.sent
	pkt, err := meshcore.ParsePacket(raw)
	if err != nil {
		t.Fatal(err)
	}
	if !pkt.IsRouteDirect() || pkt.PathHashCount() != 0 {
		t.Fatalf("local advert route %v path %d, want zero-hop direct", pkt.Route(), pkt.PathHashCount())
	}
	adv, err := meshcore.ParseAdvert(pkt.Payload)
	if err != nil || adv.Data.Name != "test" {
		t.Fatalf("advert = %+v, %v", adv, err)
	}
}

func TestDirectPriorityBeatsFlood(t *testing.T) {
	q := &txQueue{depth: 8}
	now := time.Now()
	q.push(txEntry{kind: "relay-flood", priority: prioFlood, notBefore: now.Add(-2 * time.Second)})
	q.push(txEntry{kind: "relay-direct", priority: prioDirect, notBefore: now.Add(-time.Second)})
	e, ok := q.pop(now)
	if !ok || e.kind != "relay-direct" {
		t.Fatalf("popped %v, want the direct entry first", e.kind)
	}
}

func TestDutyLedgerAdmitsAndFrees(t *testing.T) {
	d := &dutyLedger{budget: 10 * time.Millisecond}
	now := time.Now()
	d.record(now, 8*time.Millisecond)

	if ok, _, _ := d.admit(now, 2*time.Millisecond); !ok {
		t.Fatal("2ms should fit an 8/10 window")
	}
	ok, freeAt, never := d.admit(now, 5*time.Millisecond)
	if ok || never {
		t.Fatalf("5ms must wait, not pass (%v) nor die (%v)", ok, never)
	}
	if want := now.Add(time.Hour); !freeAt.Equal(want) {
		t.Fatalf("freeAt = %v, want the 8ms stamp's expiry %v", freeAt, want)
	}
	if ok, _, _ := d.admit(now.Add(time.Hour+time.Second), 5*time.Millisecond); !ok {
		t.Fatal("the window expired; 5ms should fit again")
	}
	if _, _, never := d.admit(now, 11*time.Millisecond); !never {
		t.Fatal("an airtime above the whole budget can never fit")
	}
	if used, _, _ := d.admit(now, time.Millisecond); !used {
		t.Fatal("unbudgeted check sanity")
	}
}

func TestDutyDropsWhatCannotWait(t *testing.T) {
	e, dev, sub, peer := txRig(t, "shadow")
	e.duty = &dutyLedger{budget: time.Microsecond} // any frame exceeds it
	runEngine(t, e, dev)
	dev.frames <- peerAdvert(t, peer, time.Now())

	deadline := time.After(2 * time.Second)
	for {
		select {
		case ev := <-sub.C:
			if d, ok := ev.(bus.TxDropped); ok {
				if d.Reason != "duty" {
					t.Fatalf("dropped for %q, want duty", d.Reason)
				}
				return
			}
			if fs, ok := ev.(bus.FrameSent); ok {
				t.Fatalf("sent past the budget: %+v", fs)
			}
		case <-deadline:
			t.Fatal("no duty drop on the bus")
		}
	}
}

// traceToUs builds a direct TRACE whose next target hop is this
// engine: tag, auth, flags(1-byte hashes), then our hash.
func traceToUs(t *testing.T, e *engine) radio.Frame {
	t.Helper()
	pkt, err := meshcore.BuildTrace(0x2A2A2A2A, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	pkt.Header = meshcore.MakeHeader(meshcore.RouteDirect,
		meshcore.PayloadTypeTrace, meshcore.PayloadVer1)
	pkt.Payload = append(pkt.Payload, e.id.PubKey[0])
	raw, err := pkt.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	return radio.Frame{Payload: raw, At: time.Now(), SNR: 8, RSSI: -80}
}

func TestTraceWalksOnWithOurSNR(t *testing.T) {
	// The reference retransmits a trace whose next target hop matches,
	// appending its SNR reading to the walked path.
	e, dev, sub, _ := txRig(t, "on-air")
	runEngine(t, e, dev)
	dev.frames <- traceToUs(t, e)

	sent := awaitSent(t, sub)
	if sent.Kind != "relay-trace" {
		t.Fatalf("sent = %+v, want relay-trace", sent)
	}
	raw := <-dev.sent
	pkt, err := meshcore.ParsePacket(raw)
	if err != nil {
		t.Fatalf("relayed trace does not parse: %v", err)
	}
	if len(pkt.Path) != 1 || pkt.Path[0] != byte(int8(8*4)) {
		t.Fatalf("walked path = % X, want one SNR byte of 32", pkt.Path)
	}
}

func TestFloodHopLimitsFollowTheReference(t *testing.T) {
	// The reference stops adverts at 8 hops and everything else at 64
	// (flood_max_advert, flood_max); past either, the packet is not
	// forwarded.
	e, _, _, peer := txRig(t, "shadow")
	// A path of hashes that can never be ours, so the loop scan stays out.
	fill := func(pkt *meshcore.Packet, hops int) {
		pkt.Path = make([]byte, hops)
		for i := range pkt.Path {
			pkt.Path[i] = ^e.id.PubKey[0]
		}
		pkt.SetPathHashSizeAndCount(1, hops)
	}
	advert := func(hops int) *meshcore.Packet {
		pkt, err := meshcore.BuildAdvert(peer, time.Now(), &meshcore.AdvertData{Name: "p"})
		if err != nil {
			t.Fatal(err)
		}
		fill(pkt, hops)
		return pkt
	}
	if v, _ := e.floodVerdict(advert(7), true); v != verdictRelayFlood {
		t.Errorf("advert at 7 hops = %q, want a relay", v)
	}
	if v, why := e.floodVerdict(advert(8), true); v != verdictDropFloodHops {
		t.Errorf("advert at 8 hops = %q (%s), want the advert limit to stop it", v, why)
	}

	txt, err := meshcore.BuildDatagram(meshcore.PayloadTypeTxtMsg,
		[]byte{0x01}, []byte{0x02}, make([]byte, 32), []byte("hi"))
	if err != nil {
		t.Fatal(err)
	}
	fill(txt, 8)
	if v, _ := e.floodVerdict(txt, false); v != verdictRelayFlood {
		t.Errorf("text at 8 hops = %q, want a relay — only adverts stop that early", v)
	}
	// The reference's 64-hop ceiling is a belt: the 6-bit path count
	// saturates at 63 and the capacity check binds first, so it can
	// never fire. A site that lowers the limit is what actually bites.
	e.p.FloodMaxHops = 10
	fill(txt, 10)
	if v, _ := e.floodVerdict(txt, false); v != verdictDropFloodHops {
		t.Errorf("text at 10 hops = %q under a limit of 10, want it stopped", v)
	}
	fill(txt, 9)
	if v, _ := e.floodVerdict(txt, false); v != verdictRelayFlood {
		t.Errorf("text at 9 hops = %q under a limit of 10, want a relay", v)
	}
}

func TestAdvertSeedingFollowsTheReference(t *testing.T) {
	// A reboot is news to the direct neighbourhood, never to the mesh:
	// the boot announcement is zero-hop and prompt, the first flood
	// waits its whole interval — and the boot advert goes out even
	// when recurring local adverts are disabled.
	now := time.Now()
	e := &engine{p: params{
		AdvertFloodInterval: 47 * time.Hour,
		AdvertLocalInterval: -1, // recurring local adverts off
	}}
	e.scheduleAdverts(now)
	if want := now.Add(47 * time.Hour); !e.nextFloodAdvert.Equal(want) {
		t.Errorf("first flood at %v, want the full interval %v", e.nextFloodAdvert, want)
	}
	if want := now.Add(bootAdvertDelay); !e.nextLocalAdvert.Equal(want) {
		t.Errorf("boot advert at %v, want %v even with local adverts off", e.nextLocalAdvert, want)
	}
	// After the boot announcement, a disabled local clock stops.
	e.windLocalAdvert(now.Add(bootAdvertDelay))
	if !e.nextLocalAdvert.IsZero() {
		t.Error("the one-shot boot advert re-armed a disabled clock")
	}
}

func TestAdvertIntervalsKeepTheReferenceRanges(t *testing.T) {
	// The reference CLI refuses local outside 60..240 minutes and
	// flood outside 3..168 hours; so does the config.
	withBase := func(m map[string]any) map[string]any {
		cfg := map[string]any{"frequency_hz": 869_618_000}
		maps.Copy(cfg, m)
		return cfg
	}
	for _, bad := range []map[string]any{
		{"advert_local_interval": "30m"},
		{"advert_local_interval": "5h"},
		{"advert_flood_interval": "1h"},
		{"advert_flood_interval": "200h"},
	} {
		if _, err := paramsFrom(withBase(bad)); err == nil {
			t.Errorf("%v accepted — outside the reference's range", bad)
		}
	}
	for _, good := range []map[string]any{
		{"advert_local_interval": "2h", "advert_flood_interval": "47h"},
		{"advert_local_interval": "-1s", "advert_flood_interval": "-1s"},
	} {
		if _, err := paramsFrom(withBase(good)); err != nil {
			t.Errorf("%v refused: %v", good, err)
		}
	}
}

func TestAdvertClocksAreSeededOnce(t *testing.T) {
	// The engine outlives a radio session: a bouncing radio must not
	// re-seed the clocks, or a 48 h advert becomes one per session.
	// The clocks are pure engine state: no radio, no bus needed.
	e := &engine{p: params{
		AdvertFloodInterval: 48 * time.Hour,
		AdvertLocalInterval: time.Hour,
	}}

	start := time.Now()
	e.scheduleAdverts(start)
	flood, local := e.nextFloodAdvert, e.nextLocalAdvert
	if flood.IsZero() || local.IsZero() {
		t.Fatal("first scheduling left a clock unset")
	}
	e.scheduleAdverts(start.Add(10 * time.Minute)) // a session restart
	if !e.nextFloodAdvert.Equal(flood) || !e.nextLocalAdvert.Equal(local) {
		t.Fatalf("a session restart moved the clocks: flood %v→%v, local %v→%v",
			flood, e.nextFloodAdvert, local, e.nextLocalAdvert)
	}
}

func TestReceptionPendingRequeuesInsteadOfKillingTheSession(t *testing.T) {
	// A radio busy receiving is the channel being busy, not a fault:
	// the entry waits its turn and the session lives on.
	e, dev, sub, peer := txRig(t, "on-air")
	dev.pending = 2
	runEngine(t, e, dev)
	dev.frames <- peerAdvert(t, peer, time.Now())

	sent := awaitSent(t, sub)
	if sent.Kind != "relay-flood" {
		t.Fatalf("sent = %+v, want the relay to survive the refusals", sent)
	}
	if dev.pending != 0 {
		t.Errorf("%d refusals unused — the pipeline gave up early", dev.pending)
	}
}

func TestSessionRestartClearsTheQueue(t *testing.T) {
	// The frames a dead session was about to relay are stale news by
	// the time the backoff expires.
	e, dev, sub, peer := txRig(t, "shadow")
	for range 2 {
		pkt, err := meshcore.BuildAdvert(peer, time.Now(), &meshcore.AdvertData{Name: "p"})
		if err != nil {
			t.Fatal(err)
		}
		e.queue.push(txEntry{pkt: pkt, kind: "relay-flood", origin: txn.New(),
			priority: prioFlood, notBefore: time.Now().Add(time.Hour)})
	}
	runEngine(t, e, dev) // Run clears what the previous session left

	dropped := 0
	deadline := time.After(2 * time.Second)
	for dropped < 2 {
		select {
		case ev := <-sub.C:
			if d, ok := ev.(bus.TxDropped); ok {
				if d.Reason != "session-restart" {
					t.Fatalf("dropped for %q, want session-restart", d.Reason)
				}
				dropped++
			}
		case <-deadline:
			t.Fatalf("only %d of 2 stale entries cleared", dropped)
		}
	}
}

func TestKeyingSurvivesShutdown(t *testing.T) {
	// Once the radio is committed, a cancelled daemon must not cut the
	// frame short: the emission runs on its own deadline.
	e, dev, sub, peer := txRig(t, "on-air")
	dev.txEntered = make(chan struct{})
	dev.txCtxErr = make(chan error, 1)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = e.Run(ctx, dev) }()
	t.Cleanup(func() { cancel(); <-done })

	dev.frames <- peerAdvert(t, peer, time.Now())
	select {
	case <-dev.txEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("the transmission never started")
	}
	cancel() // SIGTERM lands mid-frame

	select {
	case err := <-dev.txCtxErr:
		if err != nil {
			t.Fatalf("the keyed frame was cut short: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no transmit context verdict")
	}
	<-dev.sent
	_ = sub
}

func TestArmRefusesAnUnbudgetedBand(t *testing.T) {
	// A gate that cannot account for its airtime does not open: the
	// operator states the band's ceiling, or states it has none.
	e := &engine{id: &meshcore.LocalIdentity{}, p: params{NodeName: "test"}}
	if err := e.Arm(protocol.TXPolicy{Mode: "shadow", QueueDepth: 2}); err == nil {
		t.Fatal("shadow armed with no duty budget")
	}
	e.p.DutyCyclePct = 100
	if err := e.Arm(protocol.TXPolicy{Mode: "on-air", QueueDepth: 2}); err != nil {
		t.Fatalf("a band declaring no limit should arm: %v", err)
	}
}

func TestSaturatedPathIsJudgedFull(t *testing.T) {
	// 63 one-byte hashes fill the count field: appending ours is
	// impossible, so the judgement must say so rather than promise a
	// relay the transform would refuse.
	e, _, _, peer := txRig(t, "shadow")
	pkt, err := meshcore.BuildAdvert(peer, time.Now(), &meshcore.AdvertData{Name: "p"})
	if err != nil {
		t.Fatal(err)
	}
	pkt.Path = make([]byte, maxPathHashes)
	pkt.SetPathHashSizeAndCount(1, maxPathHashes)
	if v, _ := e.floodVerdict(pkt, true); v != verdictDropPathFull {
		t.Fatalf("verdict = %q, want %q", v, verdictDropPathFull)
	}
}

func TestAbandonedRelayIsCounted(t *testing.T) {
	// A transform that refuses still owes the audit trail a refusal.
	e, dev, sub, peer := txRig(t, "shadow")
	pkt, err := meshcore.BuildAdvert(peer, time.Now(), &meshcore.AdvertData{Name: "p"})
	if err != nil {
		t.Fatal(err)
	}
	pkt.Path = make([]byte, maxPathHashes)
	pkt.SetPathHashSizeAndCount(1, maxPathHashes)
	e.relayFor(dev, pkt, verdictRelayFlood, txn.New(), 8)

	select {
	case ev := <-sub.C:
		d, ok := ev.(bus.TxDropped)
		if !ok || d.Reason != "malformed" {
			t.Fatalf("event = %+v, want a malformed drop", ev)
		}
		if d.Txn.Short() == (txn.ID{}).Short() {
			t.Error("the drop does not name its reception")
		}
	default:
		t.Fatal("the refusal was silent")
	}
}

func TestAcksAreRelayedWithoutJitter(t *testing.T) {
	// The reference forwards ACKs with no delay: they are what a
	// sender is timing out on.
	e, dev, _, _ := txRig(t, "shadow")
	ack, err := meshcore.BuildAck([]byte{1, 2, 3, 4})
	if err != nil {
		t.Fatal(err)
	}
	ack.Header = meshcore.MakeHeader(meshcore.RouteDirect,
		meshcore.PayloadTypeAck, meshcore.PayloadVer1)
	ack.Path = []byte{e.id.PubKey[0], 0x77}
	ack.SetPathHashSizeAndCount(1, 2)
	before := time.Now()
	e.relayFor(dev, ack, verdictRelayDirect, txn.New(), 8)

	if len(e.queue.entries) != 1 {
		t.Fatalf("%d entries queued", len(e.queue.entries))
	}
	if e.queue.entries[0].notBefore.After(before.Add(time.Millisecond)) {
		t.Fatalf("the ACK waits %v — the reference sends it at once",
			e.queue.entries[0].notBefore.Sub(before))
	}
}

func TestAdvertsNeverShareAPass(t *testing.T) {
	// Two adverts built in the same second are byte-identical, so a
	// neighbour would dedup one away: the clocks must not collide.
	e, dev, _, _ := txRig(t, "on-air")
	e.p.AdvertFloodInterval = 48 * time.Hour
	e.p.AdvertLocalInterval = time.Hour
	now := time.Now()
	e.nextFloodAdvert, e.nextLocalAdvert = now, now

	e.dueAdverts(dev, now)
	if len(e.queue.entries) != 1 {
		t.Fatalf("%d adverts queued in one pass, want 1", len(e.queue.entries))
	}
	if !e.nextLocalAdvert.After(now) {
		t.Error("the local clock was not wound on with the flood's")
	}
}

func TestDutyGaugeDecaysWhenIdle(t *testing.T) {
	// The gauge is read from operator sessions long after the last
	// emission: it must report the sliding hour as it stands, not as
	// the last transmission left it.
	d := &dutyLedger{budget: time.Hour}
	d.record(time.Now().Add(-90*time.Minute), 30*time.Second)
	if used := d.usage(time.Now()); used != 0 {
		t.Fatalf("usage = %v after the hour passed, want 0", used)
	}
}

func TestRadioFaultCountsTheLostEmission(t *testing.T) {
	// An entry already popped when the radio faults would otherwise
	// vanish: the session-restart purge only counts what was still
	// queued. This event is the popped entry's only witness.
	e, dev, sub, peer := txRig(t, "on-air")
	dev.assessErr = errors.New("spi: bus gone")
	runEngine(t, e, dev)
	dev.frames <- peerAdvert(t, peer, time.Now())

	deadline := time.After(2 * time.Second)
	for {
		select {
		case ev := <-sub.C:
			if d, ok := ev.(bus.TxDropped); ok {
				if d.Reason != "tx-failed" {
					t.Fatalf("dropped for %q, want tx-failed", d.Reason)
				}
				if d.Txn.Short() == (txn.ID{}).Short() {
					t.Error("the drop does not name its reception")
				}
				return
			}
		case <-deadline:
			t.Fatal("no tx-failed drop on the bus")
		}
	}
}

func TestOperatorAdvertWakesTheReceiver(t *testing.T) {
	// The order must not wait for the next scheduled duty: it closes
	// the receive window and is served now. The rig's clocks are 16 s
	// away, the test's patience 2 s — a prompt emission proves the wake.
	e, dev, sub, _ := txRig(t, "on-air")
	runEngine(t, e, dev)
	time.Sleep(50 * time.Millisecond) // let Run park in Receive

	if err := e.RequestAdvert(false); err != nil {
		t.Fatal(err)
	}
	sent := awaitSent(t, sub)
	if sent.Kind != "advert-local" {
		t.Fatalf("sent = %+v, want advert-local", sent)
	}
	raw := <-dev.sent
	pkt, err := meshcore.ParsePacket(raw)
	if err != nil {
		t.Fatal(err)
	}
	if !pkt.IsRouteDirect() || pkt.PathHashCount() != 0 {
		t.Fatal("the operator's default advert must be zero-hop")
	}

	if err := e.RequestAdvert(true); err != nil {
		t.Fatal(err)
	}
	sent = awaitSent(t, sub)
	if sent.Kind != "advert-flood" {
		t.Fatalf("sent = %+v, want advert-flood", sent)
	}
	if pkt, err = meshcore.ParsePacket(<-dev.sent); err != nil || !pkt.IsRouteFlood() {
		t.Fatalf("flood advert route = %v (%v)", pkt.Route(), err)
	}
}

func TestOperatorAdvertNeedsALiveGate(t *testing.T) {
	e := &engine{} // dry: never armed
	if err := e.RequestAdvert(false); err == nil {
		t.Fatal("a dry engine accepted an emission order")
	}
}

func TestZeroHopRungKeysOnlyTheNeighbourhood(t *testing.T) {
	// The ladder's third rung: local adverts and discovery answers are
	// really keyed, everything routable stays on paper — the parity
	// audit runs on while the node becomes discoverable.
	e, dev, sub, peer := txRig(t, "on-air-zero-hop")
	runEngine(t, e, dev)

	// A flood relay: judged, journalled — never on the air.
	dev.frames <- peerAdvert(t, peer, time.Now())
	sent := awaitSent(t, sub)
	if sent.Kind != "relay-flood" || !sent.Shadow {
		t.Fatalf("sent = %+v, want a paper relay-flood", sent)
	}
	select {
	case raw := <-dev.sent:
		t.Fatalf("the zero-hop rung keyed a flood relay: % X", raw)
	default:
	}

	// A discovery answer: zero-hop, really emitted.
	dev.frames <- scanFrame(t, meshcore.DiscoverReq{
		Filter: meshcore.RepeaterFilter(), Tag: 42,
	})
	sent = awaitSent(t, sub)
	if sent.Kind != "discover-resp" || sent.Shadow {
		t.Fatalf("sent = %+v, want a real discover-resp", sent)
	}
	if pkt, err := meshcore.ParsePacket(<-dev.sent); err != nil || !pkt.IsRouteDirect() {
		t.Fatalf("keyed frame: %v (%v)", pkt, err)
	}
}

func TestArmRefusesANamelessNode(t *testing.T) {
	// The advert carries the name every companion screen will show;
	// a config slug is not a name, so there is no default.
	e := &engine{id: &meshcore.LocalIdentity{}, p: params{DutyCyclePct: 100}}
	if err := e.Arm(protocol.TXPolicy{Mode: "shadow", QueueDepth: 2}); err == nil {
		t.Fatal("armed without a node_name")
	}
}
