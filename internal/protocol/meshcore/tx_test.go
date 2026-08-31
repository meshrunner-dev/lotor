package meshcore

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"maps"
	"math"
	"strings"
	"testing"
	"time"

	"go.uber.org/zap"

	"meshrunner.dev/pkg/meshcore"

	"meshrunner.dev/lotor/internal/bus"
	"meshrunner.dev/lotor/internal/correlation"
	"meshrunner.dev/lotor/internal/protocol"
	"meshrunner.dev/lotor/internal/radio"
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
	// txFault is returned beside the report: with txAirtime non-zero
	// it is the chip that signalled TxDone and then failed to go back
	// to listening, which is an emission whatever happened after.
	txFault         error
	txAirtime       time.Duration
	lastCorrelation correlation.ID
}

func newFakeDevice() *fakeDevice {
	return &fakeDevice{
		frames: make(chan radio.Frame, 8),
		sent:   make(chan []byte, 8),
	}
}

func (d *fakeDevice) Envelope() radio.Envelope {
	return radio.Envelope{MaxTxPowerDBm: 5, MaxTxPowerSet: true}
}
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
	d.lastCorrelation, _ = correlation.FromContext(ctx)
	if d.txEntered != nil {
		close(d.txEntered)
		time.Sleep(50 * time.Millisecond) // the caller cancels meanwhile
		d.txCtxErr <- ctx.Err()
	}
	d.sent <- payload
	air := time.Millisecond
	if d.txAirtime != 0 {
		air = d.txAirtime
	}
	return radio.TxReport{
		At: time.Now(), Airtime: air, PowerDBm: powerDBm,
	}, d.txFault
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
	e := newEngine("test-868", params{NodeName: "test", DutyCyclePct: 100}, self, b, zap.NewNop())
	if err := e.Arm(protocol.TXPolicy{
		// CAD on, as an unset configuration resolves it: this rig
		// stands in for a running relay, not for a bare policy.
		Mode: mode, LBTExhausted: "transmit", QueueDepth: 2, PowerDBm: -5, CAD: true,
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

// armedEngine is txRig for tests with no radio to script.
func armedEngine(t *testing.T, mode string) *engine {
	t.Helper()
	e, _, _, _ := txRig(t, mode) //nolint:dogsled // the rig answers four things; this needs one
	return e
}

// rxTestSNR is the signal every hand-built reception is heard at.
const rxTestSNR = 8

// rxOf wraps a packet the way judge() would, so a test can drive a
// responder directly. Running the verdict first is what fills the
// reception's opened envelope — the same order production uses.
func rxOf(e *engine, pkt *meshcore.Packet) *reception {
	rx := &reception{pkt: pkt, id: correlation.New(),
		frame: radio.Frame{At: time.Now(), SNR: rxTestSNR}}
	e.verdict(rx)
	return rx
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
		e.enqueue(dev, pkt, "relay-flood", correlation.New(), 1, 0)
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
	q.push(txEntry{kind: "relay-flood", priority: 1, notBefore: now.Add(-2 * time.Second)})
	q.push(txEntry{kind: "relay-direct", priority: prioDirect, notBefore: now.Add(-time.Second)})
	e, ok := q.pop(now)
	if !ok || e.kind != "relay-direct" {
		t.Fatalf("popped %v, want the direct entry first", e.kind)
	}
}

func TestDutyLedgerAdmitsAndFrees(t *testing.T) {
	d := radio.NewAirtimeLedger(10*time.Millisecond, nil)
	now := time.Now()
	d.Record(now, 8*time.Millisecond)

	if ok, _, _ := d.Admit(now, 2*time.Millisecond); !ok {
		t.Fatal("2ms should fit an 8/10 window")
	}
	ok, freeAt, never := d.Admit(now, 5*time.Millisecond)
	if ok || never {
		t.Fatalf("5ms must wait, not pass (%v) nor die (%v)", ok, never)
	}
	if want := now.Add(time.Hour); !freeAt.Equal(want) {
		t.Fatalf("freeAt = %v, want the 8ms stamp's expiry %v", freeAt, want)
	}
	if ok, _, _ := d.Admit(now.Add(time.Hour+time.Second), 5*time.Millisecond); !ok {
		t.Fatal("the window expired; 5ms should fit again")
	}
	if _, _, never := d.Admit(now, 11*time.Millisecond); !never {
		t.Fatal("an airtime above the whole budget can never fit")
	}
	if used, _, _ := d.Admit(now, time.Millisecond); !used {
		t.Fatal("unbudgeted check sanity")
	}
}

func TestDutyDropsWhatCannotWait(t *testing.T) {
	e, dev, sub, peer := txRig(t, "shadow")
	e.duty = radio.NewAirtimeLedger(time.Microsecond, nil) // any frame exceeds it
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
	if v, _ := e.floodVerdict(rxOf(e, advert(7)), true); v != verdictRelayFlood {
		t.Errorf("advert at 7 hops = %q, want a relay", v)
	}
	if v, why := e.floodVerdict(rxOf(e, advert(8)), true); v != verdictDropFloodHops {
		t.Errorf("advert at 8 hops = %q (%s), want the advert limit to stop it", v, why)
	}

	txt, err := meshcore.BuildDatagram(meshcore.PayloadTypeTxtMsg,
		[]byte{0x01}, []byte{0x02}, make([]byte, 32), []byte("hi"))
	if err != nil {
		t.Fatal(err)
	}
	fill(txt, 8)
	if v, _ := e.floodVerdict(rxOf(e, txt), false); v != verdictRelayFlood {
		t.Errorf("text at 8 hops = %q, want a relay — only adverts stop that early", v)
	}
	// The reference's 64-hop ceiling is a belt: the 6-bit path count
	// saturates at 63 and the capacity check binds first, so it can
	// never fire. A site that lowers the limit is what actually bites.
	e.p.FloodMaxHops = 10
	fill(txt, 10)
	if v, _ := e.floodVerdict(rxOf(e, txt), false); v != verdictDropFloodHops {
		t.Errorf("text at 10 hops = %q under a limit of 10, want it stopped", v)
	}
	fill(txt, 9)
	if v, _ := e.floodVerdict(rxOf(e, txt), false); v != verdictRelayFlood {
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
		e.queue.push(txEntry{pkt: pkt, kind: "relay-flood", origin: correlation.New(),
			priority: 1, notBefore: time.Now().Add(time.Hour)})
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
	if v, _ := e.floodVerdict(rxOf(e, pkt), true); v != verdictDropPathFull {
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
	e.relayFor(dev, rxOf(e, pkt), verdictRelayFlood)

	select {
	case ev := <-sub.C:
		d, ok := ev.(bus.TxDropped)
		if !ok || d.Reason != "malformed" {
			t.Fatalf("event = %+v, want a malformed drop", ev)
		}
		if d.Correlation.Short() == (correlation.ID{}).Short() {
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
	e.relayFor(dev, rxOf(e, ack), verdictRelayDirect)

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
	d := radio.NewAirtimeLedger(time.Hour, nil)
	d.Record(time.Now().Add(-90*time.Minute), 30*time.Second)
	if used := d.Usage(time.Now()); used != 0 {
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
				if d.Correlation.Short() == (correlation.ID{}).Short() {
					t.Error("the drop does not name its reception")
				}
				return
			}
		case <-deadline:
			t.Fatal("no tx-failed drop on the bus")
		}
	}
}

// orderedAdvert drives one operator announcement to the air and hands
// back what left the radio. One engine per order, because the spacing
// guard is pipeline state now and a test cannot reach past it — which
// is the point of it living there.
func orderedAdvert(t *testing.T, flood bool) (bus.FrameSent, *meshcore.Packet) {
	t.Helper()
	// The order must not wait for the next scheduled duty: it closes
	// the receive window and is served now. The rig's clocks are 16 s
	// away, the test's patience 2 s — a prompt emission proves the wake.
	e, dev, sub, _ := txRig(t, "on-air")
	runEngine(t, e, dev)
	time.Sleep(50 * time.Millisecond) // let Run park in Receive

	if err := e.RequestAdvert(flood); err != nil {
		t.Fatal(err)
	}
	sent := awaitSent(t, sub)
	pkt, err := meshcore.ParsePacket(<-dev.sent)
	if err != nil {
		t.Fatal(err)
	}
	return sent, pkt
}

func TestOperatorAdvertWakesTheReceiver(t *testing.T) {
	sent, pkt := orderedAdvert(t, false)
	if sent.Kind != "advert-local" {
		t.Fatalf("sent = %+v, want advert-local", sent)
	}
	if !pkt.IsRouteDirect() || pkt.PathHashCount() != 0 {
		t.Fatal("the operator's default advert must be zero-hop")
	}
}

func TestOperatorFloodAdvertIsRoutable(t *testing.T) {
	sent, pkt := orderedAdvert(t, true)
	if sent.Kind != "advert-flood" {
		t.Fatalf("sent = %+v, want advert-flood", sent)
	}
	if !pkt.IsRouteFlood() {
		t.Fatalf("flood advert route = %v", pkt.Route())
	}
}

func TestOperatorAdvertNeedsALiveGate(t *testing.T) {
	e := &engine{} // dry: never armed
	if err := e.RequestAdvert(false); err == nil {
		t.Fatal("a dry engine accepted an emission order")
	}
}

func TestDryEngineAsksHowLongItMayListen(t *testing.T) {
	// Every receive window asks the pipeline when it next needs the
	// radio — before it asks whether there is a pipeline at all. A dry
	// run has no queue, and the question must still have an answer:
	// seen on the air as a segfault the moment the first relay came up
	// receive-only.
	e := &engine{} // dry: never armed
	if wait, ok := e.txWait(time.Now()); ok {
		t.Fatalf("a dry engine claims something is due in %s", wait)
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

func TestPrioritiesFollowTheReferenceLadder(t *testing.T) {
	// Routed traffic first (0), flood relays by distance (hop count
	// after append), own flood adverts held back (3), traces last (5).
	e, dev, _, peer := txRig(t, "shadow")
	e.queue.depth = 16

	pkt, err := meshcore.BuildAdvert(peer, time.Now(), &meshcore.AdvertData{Name: "p"})
	if err != nil {
		t.Fatal(err)
	}
	pkt.Path = []byte{^e.id.PubKey[0], ^e.id.PubKey[0]}
	pkt.SetPathHashSizeAndCount(1, 2)
	e.relayFor(dev, rxOf(e, pkt), verdictRelayFlood)

	e.advert(dev, time.Now(), "advert-flood", false)
	e.advert(dev, time.Now(), "advert-local", true)
	e.relayFor(dev, rxOf(e, traceMustParse(t, e)), verdictRelayTrace)

	got := map[string]int{}
	for _, entry := range e.queue.entries {
		got[entry.kind] = entry.priority
	}
	want := map[string]int{
		"relay-flood": 3, "advert-flood": 3, "advert-local": 0, "relay-trace": 5,
	}
	for kind, prio := range want {
		if got[kind] != prio {
			t.Errorf("%s priority = %d, want %d", kind, got[kind], prio)
		}
	}
}

// traceMustParse builds a trace whose next target hop is us.
func traceMustParse(t *testing.T, e *engine) *meshcore.Packet {
	t.Helper()
	raw := traceToUs(t, e)
	pkt, err := meshcore.ParsePacket(raw.Payload)
	if err != nil {
		t.Fatal(err)
	}
	return pkt
}

func TestScopedFloodMovesOnlyInAnAllowedRegion(t *testing.T) {
	// With no named region, every transport code is unknown and the
	// reference refuses it. The wildcard cannot authorise it because
	// the wildcard means no code at all.
	e, _, _, peer := txRig(t, "shadow")
	pkt, err := meshcore.BuildAdvert(peer, time.Now(), &meshcore.AdvertData{Name: "p"})
	if err != nil {
		t.Fatal(err)
	}
	pkt.Header = meshcore.MakeHeader(meshcore.RouteTransportFlood,
		meshcore.PayloadTypeAdvert, meshcore.PayloadVer1)
	meshcore.TransportKeyForName("be").Scope(pkt)
	if v, why := e.floodVerdict(rxOf(e, pkt), true); v != verdictDropScoped {
		t.Fatalf("unknown scope = %q (%s), want %q", v, why, verdictDropScoped)
	}

	be, err := e.regions.m.Put("be", 0)
	if err != nil {
		t.Fatal(err)
	}
	be.Flags = 0
	if v, why := e.floodVerdict(rxOf(e, pkt), true); v != verdictRelayFlood {
		t.Fatalf("allowed scope = %q (%s), want %q", v, why, verdictRelayFlood)
	}

	be.Flags = meshcore.RegionDenyFlood
	if v, _ := e.floodVerdict(rxOf(e, pkt), true); v != verdictDropScoped {
		t.Fatalf("a denied scope moved: verdict = %q", v)
	}

	// Opening or shutting plain traffic cannot change the transport
	// decision.
	e.regions.m.Wildcard().Flags = 0
	if v, _ := e.floodVerdict(rxOf(e, pkt), true); v != verdictDropScoped {
		t.Fatalf("the wildcard overrode a named denial: verdict = %q", v)
	}
}

func TestDutySeedSurvivesTheRestart(t *testing.T) {
	// The journal's memory of the sliding hour reaches the new ledger:
	// a crash-loop must not launder the budget.
	seed := make([]byte, meshcore.SeedSize)
	seed[0] = 7
	id, err := meshcore.LocalIdentityFromSeed(seed)
	if err != nil {
		t.Fatal(err)
	}
	e := &engine{id: id, p: params{NodeName: "test", DutyCyclePct: 10}}
	spent := []protocol.Spent{
		{At: time.Now().Add(-30 * time.Minute), Airtime: 30 * time.Second},
		{At: time.Now().Add(-90 * time.Minute), Airtime: 99 * time.Second}, // expired
	}
	if err := e.Arm(protocol.TXPolicy{Mode: "shadow", QueueDepth: 2, Spent: spent}); err != nil {
		t.Fatal(err)
	}
	used, _, ok := e.Duty()
	if !ok || used != 30*time.Second {
		t.Fatalf("seeded usage = %v (%v), want the unexpired 30 s", used, ok)
	}
}

func TestAdvertCarriesThePosition(t *testing.T) {
	e, dev, _, _ := txRig(t, "shadow")
	e.p.NodeLat, e.p.NodeLon = 48.8584, 2.2945
	e.advert(dev, time.Now(), "advert-local", true)
	adv, err := meshcore.ParseAdvert(e.queue.entries[0].pkt.Payload)
	if err != nil {
		t.Fatal(err)
	}
	if lat, lon := adv.Data.Lat(), adv.Data.Lon(); lat < 48.85 || lat > 48.86 || lon < 2.29 || lon > 2.30 {
		t.Fatalf("position = %f,%f", lat, lon)
	}
}

func TestMultipartUnwrapsToOneAck(t *testing.T) {
	// N redundant copies hash differently by their remaining nibble;
	// the unwrapped form collapses them to one forwarded plain ACK.
	e, dev, sub, _ := txRig(t, "on-air")
	e.queue.depth = 8
	build := func(remaining byte) *meshcore.Packet {
		pkt := &meshcore.Packet{
			Header: meshcore.MakeHeader(meshcore.RouteDirect,
				meshcore.PayloadTypeMultipart, meshcore.PayloadVer1),
			Payload: append([]byte{remaining<<4 | byte(meshcore.PayloadTypeAck)},
				0xDE, 0xAD, 0xBE, 0xEF),
			Path: []byte{e.id.PubKey[0], 0x77},
		}
		pkt.SetPathHashSizeAndCount(1, 2)
		return pkt
	}
	e.forwardMultipart(build(1), correlation.New())
	e.forwardMultipart(build(0), correlation.New()) // second copy of the same ACK

	if n := len(e.queue.entries); n != 1 {
		t.Fatalf("%d forwards queued, want the copies collapsed to 1", n)
	}
	entry := e.queue.entries[0]
	if entry.kind != "relay-ack" || entry.priority != prioDirect {
		t.Fatalf("entry = %+v", entry)
	}
	if wait := time.Until(entry.notBefore); wait < 500*time.Millisecond || wait > 700*time.Millisecond {
		t.Fatalf("forward waits %v, want ~(1+1)×300ms", wait)
	}
	ack := entry.pkt
	if ack.PayloadType() != meshcore.PayloadTypeAck || !ack.IsRouteDirect() ||
		ack.PathHashCount() != 1 || ack.Path[0] != 0x77 {
		t.Fatalf("forwarded ack = %+v", ack)
	}
	drop := false
	for done := false; !done; {
		select {
		case ev := <-sub.C:
			if d, ok := ev.(bus.TxDropped); ok && d.Reason == "duplicate" {
				drop = true
			}
		default:
			done = true
		}
	}
	if !drop {
		t.Error("the collapsed copy left no duplicate witness")
	}
	_ = dev
}

func TestOrderedAdvertsAreSpaced(t *testing.T) {
	// The guard lives on the pipeline, not on the console: every door
	// into the engine — the console today, a web button tomorrow —
	// ends at this one clock, on one goroutine, with no lock.
	e := armedEngine(t, "on-air")
	now := time.Now()
	if err := e.advertAskDue(now); err != nil {
		t.Fatalf("the first order was refused: %v", err)
	}
	// The gap is spent when an announcement is actually queued, never
	// on an order the queue then refuses — which is why the check and
	// the spending are two moves.
	e.lastAskedAdvert = now
	err := e.advertAskDue(now.Add(3 * time.Second))
	if err == nil {
		t.Fatal("a second order three seconds later went through")
	}
	// The refusal has to be actionable: how long, not just no.
	if !strings.Contains(err.Error(), "7s to wait") {
		t.Fatalf("refusal said %q", err)
	}
	if err := e.advertAskDue(now.Add(advertAskGap)); err != nil {
		t.Fatalf("an order a full gap later was refused: %v", err)
	}
}

func TestTheFirstOrderedAdvertIsNotHeldBack(t *testing.T) {
	// A zero clock means nothing was ever ordered, not that something
	// was ordered at the epoch.
	if err := armedEngine(t, "on-air").advertAskDue(time.Now()); err != nil {
		t.Fatalf("the first order of a fresh relay was refused: %v", err)
	}
}

func TestRequeueIsPacedNotImmediate(t *testing.T) {
	// A requeue means the radio was busy receiving: retrying with no
	// delay spins SPI polls against a chip mid-demodulation for a
	// whole frame's airtime. The retry must carry the LBT pacing.
	e, _, sub, _ := txRig(t, "shadow")
	defer sub.Close()
	before := time.Now()
	e.requeue(txEntry{kind: "relay-flood", origin: correlation.New()})
	entry := e.queue.entries[0]
	wait := entry.notBefore.Sub(before)
	if wait < lbtRetryNominal/2 || wait > lbtRetryNominal*3/2+50*time.Millisecond {
		t.Errorf("requeue pacing %v — want the LBT retry band around %v", wait, lbtRetryNominal)
	}
}

func TestRequeueAgesOutUnderTheDropPolicy(t *testing.T) {
	// The clock is the engine's, not the entry's: it measures one
	// continuous busy spell, the way Dispatcher::cad_busy_start does.
	e, _, sub, _ := txRig(t, "on-air")
	e.policy.LBTExhausted = "drop"
	e.busySince = time.Now().Add(-lbtMaxWait - time.Second)
	e.requeue(txEntry{kind: "relay-flood", origin: correlation.New()})

	if n := len(e.queue.entries); n != 0 {
		t.Errorf("the aged entry was requeued anyway (%d in the queue)", n)
	}
	select {
	case ev := <-sub.C:
		d, ok := ev.(bus.TxDropped)
		if !ok || d.Reason != "lbt" {
			t.Fatalf("published %#v, want a TxDropped for lbt", ev)
		}
	default:
		t.Fatal("nothing published for the dropped frame")
	}
}

func TestRequeueKeepsWaitingUnderTheTransmitPolicy(t *testing.T) {
	// The radio refuses to key over a reception whatever the policy
	// says, so transmit-anyway waits rather than drops.
	e, _, _, _ := txRig(t, "on-air") //nolint:dogsled // the rig answers four things; this needs one
	e.policy.LBTExhausted = "transmit"
	e.busySince = time.Now().Add(-lbtMaxWait - time.Second)
	e.requeue(txEntry{kind: "relay-flood", origin: correlation.New()})
	if len(e.queue.entries) != 1 {
		t.Fatal("the entry was dropped under a transmit policy")
	}
}

func TestABusySpellStartsOnceAndEndsOnAClearChannel(t *testing.T) {
	e, dev, _, _ := txRig(t, "on-air")
	e.policy.LBTExhausted = "drop"
	e.requeue(txEntry{kind: "relay-flood", origin: correlation.New()})
	started := e.busySince
	if started.IsZero() {
		t.Fatal("the first refusal did not start the spell")
	}
	e.requeue(txEntry{kind: "relay-flood", origin: correlation.New()})
	if !e.busySince.Equal(started) {
		t.Errorf("a second refusal restarted the spell: %v then %v", started, e.busySince)
	}
	// A clear channel ends it, so sparse refusals never accumulate
	// into a drop the way a continuous spell does.
	if _, err := e.clearChannel(context.Background(), dev, zap.NewNop(), correlation.New(), "test"); err != nil {
		t.Fatalf("clearChannel: %v", err)
	}
	if !e.busySince.IsZero() {
		t.Errorf("a clear channel left the spell running: %v", e.busySince)
	}
}

func TestADutyCeilingNeverRoundsAwayToNoCeiling(t *testing.T) {
	// The ledger reads a zero budget as "unbudgeted", so a percentage
	// that rounds to no nanoseconds would turn the most restrictive
	// ceiling an operator can write into no ceiling at all.
	arm := func(pct float64) (*engine, error) {
		seed := make([]byte, meshcore.SeedSize)
		seed[0] = 3
		self, err := meshcore.LocalIdentityFromSeed(seed)
		if err != nil {
			t.Fatal(err)
		}
		e := newEngine("duty", params{NodeName: "n", DutyCyclePct: pct},
			self, bus.New(), zap.NewNop())
		return e, e.Arm(protocol.TXPolicy{Mode: "on-air", QueueDepth: 2})
	}
	// Positive but unrepresentable: refused, and named as such.
	for _, tiny := range []float64{math.SmallestNonzeroFloat64, 1e-20, smallestDutyCyclePct / 2} {
		if _, err := arm(tiny); err == nil {
			t.Errorf("duty_cycle_pct %g armed a relay with no ceiling", tiny)
		} else if !strings.Contains(err.Error(), "rounds to no airtime") {
			t.Errorf("duty_cycle_pct %g refused for the wrong reason: %v", tiny, err)
		}
	}
	// The first representable ceiling holds, and holds honestly: one
	// nanosecond of budget fits no frame, so every emission is
	// refused rather than waved through.
	e, err := arm(smallestDutyCyclePct)
	if err != nil {
		t.Fatalf("the smallest holdable ceiling was refused: %v", err)
	}
	if e.duty.Budget() <= 0 {
		t.Fatalf("budget = %v", e.duty.Budget())
	}
	ok, _, never := e.duty.Admit(time.Now(), time.Millisecond)
	if ok || !never {
		t.Errorf("a frame under a one-nanosecond budget: ok=%v never=%v", ok, never)
	}
	// The band defaults and the unbounded end still arm.
	for _, pct := range []float64{0.1, 1, 10, 100} {
		if _, err := arm(pct); err != nil {
			t.Errorf("duty_cycle_pct %g refused: %v", pct, err)
		}
	}
	// Zero is not a ceiling at all, and Arm has always said so.
	if _, err := arm(0); err == nil {
		t.Error("duty_cycle_pct 0 armed a transmitting relay")
	}
}

func TestAFullQueueRefusesTheOrdersItDrops(t *testing.T) {
	// An order answered "sent" whose packet was dropped is the worst
	// of both: the operator believes the mesh heard from us, and the
	// clocks were wound as if it had.
	e, dev, _, _ := txRig(t, "on-air")
	e.queue.depth = 1
	// One entry already holds the only place.
	e.enqueueAfter(&meshcore.Packet{
		Header:  meshcore.MakeHeader(meshcore.RouteDirect, meshcore.PayloadTypeAck, meshcore.PayloadVer1),
		Payload: []byte{1, 2, 3, 4},
	}, "filler", correlation.New(), prioDirect, time.Hour)

	floodClock, localClock := e.nextFloodAdvert, e.nextLocalAdvert
	o := &advertOrder{kind: "advert-local", started: newAck()}
	e.advertAsk <- o
	e.drainAdvertAsk(dev, time.Now())
	err := o.started.wait("advert")
	if err == nil {
		t.Fatal("a dropped advert was answered as sent")
	}
	if !strings.Contains(err.Error(), "queue is full") {
		t.Errorf("refusal = %v", err)
	}
	// Nothing was spent on it: not the gap, not the clocks.
	if !e.lastAskedAdvert.IsZero() {
		t.Error("the ten-second gap was consumed by an advert that never left")
	}
	if e.nextFloodAdvert != floodClock || e.nextLocalAdvert != localClock {
		t.Error("the advert clocks wound for an announcement nobody heard")
	}

	// The same for a scan: no window opens for a question that was
	// dropped, and the next scan is not blocked behind it.
	s := &sweep{tag: 7, found: make(chan Neighbour, 1), started: newAck(),
		seen: map[[meshcore.PubKeySize]byte]bool{}}
	e.sweepAsk <- s
	e.drainSweepAsk(dev, time.Now())
	if err := s.started.wait("scan"); err == nil {
		t.Fatal("a dropped scan reported a window")
	}
	if e.pendingSweep != nil {
		t.Error("a dropped scan holds the scan slot")
	}
	if e.sweepUntil.Load() != 0 {
		t.Error("a dropped scan published a listening window")
	}
}

func TestAnOrderTheCallerGaveUpOnNeverHappens(t *testing.T) {
	// The radio's retry backoff outlasts the order deadline, so an
	// advert refused to the operator's face used to sit in its channel
	// and go out when the session came back.
	e, dev, _, _ := txRig(t, "on-air")
	e.queue.depth = 4
	o := &advertOrder{kind: "advert-local", started: newAck()}
	e.advertAsk <- o

	// The caller waits out its deadline while nothing turns.
	if err := o.started.wait("advert"); err == nil {
		t.Fatal("an order nobody served reported success")
	}
	// The pipeline comes back to life and finds the abandoned order.
	e.drainAdvertAsk(dev, time.Now())
	if n := len(e.queue.entries); n != 0 {
		t.Fatalf("an abandoned order queued %d emissions", n)
	}
	if !e.lastAskedAdvert.IsZero() {
		t.Error("an abandoned order spent the gap")
	}
}

func TestAGrantTheCallerGaveUpOnIsNotApplied(t *testing.T) {
	// A permission change landing after its author was told it had
	// not is the same defect with teeth.
	e, _ := identifiedEngine(t)
	if err := e.AttachSessions(newFakeStore()); err != nil {
		t.Fatal(err)
	}
	peer, err := meshcore.NewLocalIdentity(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	o := &aclOrder{perms: permAdmin, prefixLen: meshcore.PubKeySize, done: newAck()}
	copy(o.pubKey[:], peer.PubKey[:])
	e.aclAsk <- o
	if err := o.done.wait("permission change"); err == nil {
		t.Fatal("a change nobody served reported success")
	}
	e.drainACLAsk()
	if len(e.acl.by) != 0 {
		t.Fatal("an abandoned grant reached the table")
	}
}

func TestAClaimedOrderStillAnswersAcrossTheDeadline(t *testing.T) {
	// The other side of the arbitration: when the pipeline wins the
	// claim, the caller must hear the real answer rather than a
	// timeout the order is about to contradict.
	a := newAck()
	if !a.claim() {
		t.Fatal("a fresh order could not be claimed")
	}
	go func() {
		time.Sleep(askWait + 50*time.Millisecond)
		a.taken()
	}()
	if err := a.wait("advert"); err != nil {
		t.Errorf("a claimed order reported %v", err)
	}
	// And an abandoned one can never be claimed afterwards.
	b := newAck()
	if err := b.wait("advert"); err == nil {
		t.Fatal("an unserved order reported success")
	}
	if b.claim() {
		t.Error("an abandoned order was claimed after the fact")
	}
}

func TestAFrameRadiatedThenFaultedIsCountedOnce(t *testing.T) {
	// The chip signalled TxDone and then failed to go back to
	// listening. That frame occupied the channel: the ledger and the
	// journal knew it, while the duplicate table and the counters did
	// not — so the status answer and the heartbeat denied an emission
	// the budget had already paid for.
	e, dev, sub, peer := txRig(t, "on-air")
	dev.txFault = errors.New("handback failed")
	pkt, err := meshcore.BuildAdvert(peer, time.Now(), &meshcore.AdvertData{
		Type: meshcore.AdvTypeRepeater, Name: "p",
	})
	if err != nil {
		t.Fatal(err)
	}
	id := correlation.New()
	e.enqueueAfter(pkt, "advert-flood", id, prioDirect, 0)

	err = e.txPhase(context.Background(), dev)
	if err == nil {
		t.Fatal("the radio fault did not reach the supervisor")
	}
	<-dev.sent

	stats := e.TrafficStats()
	if stats.SentDirect+stats.SentFlood != 1 {
		t.Errorf("counted %d emissions, want 1", stats.SentDirect+stats.SentFlood)
	}
	if stats.TxAirtime == 0 {
		t.Error("the traffic tally lost the airtime the ledger charged")
	}
	if used, _, ok := e.Duty(); !ok || used == 0 {
		t.Error("the duty ledger lost the airtime")
	}
	// Witnessed too: our own words must not come back as a stranger's.
	if _, dup := e.seen.witness(pkt.Hash(), correlation.New(), time.Now()); !dup {
		t.Error("the emission was never witnessed — its echo would be re-flooded")
	}
	// And exactly one journal line for it.
	sentEvents := 0
	for {
		select {
		case ev := <-sub.C:
			if _, ok := ev.(bus.FrameSent); ok {
				sentEvents++
			}
			continue
		default:
		}
		break
	}
	if sentEvents != 1 {
		t.Errorf("published %d FrameSent events, want 1", sentEvents)
	}
}

func TestADueFloodAdvertOutranksAnOrderedLocalOne(t *testing.T) {
	// Both carry this second in the same signed payload and hash
	// alike, so a neighbour hearing the zero-hop copy first judges the
	// routable one a duplicate and never re-floods it — and the flood
	// is the one the mesh cannot get again for hours.
	e, dev, _, _ := txRig(t, "on-air")
	e.queue.depth = 8
	now := time.Now()
	e.nextFloodAdvert = now.Add(-time.Second) // due
	e.nextLocalAdvert = now.Add(time.Hour)

	o := &advertOrder{kind: "advert-local", started: newAck()}
	e.advertAsk <- o
	e.drainAdvertAsk(dev, now)
	err := o.started.wait("advert")
	if err == nil {
		t.Fatal("an ordered local advert went out on top of a due flood")
	}
	if !strings.Contains(err.Error(), "flooded advert is due") {
		t.Errorf("refusal = %v", err)
	}
	if len(e.queue.entries) != 0 {
		t.Fatalf("the refused order queued %d emissions", len(e.queue.entries))
	}
	// The flood then goes out on its own turn, alone.
	e.dueAdverts(dev, now)
	if len(e.queue.entries) != 1 || e.queue.entries[0].kind != "advert-flood" {
		t.Fatalf("queue = %+v", e.queue.entries)
	}
	// An ordered local advert with no flood due is served normally.
	e.queue.entries = e.queue.entries[:0]
	e.lastAskedAdvert = time.Time{}
	o2 := &advertOrder{kind: "advert-local", started: newAck()}
	e.advertAsk <- o2
	e.drainAdvertAsk(dev, now.Add(time.Minute))
	if err := o2.started.wait("advert"); err != nil {
		t.Fatalf("an ordered local advert with no flood due: %v", err)
	}
	if len(e.queue.entries) != 1 || e.queue.entries[0].kind != "advert-local" {
		t.Fatalf("queue = %+v", e.queue.entries)
	}
}

func TestTheCADKnobIsADeclaredDivergence(t *testing.T) {
	// The firmware ships its scan disabled and this daemon does not:
	// a Linux host with a healthy SPI bus can afford to look before it
	// speaks. The divergence has to be one a site can turn off, or it
	// cannot be measured against the reference at all.
	e, dev, _, _ := txRig(t, "on-air")
	dev.busy = 3 // the channel would be found busy three times over
	log := zap.NewNop()

	// On — this node's default — the busy channel is waited out.
	outcome, err := e.clearChannel(context.Background(), dev, log, correlation.New(), "test")
	if err != nil || outcome != lbtGo {
		t.Fatalf("outcome %v err %v", outcome, err)
	}
	if dev.busy != 0 {
		t.Errorf("%d refusals unused — the scan did not run", dev.busy)
	}

	// Off — the reference's own posture — nothing is asked of the
	// radio at all, and the frame is keyed straight away.
	e.policy.CAD = false
	dev.busy = 3
	outcome, err = e.clearChannel(context.Background(), dev, log, correlation.New(), "test")
	if err != nil || outcome != lbtGo {
		t.Fatalf("with CAD off: outcome %v err %v", outcome, err)
	}
	if dev.busy != 3 {
		t.Errorf("the channel was assessed %d times with CAD off", 3-dev.busy)
	}
}

func TestAdvertsNeverShareTheSecondTheySign(t *testing.T) {
	// The wire counts in seconds: BuildAdvert signs now.Unix(), so two
	// adverts composed inside one second carry the same payload and
	// hash alike. Comparing nanosecond instants let a flood half a
	// second away read as "not yet" — and the zero-hop local, at
	// priority 0, then taught the neighbour to dedup the routable one.
	//
	// now is pinned just after a second boundary so "same second" and
	// "next second" are unambiguous.
	base := time.Unix(time.Now().Unix(), 0).Add(100 * time.Millisecond)

	// An ordered local advert, with the flood due later in this second.
	ordered := func(t *testing.T, floodAt time.Time) error {
		t.Helper()
		e, dev, _, _ := txRig(t, "on-air")
		e.queue.depth = 8
		e.nextFloodAdvert = floodAt
		e.nextLocalAdvert = base.Add(time.Hour)
		o := &advertOrder{kind: "advert-local", started: newAck()}
		e.advertAsk <- o
		e.drainAdvertAsk(dev, base)
		return o.started.wait("advert")
	}
	if err := ordered(t, base.Add(500*time.Millisecond)); err == nil {
		t.Error("an ordered local advert went out in the second the flood will sign")
	}
	// A flood in the next second is a different signature: no conflict.
	if err := ordered(t, base.Add(1500*time.Millisecond)); err != nil {
		t.Errorf("an ordered local advert a second clear of the flood: %v", err)
	}

	// The scheduled side of the same conflict, which the operator
	// never touches: both clocks due inside one second.
	scheduled := func(t *testing.T, floodAt time.Time) []string {
		t.Helper()
		e, dev, _, _ := txRig(t, "on-air")
		e.queue.depth = 8
		e.nextFloodAdvert = floodAt
		e.nextLocalAdvert = base
		e.dueAdverts(dev, base)
		kinds := make([]string, 0, len(e.queue.entries))
		for _, entry := range e.queue.entries {
			kinds = append(kinds, entry.kind)
		}
		return kinds
	}
	got := scheduled(t, base.Add(500*time.Millisecond))
	if len(got) != 1 || got[0] != "advert-flood" {
		t.Errorf("both clocks inside one second queued %v, want the flood alone", got)
	}
	// A second apart, the local one is served on its own turn.
	got = scheduled(t, base.Add(1500*time.Millisecond))
	if len(got) != 1 || got[0] != "advert-local" {
		t.Errorf("clocks a second apart queued %v, want the local one", got)
	}
}

func TestTwoAdvertsInOneSecondWouldHashAlike(t *testing.T) {
	// What the arbitration is protecting against, stated directly: the
	// signature covers a whole-second timestamp, so two adverts
	// composed inside one second are one packet as far as a
	// neighbour's duplicate table is concerned.
	e := armedEngine(t, "on-air")
	at := time.Unix(time.Now().Unix(), 0)
	build := func(when time.Time) *meshcore.Packet {
		pkt, err := meshcore.BuildAdvert(e.id, when, &meshcore.AdvertData{
			Type: meshcore.AdvTypeRepeater, Name: e.p.NodeName,
		})
		if err != nil {
			t.Fatal(err)
		}
		return pkt
	}
	same := build(at).Hash()
	if same != build(at.Add(900*time.Millisecond)).Hash() {
		t.Error("two adverts inside one second hash differently — the premise moved")
	}
	if same == build(at.Add(time.Second)).Hash() {
		t.Error("adverts a second apart hash alike")
	}
}

// The hand-over rule rests on a retransmission hashing like its
// original, so the peer's dedup absorbs it. That holds for every
// packet type but one: a trace's hash covers its path length, so the
// copy carrying our hop is new to the peer and must be carried.
func TestOnlyATraceRetransmissionIsNewToAPeer(t *testing.T) {
	ordinary, err := meshcore.BuildAck([]byte{1, 2, 3, 4})
	if err != nil {
		t.Fatal(err)
	}
	before := ordinary.Hash()
	if err := ordinary.AppendPathHash([]byte{0xAB}); err != nil {
		t.Fatal(err)
	}
	if ordinary.Hash() != before {
		t.Fatalf("an ordinary retransmission changed hash %x -> %x — "+
			"withholding it from a peer would now lose the packet",
			before, ordinary.Hash())
	}

	trace, err := meshcore.BuildTrace(0x2A2A2A2A, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	beforeTrace := trace.Hash()
	if err := trace.AppendTraceHop(6); err != nil {
		t.Fatal(err)
	}
	if trace.Hash() == beforeTrace {
		t.Fatalf("a trace retransmission kept hash %x — then it is a duplicate "+
			"like the others, and relay-trace should stop being carried", beforeTrace)
	}
}
