package meshcore

import (
	"context"
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
	if d.busy > 0 {
		d.busy--
		return true, nil
	}
	return false, nil
}

func (d *fakeDevice) Transmit(_ context.Context, payload []byte, powerDBm int8) (radio.TxReport, error) {
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
		p:     params{NodeName: "test"},
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
