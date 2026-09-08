package room

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.uber.org/zap"

	"meshrunner.dev/lotor/internal/application"
	"meshrunner.dev/lotor/internal/config"
	"meshrunner.dev/lotor/internal/correlation"
	"meshrunner.dev/lotor/internal/origin"
	"meshrunner.dev/lotor/internal/radio"

	mesh "meshrunner.dev/pkg/meshcore"
)

// The protocol bench normally stops at composition. These tests carry
// pushes through the emission door too: the reference's ACK clock is
// about delivery to a member, not time spent waiting for hosted RF.
func pushRoom(t *testing.T) (*service, client) {
	t.Helper()
	svc := benchRoom(t, nil)
	svc.tx.Mode = config.TXOnAir
	alice, bob := newClient(t, svc), newClient(t, svc)
	login(t, svc, alice, "welcome", 0)
	login(t, svc, bob, "welcome", 0)
	if ack, _ := sendPost(t, svc, alice, "hello room", time.Now()); ack == nil {
		t.Fatal("post refused")
	}
	svc.posts[0].at -= uint32(2 * postSyncDelay / time.Second)
	svc.nextPush = time.Now().Add(time.Hour)
	return svc, bob
}

func queuePush(t *testing.T, svc *service, c client) origin.Emission {
	t.Helper()
	svc.mu.Lock()
	pushed := svc.pushToLocked(time.Now(), c.id.PubKey)
	svc.mu.Unlock()
	if !pushed {
		t.Fatal("the reader was not offered its post")
	}
	return queued(t, svc)
}

func finishPush(svc *service, item origin.Emission, out origin.Outcome) {
	svc.mu.Lock()
	svc.pushOutcomeLocked(item.Correlation, out)
	svc.mu.Unlock()
}

func pushState(svc *service, c client) member {
	svc.mu.Lock()
	defer svc.mu.Unlock()
	return *svc.member(c.id.PubKey)
}

type pushRadio struct {
	roomRadio

	assess   func() (bool, error)
	transmit func(context.Context, []byte, int8) (radio.TxReport, error)
}

func (r *pushRadio) AssessChannel(context.Context, float64) (bool, error) {
	if r.assess != nil {
		return r.assess()
	}
	return false, nil
}

func (r *pushRadio) Transmit(ctx context.Context, frame []byte, power int8) (radio.TxReport, error) {
	if r.transmit != nil {
		return r.transmit(ctx, frame, power)
	}
	return r.roomRadio.Transmit(ctx, frame, power)
}

func TestAPushWaitsForEmissionBeforeStartingItsAckClock(t *testing.T) {
	svc, bob := pushRoom(t)
	svc.pipeline = origin.New(origin.Config{LBTRetry: time.Millisecond}, 8)
	item := queuePush(t, svc, bob)
	waiting := pushState(svc, bob)
	if waiting.pendingAck == 0 || !waiting.ackDeadline.IsZero() {
		t.Fatalf("queued push has no CRC or already has a deadline: %+v", waiting)
	}
	// Even a long queue wait cannot consume a delivery attempt or
	// permit a second copy to replace the pending one.
	svc.pushDue(time.Now().Add(2 * time.Hour))
	if m := pushState(svc, bob); m.failures != 0 || m.pendingAck != waiting.pendingAck || !m.ackDeadline.IsZero() {
		t.Fatalf("queue wait changed the delivery state: %+v", m)
	}
	dev := &pushRadio{assess: func() (bool, error) { return false, radio.ErrBusyReceiving }}
	ledger := radio.NewAirtimeLedger(0, nil)
	policy := origin.Policy{Mode: config.TXOnAir, CAD: true}
	out := svc.pipeline.Emit(context.Background(), item, dev, ledger, policy, 0)
	finishPush(svc, item, out)
	if !out.Requeued || !pushState(svc, bob).ackDeadline.IsZero() {
		t.Fatalf("a reception retry started the ACK clock: %+v", out)
	}
	item = queued(t, svc)
	dev.assess = nil
	out = svc.pipeline.Emit(context.Background(), item, dev, ledger, policy, 0)
	finishPush(svc, item, out)
	if !out.Sent {
		t.Fatalf("push did not leave: %+v", out)
	}
	if m := pushState(svc, bob); m.ackDeadline != out.At.Add(pushAckFlood) || m.failures != 0 {
		t.Fatalf("sent push did not start the reference's flood deadline: %+v", m)
	}
}

func TestLocalPushRefusalsDoNotSpendTheReadersRetries(t *testing.T) {
	for _, reason := range []string{"radio-down", "duty", "expired", "tx-failed", "cancelled", "shadow"} {
		t.Run(reason, func(t *testing.T) {
			svc, bob := pushRoom(t)
			dev := &pushRadio{}
			var device radio.Device = dev
			ledger := radio.NewAirtimeLedger(0, nil)
			policy := origin.Policy{Mode: config.TXOnAir}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			for range maxPushFailures + 1 {
				item := queuePush(t, svc, bob)
				switch reason {
				case "radio-down":
					device = nil
				case "duty":
					ledger = radio.NewAirtimeLedger(time.Millisecond, nil)
					item.Expires = time.Time{}
				case "expired":
					item.Expires = time.Now().Add(-time.Second)
				case "tx-failed":
					dev.transmit = func(context.Context, []byte, int8) (radio.TxReport, error) {
						return radio.TxReport{}, errors.New("radio failed before transmitting")
					}
				case "cancelled":
					ledger = radio.NewAirtimeLedger(time.Second, nil)
					r, _, _ := ledger.Reserve(time.Now(), time.Second)
					defer r.Cancel()
					cancel()
				case "shadow":
					policy.Mode = config.TXShadow
				}
				out := svc.pipeline.Emit(ctx, item, device, ledger, policy, 0)
				if reason == "shadow" {
					if !out.Sent || !out.Shadow {
						t.Fatalf("expected shadow accounting: %+v", out)
					}
				} else if out.Dropped != reason {
					t.Fatalf("expected %s: %+v", reason, out)
				}
				finishPush(svc, item, out)
				if m := pushState(svc, bob); m.pendingAck != 0 || m.failures != 0 || !m.ackDeadline.IsZero() {
					t.Fatalf("local refusal spent a delivery retry: %+v", m)
				}
			}
		})
	}
}

func TestAFullQueueLeavesTheReaderReadyForTheNextPush(t *testing.T) {
	svc, bob := pushRoom(t)
	svc.pipeline = origin.New(origin.Config{}, 1)
	if out := svc.pipeline.Submit(origin.Emission{Frame: []byte{1}}); !out.Requeued {
		t.Fatal("could not fill the queue")
	}
	svc.mu.Lock()
	svc.pushToLocked(time.Now(), bob.id.PubKey)
	svc.mu.Unlock()
	if m := pushState(svc, bob); m.pendingAck != 0 || m.failures != 0 {
		t.Fatalf("queue refusal left an ACK outstanding: %+v", m)
	}
	queued(t, svc) // the unrelated frame leaves; the reader can be offered its post again
	if item := queuePush(t, svc, bob); item.Kind != "post-push" {
		t.Fatalf("the next turn produced %q", item.Kind)
	}
}

func TestAFastHandoverAckWinsBeforeTheEmissionReturns(t *testing.T) {
	svc, bob := pushRoom(t)
	item := queuePush(t, svc, bob)
	dev := &pushRadio{transmit: func(_ context.Context, raw []byte, power int8) (radio.TxReport, error) {
		pkt, err := mesh.ParsePacket(raw)
		if err != nil {
			t.Fatal(err)
		}
		d, err := mesh.ParseDatagram(pkt.Payload)
		if err != nil {
			t.Fatal(err)
		}
		plain, err := d.Open(bob.secret)
		if err != nil {
			t.Fatal(err)
		}
		ack, err := mesh.BuildTextAckBody(plain, bob.id.PubKey[:])
		if err != nil {
			t.Fatal(err)
		}
		// The controller's hand-over can deliver the peer's ACK before
		// Emit reports the successful transmission back to the room.
		svc.handleAck(ack, correlation.New())
		return radio.TxReport{At: time.Now(), Airtime: time.Millisecond, PowerDBm: power}, nil
	}}
	out := svc.pipeline.Emit(context.Background(), item, dev, radio.NewAirtimeLedger(0, nil), origin.Policy{Mode: config.TXOnAir}, 0)
	finishPush(svc, item, out)
	if !out.Sent {
		t.Fatalf("push did not leave: %+v", out)
	}
	if m := pushState(svc, bob); m.pendingAck != 0 || !m.ackDeadline.IsZero() || m.syncSince != svc.posts[0].at {
		t.Fatalf("late emission result overwrote the fast ACK: %+v", m)
	}
}

func TestAPushOutcomeCannotReplaceANewerPushAfterAKeepAlive(t *testing.T) {
	svc, bob := pushRoom(t)
	old := queuePush(t, svc, bob)
	req, err := mesh.BuildRequest(bob.id, svc.id.PubKey[:], bob.secret, uint32(time.Now().Unix()), mesh.FrameKeepAliveRequest(0))
	if err != nil {
		t.Fatal(err)
	}
	req.Header = mesh.MakeHeader(mesh.RouteDirect, mesh.PayloadTypeReq, mesh.PayloadVer1)
	hear(t, svc, req)
	newer := queuePush(t, svc, bob)
	finishPush(svc, old, origin.Outcome{Sent: true, At: time.Now()})
	if m := pushState(svc, bob); m.pushEmission != newer.Correlation || !m.ackDeadline.IsZero() {
		t.Fatalf("old outcome changed the newer push: %+v", m)
	}
}

func TestRoomPushesResumeAfterTheRadioIsReattached(t *testing.T) {
	svc, bob := pushRoom(t)
	dev := &roomRadio{frames: make(chan radio.Frame)}
	controller, err := radio.NewController("slot1", radio.Driver{
		Inspect: func(map[string]any) (radio.Envelope, error) { return dev.Envelope(), nil },
		Open:    func(map[string]any, *zap.Logger) (radio.Device, error) { return dev, nil },
	}, nil, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go controller.Run(ctx)
	done := make(chan struct{})
	go func() { defer close(done); _ = svc.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })
	waitFor := func(ready func(member) bool) {
		t.Helper()
		deadline := time.Now().Add(2 * time.Second)
		for !ready(pushState(svc, bob)) {
			if time.Now().After(deadline) {
				t.Fatalf("push state did not settle: %+v", pushState(svc, bob))
			}
			time.Sleep(time.Millisecond)
		}
	}
	for range maxPushFailures + 1 {
		svc.mu.Lock()
		pushed := svc.pushToLocked(time.Now(), bob.id.PubKey)
		svc.mu.Unlock()
		if !pushed {
			t.Fatal("detached reader was stalled")
		}
		waitFor(func(m member) bool { return m.pendingAck == 0 })
	}
	binding, err := controller.Bind("lobby", radio.RoleApplication, svc.p.Waveform)
	if err != nil {
		t.Fatal(err)
	}
	svc.AttachRadio("slot1", binding, radio.NewAirtimeLedger(0, nil), "")
	waitRF(t, svc, application.RFActive)
	svc.mu.Lock()
	pushed := svc.pushToLocked(time.Now(), bob.id.PubKey)
	svc.mu.Unlock()
	if !pushed {
		t.Fatal("reattaching did not leave the reader ready")
	}
	waitFor(func(m member) bool { return !m.ackDeadline.IsZero() })
	if m := pushState(svc, bob); m.failures != 0 {
		t.Fatalf("radio downtime spent reader retries: %+v", m)
	}
}
