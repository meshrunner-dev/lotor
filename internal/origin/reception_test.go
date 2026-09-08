package origin

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"meshrunner.dev/lotor/internal/bus"
	"meshrunner.dev/lotor/internal/config"
	"meshrunner.dev/lotor/internal/radio"
)

func TestReceptionAtKeyingKeepsTheFrameAndReleasesDuty(t *testing.T) {
	for _, cad := range []bool{false, true} {
		name := "cad-off"
		if cad {
			name = "cad-clear"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				b := bus.New()
				sub := b.Subscribe(4)
				defer sub.Close()
				p := New(Config{Bus: b}, 1)
				dev := &fakeRadio{airtime: time.Millisecond,
					txErr: errors.Join(radio.ErrBusyReceiving, errors.New("packet arrived after CAD"))}
				ledger := radio.NewAirtimeLedger(time.Millisecond, nil)
				policy := Policy{Mode: config.TXOnAir, CAD: cad, LBTExhausted: config.LBTDrop}
				item := emission("reply")
				item.Priority, item.Route = 3, RouteDirect
				item.Expires = time.Now().Add(time.Minute)
				started := time.Now()
				out := p.Emit(t.Context(), item, dev, ledger, policy, 10)
				if !out.Requeued || out.Sent || out.Dropped != "" || dev.transmits != 1 {
					t.Fatalf("keying refusal: %+v, transmits=%d", out, dev.transmits)
				}
				if (dev.assesses == 1) != cad || len(sub.C) != 0 {
					t.Fatalf("assessments=%d, premature terminal events=%d", dev.assesses, len(sub.C))
				}
				assertDutyReleased(t, ledger, time.Millisecond)
				if _, ok := p.Queue.TakeUntil(t.Context(), started.Add(DefaultLBTRetry/2-time.Nanosecond)); ok {
					t.Fatal("a receiving radio was retried without pacing")
				}
				retry, ok := p.Queue.TakeUntil(t.Context(), started.Add(2*DefaultLBTRetry))
				if !ok || !retry.BusySince.Equal(started) || !bytes.Equal(retry.Frame, item.Frame) ||
					retry.Correlation != item.Correlation || retry.Priority != item.Priority ||
					retry.Route != item.Route || retry.Kind != item.Kind || !retry.Expires.Equal(item.Expires) {
					t.Fatalf("retry lost the emission: %+v, taken=%v", retry, ok)
				}
				if delay := retry.NotBefore.Sub(started); delay < DefaultLBTRetry/2 || delay >= 3*DefaultLBTRetry/2 {
					t.Fatalf("retry delay=%s", delay)
				}
				dev.txErr = nil
				out = p.Emit(t.Context(), retry, dev, ledger, policy, 10)
				if !out.Sent || out.Requeued || out.Dropped != "" || dev.transmits != 2 ||
					ledger.Usage(time.Now()) != time.Millisecond {
					t.Fatalf("retry on a free radio: %+v, transmits=%d, duty=%s", out, dev.transmits, ledger.Usage(time.Now()))
				}
				if event, ok := (<-sub.C).(bus.FrameSent); !ok || event.Correlation != item.Correlation || len(sub.C) != 0 {
					t.Fatalf("retry terminal event=%+v, unread=%d", event, len(sub.C))
				}
			})
		})
	}
}

// receptionRaceRadio can hold the last hardware guard while the frame
// expires or its owner shuts down; no airtime is radiated in that case.
type receptionRaceRadio struct {
	fakeRadio

	transmitDelay time.Duration
	onTransmit    func()
}

func (r *receptionRaceRadio) Transmit(ctx context.Context, frame []byte, power int8) (radio.TxReport, error) {
	time.Sleep(r.transmitDelay)
	if r.onTransmit != nil {
		r.onTransmit()
	}
	return r.fakeRadio.Transmit(ctx, frame, power)
}

func TestReceptionAtKeyingHonoursDropExpiryAndQueueLimits(t *testing.T) {
	for _, tc := range []struct {
		name          string
		exhausted     string
		busyFor       time.Duration
		lifetime      time.Duration
		transmitDelay time.Duration
		full          bool
		wantDrop      string
	}{
		{name: "bound-drop", exhausted: config.LBTDrop, busyFor: DefaultLBTBound, wantDrop: "lbt"},
		{name: "bound-transmit-waits-for-reception", exhausted: config.LBTTransmit, busyFor: 2 * DefaultLBTBound},
		{name: "retry-past-expiry", lifetime: DefaultLBTRetry / 4, wantDrop: "expired"},
		{name: "expiry-during-refusal", lifetime: time.Millisecond, transmitDelay: 2 * time.Millisecond, wantDrop: "expired"},
		{name: "queue-full", full: true, wantDrop: "queue-full"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				b := bus.New()
				sub := b.Subscribe(2)
				defer sub.Close()
				p := New(Config{Bus: b}, 1)
				dev := &receptionRaceRadio{fakeRadio: fakeRadio{airtime: time.Millisecond, txErr: radio.ErrBusyReceiving},
					transmitDelay: tc.transmitDelay}
				ledger := radio.NewAirtimeLedger(time.Millisecond, nil)
				item := emission("reply")
				if tc.busyFor > 0 {
					item.BusySince = time.Now().Add(-tc.busyFor)
				}
				if tc.lifetime > 0 {
					item.Expires = time.Now().Add(tc.lifetime)
				}
				if tc.full && !p.Queue.Offer(emission("another-frame")) {
					t.Fatal("could not fill the queue")
				}
				out := p.Emit(t.Context(), item, dev, ledger,
					Policy{Mode: config.TXOnAir, LBTExhausted: tc.exhausted}, 10)
				if out.Dropped != tc.wantDrop || out.Sent || out.Requeued != (tc.wantDrop == "") {
					t.Fatalf("reception limit: %+v, want drop=%q", out, tc.wantDrop)
				}
				assertDutyReleased(t, ledger, time.Millisecond)
				if tc.wantDrop == "" {
					retry := p.Queue.Drain()
					if len(retry) != 1 || !retry[0].BusySince.Equal(item.BusySince) || len(sub.C) != 0 {
						t.Fatalf("continued reception reset its bound or ended: retries=%+v, events=%d", retry, len(sub.C))
					}
					return
				}
				if event, ok := (<-sub.C).(bus.TxDropped); !ok || event.Reason != tc.wantDrop || len(sub.C) != 0 {
					t.Fatalf("drop event=%+v, unread=%d", event, len(sub.C))
				}
				if queued := p.Queue.Drain(); (len(queued) == 1) != tc.full ||
					(tc.full && queued[0].Kind != "another-frame") {
					t.Fatalf("refusal displaced a queued frame: %+v", queued)
				}
			})
		})
	}
}

func TestAReceptionErrorAfterRadiatingIsStillAccounted(t *testing.T) {
	b := bus.New()
	sub := b.Subscribe(2)
	defer sub.Close()
	p := New(Config{Bus: b}, 1)
	dev := &fakeRadio{airtime: time.Millisecond, txErr: radio.ErrBusyReceiving, txErrAirtime: 2 * time.Millisecond}
	ledger := radio.NewAirtimeLedger(10*time.Millisecond, nil)
	out := p.Emit(t.Context(), emission("radiated"), dev, ledger, Policy{Mode: config.TXOnAir}, 10)
	if !out.Sent || out.Requeued || out.Dropped != "" || out.Airtime != dev.txErrAirtime || p.Queue.Len() != 0 ||
		ledger.Usage(time.Now()) != dev.txErrAirtime {
		t.Fatalf("radiated frame was retried or not charged: %+v, queue=%d, duty=%s", out, p.Queue.Len(), ledger.Usage(time.Now()))
	}
	if event, ok := (<-sub.C).(bus.FrameSent); !ok || event.Airtime != dev.txErrAirtime || len(sub.C) != 0 {
		t.Fatalf("radiated event=%+v, unread=%d", event, len(sub.C))
	}
}

func TestCancellationBeforeRadiatingHasATerminalOutcome(t *testing.T) {
	for _, phase := range []string{"before-duty", "after-clear", "cad-retry", "keying-refused", "keying-radiated"} {
		t.Run(phase, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				b := bus.New()
				sub := b.Subscribe(2)
				defer sub.Close()
				p := New(Config{Bus: b}, 1)
				ledger := radio.NewAirtimeLedger(time.Millisecond, nil)
				var dev radio.Device
				policy := Policy{Mode: config.TXOnAir}
				switch phase {
				case "before-duty":
					cancel()
					dev = &fakeRadio{airtime: time.Millisecond}
				case "after-clear", "cad-retry":
					policy.CAD = true
					r := &delayedRadio{fakeRadio: fakeRadio{airtime: time.Millisecond, busy: phase == "cad-retry"}}
					if phase == "after-clear" {
						r.assessDelay = 2 * time.Millisecond
					}
					dev = r
					go func() { time.Sleep(time.Millisecond); cancel() }()
				case "keying-refused", "keying-radiated":
					r := &receptionRaceRadio{fakeRadio: fakeRadio{airtime: time.Millisecond, txErr: radio.ErrBusyReceiving}, onTransmit: cancel}
					if phase == "keying-radiated" {
						r.txErrAirtime = time.Millisecond
					}
					dev = r
				}
				out := p.Emit(ctx, emission("cancelled"), dev, ledger, policy, 10)
				if phase == "keying-radiated" {
					if !out.Sent || out.Requeued || out.Dropped != "" || ledger.Usage(time.Now()) != time.Millisecond {
						t.Fatalf("cancellation lost a radiated frame: %+v", out)
					}
					if _, ok := (<-sub.C).(bus.FrameSent); !ok || len(sub.C) != 0 {
						t.Fatalf("radiated frame was not announced exactly once, unread=%d", len(sub.C))
					}
					return
				}
				if out.Dropped != "cancelled" || out.Sent || out.Requeued || p.Queue.Len() != 0 {
					t.Fatalf("cancelled emission: %+v, queue=%d", out, p.Queue.Len())
				}
				assertDutyReleased(t, ledger, time.Millisecond)
				if event, ok := (<-sub.C).(bus.TxDropped); !ok || event.Reason != "cancelled" {
					t.Fatalf("cancellation event=%+v", event)
				}
				if len(sub.C) != 0 {
					t.Fatalf("duplicate terminal events=%d", len(sub.C))
				}
			})
		})
	}
}

func assertDutyReleased(t *testing.T, ledger *radio.AirtimeLedger, airtime time.Duration) {
	t.Helper()
	if usage := ledger.Usage(time.Now()); usage != 0 {
		t.Fatalf("unradiated attempt charged %s", usage)
	}
	reservation, _, _ := ledger.Reserve(time.Now(), airtime)
	if reservation == nil {
		t.Fatal("unradiated attempt kept its duty reservation")
	}
	reservation.Cancel()
}
