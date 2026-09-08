package origin

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"meshrunner.dev/lotor/internal/bus"
	"meshrunner.dev/lotor/internal/config"
	"meshrunner.dev/lotor/internal/correlation"
	"meshrunner.dev/lotor/internal/radio"
)

// fakeRadio is a device that counts what the pipeline asked of it.
type fakeRadio struct {
	transmits, assesses int
	assessErr           error
	// txErr fails every transmission; txErrAirtime is the airtime the
	// failing report still claims radiated, zero for a dead key.
	txErr        error
	txErrAirtime time.Duration
	busy         bool
	airtime      time.Duration
}

func (*fakeRadio) Envelope() radio.Envelope {
	return radio.Envelope{MaxTxPowerSet: true, MaxTxPowerDBm: 22, ChipMinDBm: -9, ChipMaxDBm: 22}
}
func (*fakeRadio) Configure(radio.Waveform) error { return nil }
func (*fakeRadio) StartReceive() error            { return nil }
func (*fakeRadio) Receive(ctx context.Context) (radio.Frame, error) {
	<-ctx.Done()
	return radio.Frame{}, ctx.Err()
}
func (*fakeRadio) NoiseFloor() (radio.NoiseFloor, bool) { return radio.NoiseFloor{}, false }
func (*fakeRadio) NoiseStarved() uint64                 { return 0 }
func (*fakeRadio) ChipStats() (radio.ChipStats, bool)   { return radio.ChipStats{}, false }
func (r *fakeRadio) Airtime(int) time.Duration          { return r.airtime }
func (*fakeRadio) Close() error                         { return nil }
func (r *fakeRadio) AssessChannel(context.Context, float64) (bool, error) {
	r.assesses++
	return r.busy, r.assessErr
}
func (r *fakeRadio) Transmit(_ context.Context, _ []byte, power int8) (radio.TxReport, error) {
	r.transmits++
	if r.txErr != nil {
		return radio.TxReport{At: time.Now(), Airtime: r.txErrAirtime, PowerDBm: power}, r.txErr
	}
	return radio.TxReport{At: time.Now(), Airtime: r.airtime, PowerDBm: power}, nil
}

// TestThePipelineTellsTheBusWhatItSentAndWhatItDropped is the bus
// contract: every emission ends as one FrameSent — shadow marked as
// such — or one TxDropped naming its reason, under the producer's own
// kind and name.
func TestThePipelineTellsTheBusWhatItSentAndWhatItDropped(t *testing.T) {
	b := bus.New()
	sub := b.Subscribe(16)
	defer sub.Close()
	p := New(Config{SourceKind: bus.SourceApplication, Source: "lobby", Bus: b, DutyWait: 50 * time.Millisecond}, 1)
	dev := &fakeRadio{airtime: 100 * time.Millisecond}
	free := radio.NewAirtimeLedger(time.Hour, nil)

	p.Emit(context.Background(), emission("shadow"), dev, free, Policy{Mode: config.TXShadow}, 10)
	sent, ok := (<-sub.C).(bus.FrameSent)
	if !ok || !sent.Shadow || sent.SourceKind != bus.SourceApplication || sent.Source != "lobby" ||
		sent.Kind != "shadow" || sent.Airtime != 100*time.Millisecond {
		t.Fatalf("shadow emission announced as %+v", sent)
	}
	p.Emit(context.Background(), emission("air"), dev, free, Policy{Mode: config.TXOnAir}, 10)
	if sent, ok := (<-sub.C).(bus.FrameSent); !ok || sent.Shadow || sent.Kind != "air" {
		t.Fatalf("on-air emission announced as %+v", sent)
	}

	// Every refusal is one TxDropped with its reason.
	saturated := radio.NewAirtimeLedger(time.Second, []radio.AirtimeStamp{{At: time.Now(), Airtime: time.Second}})
	dead := &fakeRadio{airtime: 100 * time.Millisecond, txErr: errors.New("pa fault")}
	radiated := &fakeRadio{airtime: 100 * time.Millisecond, txErr: errors.New("late irq"), txErrAirtime: 90 * time.Millisecond}
	if out := p.Emit(context.Background(), emission("radiated"), radiated, free, Policy{Mode: config.TXOnAir}, 10); !out.Sent ||
		out.Airtime != 90*time.Millisecond {
		t.Fatalf("a failing key that still radiated: %+v", out)
	}
	if sent, ok := (<-sub.C).(bus.FrameSent); !ok || sent.Kind != "radiated" {
		t.Fatalf("radiated failure announced as %+v", sent)
	}
	for _, c := range []struct {
		reason string
		emit   func() Outcome
	}{
		{"dry", func() Outcome { return p.Emit(context.Background(), emission("dry"), dev, free, Policy{}, 10) }},
		{"radio-down", func() Outcome {
			return p.Emit(context.Background(), emission("down"), nil, free, Policy{Mode: config.TXOnAir}, 10)
		}},
		{"duty", func() Outcome {
			return p.Emit(context.Background(), emission("duty"), dev, saturated, Policy{Mode: config.TXOnAir}, 10)
		}},
		{"tx-failed", func() Outcome {
			return p.Emit(context.Background(), emission("dead"), dead, free, Policy{Mode: config.TXOnAir}, 10)
		}},
		{"queue-full", func() Outcome {
			if !p.Queue.Offer(emission("first")) {
				t.Fatal("a queue of one refused its first frame")
			}
			return p.Requeue(emission("second"))
		}},
	} {
		if out := c.emit(); out.Dropped != c.reason {
			t.Fatalf("%s: %+v", c.reason, out)
		}
		dropped, ok := (<-sub.C).(bus.TxDropped)
		if !ok || dropped.Reason != c.reason || dropped.SourceKind != bus.SourceApplication || dropped.Source != "lobby" {
			t.Fatalf("%s announced as %+v", c.reason, dropped)
		}
	}
	if free.Usage(time.Now()) != 290*time.Millisecond {
		t.Errorf("ledger usage %s: the shadow, the on-air and the radiated failure should have spent", free.Usage(time.Now()))
	}
	if sub.Dropped() != 0 || len(sub.C) != 0 {
		t.Errorf("bus: %d events lost, %d unread", sub.Dropped(), len(sub.C))
	}
}

func emission(kind string) Emission {
	return Emission{Frame: []byte{1, 2, 3}, Correlation: correlation.New(), Kind: kind}
}

func TestTheGateDecidesWhetherTheRadioIsKeyed(t *testing.T) {
	p := New(Config{SourceKind: "test", Source: "t"}, 4)
	dev := &fakeRadio{airtime: 100 * time.Millisecond}
	ledger := radio.NewAirtimeLedger(time.Hour, nil)

	// Dry and empty reach no radio: the contract enforced where the
	// keying would happen, not merely stated.
	for _, mode := range []string{"", config.TXDry} {
		out := p.Emit(context.Background(), emission("dry"), dev, ledger, Policy{Mode: mode}, 10)
		if out.Sent || out.Dropped != "dry" || dev.transmits != 0 {
			t.Fatalf("mode %q: %+v, transmits %d", mode, out, dev.transmits)
		}
	}
	// Shadow spends the duty it would have spent and never keys.
	out := p.Emit(context.Background(), emission("shadow"), dev, ledger, Policy{Mode: config.TXShadow}, 10)
	if !out.Sent || !out.Shadow || dev.transmits != 0 || ledger.Usage(time.Now()) != 100*time.Millisecond {
		t.Fatalf("shadow: %+v, transmits %d, usage %s", out, dev.transmits, ledger.Usage(time.Now()))
	}
	// On-air keys, and commits what the radio measured.
	out = p.Emit(context.Background(), emission("air"), dev, ledger, Policy{Mode: config.TXOnAir}, 17)
	if !out.Sent || out.Shadow || dev.transmits != 1 || out.PowerDBm != 17 || out.Airtime != 100*time.Millisecond {
		t.Fatalf("on-air: %+v, transmits %d", out, dev.transmits)
	}
	// No radio, no ledger: refused as radio-down, never as sent.
	if out := p.Emit(context.Background(), emission("down"), nil, ledger, Policy{Mode: config.TXOnAir}, 10); out.Dropped != "radio-down" {
		t.Fatalf("radio-down: %+v", out)
	}
}

func TestDutyWaitsAreBoundedAndACancelledWaitIsNamed(t *testing.T) {
	p := New(Config{SourceKind: "test", Source: "t", DutyWait: 50 * time.Millisecond}, 4)
	dev := &fakeRadio{airtime: time.Second}
	// A ledger already full for the hour: the wait would outlast the
	// patience, and the frame is dropped as duty.
	ledger := radio.NewAirtimeLedger(time.Second, []radio.AirtimeStamp{{At: time.Now(), Airtime: time.Second}})
	if out := p.Emit(context.Background(), emission("saturated"), dev, ledger, Policy{Mode: config.TXOnAir}, 10); out.Dropped != "duty" {
		t.Fatalf("saturated ledger: %+v", out)
	}
	// A wait the owner cancels is its own reason, not a saturated ledger.
	waiting := radio.NewAirtimeLedger(2*time.Second, []radio.AirtimeStamp{{At: time.Now().Add(-59 * time.Minute), Airtime: 2 * time.Second}})
	slow := New(Config{SourceKind: "test", Source: "t", DutyWait: time.Hour}, 4)
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(20 * time.Millisecond); cancel() }()
	if out := slow.Emit(ctx, emission("cancelled"), dev, waiting, Policy{Mode: config.TXOnAir}, 10); out.Dropped != "cancelled" {
		t.Fatalf("cancelled wait: %+v", out)
	}
	if dev.transmits != 0 {
		t.Error("a refused frame was keyed")
	}
}

func TestTheLBTLadderRequeuesAReceptionAndDropsWhenToldTo(t *testing.T) {
	p := New(Config{SourceKind: "test", Source: "t", LBTBound: 100 * time.Millisecond, LBTRetry: 10 * time.Millisecond}, 4)
	ledger := radio.NewAirtimeLedger(time.Hour, nil)
	policy := Policy{Mode: config.TXOnAir, CAD: true, LBTExhausted: config.LBTDrop}

	// A reception in progress sends the frame back to the queue, paced,
	// with its busy spell started.
	dev := &fakeRadio{airtime: 10 * time.Millisecond, assessErr: radio.ErrBusyReceiving}
	out := p.Emit(context.Background(), emission("busy"), dev, ledger, policy, 10)
	if !out.Requeued || p.Queue.Len() != 1 {
		t.Fatalf("reception in progress: %+v, queue %d", out, p.Queue.Len())
	}
	item, ok := p.Queue.TakeUntil(context.Background(), time.Now().Add(time.Second))
	if !ok || item.BusySince.IsZero() || !item.NotBefore.After(time.Now().Add(-time.Second)) {
		t.Fatalf("requeued item = %+v", item)
	}
	// Past the bound, the exhausted policy decides: drop.
	item.BusySince = time.Now().Add(-time.Second)
	if out := p.Emit(context.Background(), item, dev, ledger, policy, 10); out.Dropped != "lbt" {
		t.Fatalf("exhausted reception: %+v", out)
	}
	// A busy verdict retries in place until the bound, then drops.
	busy := &fakeRadio{airtime: 10 * time.Millisecond, busy: true}
	if out := p.Emit(context.Background(), emission("channel-busy"), busy, ledger, policy, 10); out.Dropped != "lbt" || busy.assesses < 2 {
		t.Fatalf("busy channel: %+v, assessments %d", out, busy.assesses)
	}
	// ...or transmits anyway when the site chose the mesh's convention.
	transmit := policy
	transmit.LBTExhausted = config.LBTTransmit
	if out := p.Emit(context.Background(), emission("anyway"), busy, ledger, transmit, 10); !out.Sent || busy.transmits != 1 {
		t.Fatalf("busy channel, transmit anyway: %+v, transmits %d", out, busy.transmits)
	}
	// A failing assessment is its own drop.
	broken := &fakeRadio{airtime: 10 * time.Millisecond, assessErr: errors.New("cad failed")}
	if out := p.Emit(context.Background(), emission("broken"), broken, ledger, policy, 10); out.Dropped != "lbt-failed" {
		t.Fatalf("failing CAD: %+v", out)
	}
	if ledger.Usage(time.Now()) != 10*time.Millisecond {
		t.Errorf("ledger usage %s: only the frame sent anyway should have spent", ledger.Usage(time.Now()))
	}
}

func TestAnExpiredEmissionIsDroppedByNameAndNeverHeldForDuty(t *testing.T) {
	dev := &fakeRadio{airtime: time.Second}
	// Already past its moment when its turn comes: dropped as expired,
	// before any duty is asked for.
	p := New(Config{SourceKind: "test", Source: "t", DutyWait: time.Hour}, 4)
	stale := emission("stale")
	stale.Expires = time.Now().Add(-time.Millisecond)
	free := radio.NewAirtimeLedger(time.Hour, nil)
	if out := p.Emit(context.Background(), stale, dev, free, Policy{Mode: config.TXOnAir}, 10); out.Dropped != "expired" {
		t.Fatalf("stale frame: %+v", out)
	}
	// A budget that frees only after the expiry: the pipeline's own
	// hour of patience does not apply, the wait is cut at the expiry,
	// and the drop is named for what ended it.
	waiting := radio.NewAirtimeLedger(2*time.Second, []radio.AirtimeStamp{{At: time.Now().Add(-59 * time.Minute), Airtime: 2 * time.Second}})
	soon := emission("soon")
	soon.Expires = time.Now().Add(20 * time.Millisecond)
	start := time.Now()
	if out := p.Emit(context.Background(), soon, dev, waiting, Policy{Mode: config.TXOnAir}, 10); out.Dropped != "expired" {
		t.Fatalf("expiring wait: %+v", out)
	}
	if waited := time.Since(start); waited > time.Second {
		t.Fatalf("the wait outlived the expiry: %v", waited)
	}
	// A frame with no expiry keeps the old contract: it is the ledger
	// that refuses it, under the ledger's name.
	brief := New(Config{SourceKind: "test", Source: "t", DutyWait: 50 * time.Millisecond}, 4)
	if out := brief.Emit(context.Background(), emission("patient"), dev,
		radio.NewAirtimeLedger(time.Second, []radio.AirtimeStamp{{At: time.Now(), Airtime: time.Second}}),
		Policy{Mode: config.TXOnAir}, 10); out.Dropped != "duty" {
		t.Fatalf("no expiry: %+v", out)
	}
	// Requeueing past the expiry drops instead of taking a turn.
	late := emission("late")
	late.Expires = time.Now().Add(time.Second)
	late.NotBefore = late.Expires.Add(time.Millisecond)
	if out := p.Requeue(late); out.Dropped != "expired" || p.Queue.Len() != 0 {
		t.Fatalf("late requeue: %+v, backlog %d", out, p.Queue.Len())
	}
	if dev.transmits != 0 {
		t.Error("a refused frame was keyed")
	}
}

// delayedRadio models time spent outside the pipeline, including a
// driver that returns a CAD result after the deadline it was given.
type delayedRadio struct {
	fakeRadio

	airtimeDelay, assessDelay time.Duration
	assessDeadline            time.Time
}

func (r *delayedRadio) Airtime(size int) time.Duration {
	time.Sleep(r.airtimeDelay)
	return r.fakeRadio.Airtime(size)
}

func (r *delayedRadio) AssessChannel(ctx context.Context, threshold float64) (bool, error) {
	r.assessDeadline, _ = ctx.Deadline()
	time.Sleep(r.assessDelay)
	return r.fakeRadio.AssessChannel(ctx, threshold)
}

func TestExpiryDuringPreparationNeverKeysOrSpendsDuty(t *testing.T) {
	for _, mode := range []string{config.TXOnAir, config.TXShadow} {
		for _, phase := range []string{"airtime", "clear", "busy", "receiving", "cad-error"} {
			t.Run(mode+"/"+phase, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					b := bus.New()
					sub := b.Subscribe(2)
					defer sub.Close()
					p := New(Config{Bus: b}, 1)
					dev := &delayedRadio{fakeRadio: fakeRadio{airtime: time.Millisecond}}
					policy := Policy{Mode: mode, CAD: phase != "airtime"}
					if phase == "airtime" {
						dev.airtimeDelay = 25 * time.Millisecond
					} else {
						dev.assessDelay = 25 * time.Millisecond
						dev.busy = phase == "busy"
						switch phase {
						case "receiving":
							dev.assessErr = radio.ErrBusyReceiving
						case "cad-error":
							dev.assessErr = context.DeadlineExceeded
						}
					}
					ledger := radio.NewAirtimeLedger(time.Millisecond, nil)
					item := emission("expiring-answer")
					item.Expires = time.Now().Add(10 * time.Millisecond)
					out := p.Emit(t.Context(), item, dev, ledger, policy, 10)
					if out.Dropped != "expired" || out.Sent || out.Requeued || dev.transmits != 0 {
						t.Fatalf("expired during %s: %+v, transmits=%d", phase, out, dev.transmits)
					}
					if policy.CAD && !dev.assessDeadline.Equal(item.Expires) {
						t.Errorf("CAD deadline=%s, want expiry=%s", dev.assessDeadline, item.Expires)
					}
					if event, ok := (<-sub.C).(bus.TxDropped); !ok || event.Reason != "expired" || len(sub.C) != 0 {
						t.Errorf("expiry event=%+v, unread events=%d", event, len(sub.C))
					}
					if ledger.Usage(time.Now()) != 0 || p.Queue.Len() != 0 {
						t.Error("expiry spent duty or left a queued retry")
					}
					reservation, _, _ := ledger.Reserve(time.Now(), time.Millisecond)
					if reservation == nil {
						t.Fatal("expired frame kept its duty reservation")
					}
					reservation.Cancel()
				})
			})
		}
	}
}

func TestBusyChannelWaitStopsAtExpiry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := New(Config{}, 1)
		dev := &fakeRadio{airtime: time.Millisecond, busy: true}
		ledger := radio.NewAirtimeLedger(time.Millisecond, nil)
		item := emission("answer-on-busy-channel")
		item.Expires = time.Now().Add(50 * time.Millisecond)
		out := p.Emit(t.Context(), item, dev, ledger,
			Policy{Mode: config.TXOnAir, CAD: true, LBTExhausted: config.LBTTransmit}, 10)
		if out.Dropped != "expired" || !time.Now().Equal(item.Expires) || dev.transmits != 0 {
			t.Fatalf("busy channel outlived expiry: %+v, time=%s, transmits=%d", out, time.Now(), dev.transmits)
		}
	})
}
