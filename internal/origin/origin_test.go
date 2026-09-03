package origin

import (
	"context"
	"errors"
	"testing"
	"time"

	"meshrunner.dev/lotor/internal/config"
	"meshrunner.dev/lotor/internal/correlation"
	"meshrunner.dev/lotor/internal/radio"
)

// fakeRadio is a device that counts what the pipeline asked of it.
type fakeRadio struct {
	transmits, assesses int
	assessErr           error
	busy                bool
	airtime             time.Duration
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
	return radio.TxReport{At: time.Now(), Airtime: r.airtime, PowerDBm: power}, nil
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
