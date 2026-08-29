package relay

// The supervisor's lifecycle, walked deterministically: the session
// order, the retry ladder, the joins, and the shutdown races — with a
// fake driver, device and engine, no hardware and no fuzzing. The
// backoff fields are shrunk to milliseconds so the whole ladder fits
// in a test's patience.

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"

	"meshrunner.dev/lotor/internal/bus"
	"meshrunner.dev/lotor/internal/radio"
)

// recorder collects lifecycle calls in order, shared by the fakes.
type recorder struct {
	mu    sync.Mutex
	calls []string
}

func (r *recorder) note(call string) {
	r.mu.Lock()
	r.calls = append(r.calls, call)
	r.mu.Unlock()
}

func (r *recorder) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.calls...)
}

// fakeLifeDevice records the session's touches and blocks Receive
// until cancelled, as an idle radio would.
type fakeLifeDevice struct {
	rec           *recorder
	failConfigure error
	failStart     error
}

func (d *fakeLifeDevice) Envelope() radio.Envelope { return radio.Envelope{} }
func (d *fakeLifeDevice) Configure(radio.Waveform) error {
	d.rec.note("configure")
	return d.failConfigure
}

func (d *fakeLifeDevice) StartReceive() error {
	d.rec.note("start-receive")
	return d.failStart
}

func (d *fakeLifeDevice) Receive(ctx context.Context) (radio.Frame, error) {
	<-ctx.Done()
	return radio.Frame{}, ctx.Err()
}
func (d *fakeLifeDevice) NoiseFloor() (radio.NoiseFloor, bool) { return radio.NoiseFloor{}, false }
func (d *fakeLifeDevice) NoiseStarved() uint64                 { return 0 }
func (d *fakeLifeDevice) ChipStats() (radio.ChipStats, bool)   { return radio.ChipStats{}, false }
func (d *fakeLifeDevice) Airtime(int) time.Duration            { return time.Millisecond }
func (d *fakeLifeDevice) Transmit(context.Context, []byte, int8) (radio.TxReport, error) {
	return radio.TxReport{}, errors.New("not in this test")
}

func (d *fakeLifeDevice) AssessChannel(context.Context, float64) (bool, error) {
	return false, nil
}

func (d *fakeLifeDevice) Close() error {
	d.rec.note("close")
	return nil
}

// fakeLifeEngine runs until cancelled, or fails per session from a
// scripted list — one error per session, nil meaning run clean.
type fakeLifeEngine struct {
	rec     *recorder
	mu      sync.Mutex
	script  []error
	session int
}

func (e *fakeLifeEngine) Waveform() radio.Waveform { return radio.Waveform{} }
func (e *fakeLifeEngine) TxPower() (int8, bool)    { return 0, false }
func (e *fakeLifeEngine) Identity() string         { return "" }
func (e *fakeLifeEngine) Run(ctx context.Context, dev radio.Device) error {
	e.rec.note("engine-run")
	e.mu.Lock()
	var fault error
	if e.session < len(e.script) {
		fault = e.script[e.session]
	}
	e.session++
	e.mu.Unlock()
	if fault != nil {
		return fault
	}
	<-ctx.Done()
	return ctx.Err()
}

// lifeRelay builds a relay over the fakes with a millisecond ladder;
// openErrs scripts Open refusals, consumed one per session.
func lifeRelay(t *testing.T, rec *recorder, dev *fakeLifeDevice,
	eng *fakeLifeEngine, openErrs []error,
) (*Relay, *bus.Subscription) {
	t.Helper()
	var mu sync.Mutex
	session := 0
	drv := radio.Driver{Open: func(map[string]any, *zap.Logger) (radio.Device, error) {
		rec.note("open")
		mu.Lock()
		var fault error
		if session < len(openErrs) {
			fault = openErrs[session]
		}
		session++
		mu.Unlock()
		if fault != nil {
			return nil, fault
		}
		return dev, nil
	}}
	b := bus.New()
	sub := b.Subscribe(64)
	t.Cleanup(sub.Close)
	r := New("life", drv, nil, eng, b, zap.NewNop(), nil, "")
	r.backoffFirst, r.backoffCap = time.Millisecond, 4*time.Millisecond
	return r, sub
}

// states drains every RelayState the bus saw so far.
func states(sub *bus.Subscription) []string {
	var out []string
	for {
		select {
		case ev := <-sub.C:
			if s, ok := ev.(bus.RelayState); ok {
				st := s.State
				if s.Err != "" {
					st += ":" + s.Err
				}
				out = append(out, st)
			}
		default:
			return out
		}
	}
}

// awaitState polls the relay until it reports the wanted state.
func awaitState(t *testing.T, r *Relay, want string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if r.State() == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("relay never reached %q (state %q, err %q)", want, r.State(), r.Err())
}

func TestSessionRunsInOrderAndJoins(t *testing.T) {
	rec := &recorder{}
	r, _ := lifeRelay(t, rec, &fakeLifeDevice{rec: rec}, &fakeLifeEngine{rec: rec}, nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); r.Run(ctx) }()

	awaitState(t, r, StateRunning)
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
	want := []string{"open", "configure", "start-receive", "engine-run", "close"}
	got := rec.snapshot()
	if len(got) != len(want) {
		t.Fatalf("calls = %v", got)
	}
	for i, w := range want {
		if got[i] != w {
			t.Fatalf("call %d = %q, want %q (all: %v)", i, got[i], w, got)
		}
	}
}

func TestOpenFailuresRetryUntilTheRadioReturns(t *testing.T) {
	rec := &recorder{}
	openErrs := []error{errors.New("spi absent"), errors.New("spi absent")}
	r, sub := lifeRelay(t, rec, &fakeLifeDevice{rec: rec}, &fakeLifeEngine{rec: rec}, openErrs)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); r.Run(ctx) }()

	awaitState(t, r, StateRunning)
	cancel()
	<-done
	// Three opens: two refused, the third reached running — and the
	// error states carried their cause on the bus.
	opens := 0
	for _, c := range rec.snapshot() {
		if c == "open" {
			opens++
		}
	}
	if opens != 3 {
		t.Errorf("opened %d times, want 3", opens)
	}
	errStates := 0
	for _, s := range states(sub) {
		if s == "error:spi absent" {
			errStates++
		}
	}
	if errStates != 2 {
		t.Errorf("published %d error states, want 2", errStates)
	}
}

func TestConfigureFailureStillClosesTheDevice(t *testing.T) {
	rec := &recorder{}
	dev := &fakeLifeDevice{rec: rec, failConfigure: errors.New("chip sulks")}
	r, _ := lifeRelay(t, rec, dev, &fakeLifeEngine{rec: rec}, nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); r.Run(ctx) }()

	awaitState(t, r, StateError)
	cancel()
	<-done
	got := rec.snapshot()
	if len(got) < 3 || got[0] != "open" || got[1] != "configure" || got[2] != "close" {
		t.Fatalf("calls = %v — an opened device must close on any exit", got)
	}
}

func TestEngineFaultRetriesAndTheLadderResets(t *testing.T) {
	rec := &recorder{}
	eng := &fakeLifeEngine{rec: rec, script: []error{errors.New("engine hiccup")}}
	r, sub := lifeRelay(t, rec, &fakeLifeDevice{rec: rec}, eng, nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); r.Run(ctx) }()

	// First session faults, second runs clean.
	awaitState(t, r, StateRunning)
	for {
		if eng.mu.Lock(); eng.session >= 2 {
			eng.mu.Unlock()
			break
		}
		eng.mu.Unlock()
		time.Sleep(time.Millisecond)
	}
	cancel()
	<-done
	var sawFault, ranAgain bool
	for _, s := range states(sub) {
		if s == "error:engine hiccup" {
			sawFault = true
		}
		if sawFault && s == "running" {
			ranAgain = true
		}
	}
	if !sawFault || !ranAgain {
		t.Errorf("states = %v — want a fault, then running again", states(sub))
	}
}

func TestShutdownDuringBackoffReturnsPromptly(t *testing.T) {
	rec := &recorder{}
	// Every open refuses: the supervisor lives in the backoff wait.
	openErrs := []error{errors.New("dead"), errors.New("dead"), errors.New("dead")}
	r, _ := lifeRelay(t, rec, &fakeLifeDevice{rec: rec}, &fakeLifeEngine{rec: rec}, openErrs)
	r.backoffFirst, r.backoffCap = time.Hour, time.Hour // park it there
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); r.Run(ctx) }()

	awaitState(t, r, StateError)
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run sat out its hour of backoff through a shutdown")
	}
}

func TestFaultRacingShutdownKeepsItsErrorState(t *testing.T) {
	rec := &recorder{}
	// The engine returns a real fault the moment it runs; the test
	// cancels first, so Run sees a dead context alongside the fault.
	eng := &fakeLifeEngine{rec: rec, script: []error{errors.New("radio died")}}
	fault := make(chan struct{})
	drv := radio.Driver{Open: func(map[string]any, *zap.Logger) (radio.Device, error) {
		<-fault // hold the session until the shutdown is already ordered
		return &fakeLifeDevice{rec: rec}, nil
	}}
	b := bus.New()
	r := New("life", drv, nil, eng, b, zap.NewNop(), nil, "")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); r.Run(ctx) }()
	cancel()
	close(fault)
	<-done
	if r.State() != StateError || r.Err() != "radio died" {
		t.Errorf("state %q err %q — a fault racing shutdown was read as a clean exit",
			r.State(), r.Err())
	}
}

func TestStillbornPublishesAndWaits(t *testing.T) {
	b := bus.New()
	sub := b.Subscribe(4)
	defer sub.Close()
	r := Stillborn("corpse", errors.New("config refused"), b, zap.NewNop())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); r.Run(ctx) }()
	cancel()
	<-done
	if r.State() != StateError || r.Err() != "config refused" {
		t.Errorf("state %q err %q", r.State(), r.Err())
	}
	if got := states(sub); len(got) != 1 || got[0] != "error:config refused" {
		t.Errorf("states = %v", got)
	}
}
