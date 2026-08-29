package sensor

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
	"time"
)

// opens hands the same part back every time, which is what a driver
// whose device is already there behaves like.
func opens(d Device) func() (Device, error) {
	return func() (Device, error) { return d, nil }
}

// fakeDevice stands in for a part on a bus: it says when it was read,
// and it can be made to hang or to fail the way a real one does.
type fakeDevice struct {
	mu      sync.Mutex
	reads   int
	give    []Reading
	err     error
	closed  bool
	entered chan struct{} // signalled as each Read begins
	release chan struct{} // Read waits here when non-nil
}

func (f *fakeDevice) Read(context.Context) ([]Reading, error) {
	f.mu.Lock()
	f.reads++
	give, err, release := f.give, f.err, f.release
	f.mu.Unlock()
	if f.entered != nil {
		f.entered <- struct{}{}
	}
	if release != nil {
		<-release
	}
	return give, err
}

func (f *fakeDevice) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	return nil
}

func (f *fakeDevice) wasClosed() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closed
}

func TestLatestDoesNotWaitForTheBus(t *testing.T) {
	// The property the sampler exists for: a wedged bus must not stop
	// the goroutines that answer questions.
	dev := &fakeDevice{
		entered: make(chan struct{}, 1),
		release: make(chan struct{}),
	}
	s := NewSampler(opens(dev), time.Hour, nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); s.Run(ctx) }()

	<-dev.entered // the read is now in flight and will not return
	answered := make(chan []Reading, 1)
	go func() { answered <- s.Latest() }()
	select {
	case <-answered:
	case <-time.After(2 * time.Second):
		t.Fatal("Latest waited for a read that never returns")
	}

	close(dev.release)
	cancel()
	<-done
}

func TestASampleReachesTheCacheAndAFailureKeepsIt(t *testing.T) {
	at := time.Date(2026, 8, 29, 12, 0, 0, 0, time.UTC)
	dev := &fakeDevice{
		entered: make(chan struct{}, 4),
		give:    []Reading{{Quantity: Voltage, Value: 3.9, At: at}},
	}
	s := NewSampler(opens(dev), time.Millisecond, nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); s.Run(ctx) }()

	<-dev.entered
	waitFor(t, func() bool { return len(s.Latest()) == 1 }, "the first sample never reached the cache")
	if got := s.Latest()[0]; got.Quantity != Voltage || got.Value != 3.9 || !got.At.Equal(at) {
		t.Fatalf("cached %+v", got)
	}

	// A part that stops answering leaves the last reading in place,
	// stamped with the moment it was true.
	dev.mu.Lock()
	dev.give, dev.err = nil, errors.New("bus wedged")
	dev.mu.Unlock()
	<-dev.entered
	<-dev.entered
	if got := s.Latest(); len(got) != 1 || got[0].Value != 3.9 {
		t.Fatalf("a failed read discarded the cache: %+v", got)
	}

	cancel()
	<-done
	if !dev.wasClosed() {
		t.Error("the device was not closed when the context ended")
	}
}

func TestLatestCopiesWhatItGivesBack(t *testing.T) {
	s := NewSampler(opens(&fakeDevice{}), time.Hour, nil)
	s.last = []Reading{{Quantity: Voltage, Value: 3.9}}
	got := s.Latest()
	got[0].Value = 0
	if s.Latest()[0].Value != 3.9 {
		t.Fatal("a caller mutated the cache through Latest")
	}
}

func TestACadenceOfZeroTakesTheDefault(t *testing.T) {
	if s := NewSampler(opens(&fakeDevice{}), 0, nil); s.every != DefaultInterval {
		t.Fatalf("every = %s", s.every)
	}
}

func waitFor(t *testing.T, ok func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if ok() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal(msg)
}

func TestADriverMayRefillItsOwnBuffer(t *testing.T) {
	// A driver that avoids allocating hands back the same slice every
	// time. The cache must not be that slice.
	buf := []Reading{{Quantity: Voltage, Value: 3.9}}
	dev := &fakeDevice{entered: make(chan struct{}, 1), give: buf}
	s := NewSampler(opens(dev), time.Hour, nil)
	_ = s.sample(context.Background(), dev)
	<-dev.entered

	buf[0] = Reading{Quantity: Current, Value: 0.2}
	if got := s.Latest(); got[0].Quantity != Voltage || got[0].Value != 3.9 {
		t.Fatalf("the cache followed the driver's buffer: %+v", got[0])
	}
}

func TestAPartThatRefusesIsTriedAgain(t *testing.T) {
	// The operator fixes a permission, plugs a board in, and the
	// daemon must notice without being restarted.
	dev := &fakeDevice{
		entered: make(chan struct{}, 2),
		give:    []Reading{{Quantity: Voltage, Value: 5.0}},
	}
	var tries atomic.Int32
	open := func() (Device, error) {
		if tries.Add(1) < 2 {
			return nil, errors.New("permission denied")
		}
		return dev, nil
	}
	s := NewSampler(open, time.Hour, nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); s.Run(ctx) }()

	waitFor(t, s.Opened, "the part was never opened after refusing once")
	waitFor(t, func() bool { return len(s.Latest()) == 1 }, "no reading followed the open")
	if n := tries.Load(); n < 2 {
		t.Errorf("opened after %d tries", n)
	}
	cancel()
	<-done
	if s.Opened() {
		t.Error("still open after the context ended")
	}
}

func TestAPartThatNeverOpensStopsWithTheContext(t *testing.T) {
	open := func() (Device, error) { return nil, errors.New("no such bus") }
	s := NewSampler(open, time.Hour, nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); s.Run(ctx) }()
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Run kept retrying after its context ended")
	}
	if s.Opened() {
		t.Error("Opened is true for a part that never opened")
	}
}

func TestTheReasonSurvivesForTheStatusLine(t *testing.T) {
	// A journal on an embedded host rotates; the reason a part is not
	// answering has to be readable now, in the driver's own words.
	open := func() (Device, error) { return nil, errors.New("permission denied") }
	s := NewSampler(open, time.Hour, nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); s.Run(ctx) }()

	waitFor(t, func() bool { return s.Cause() != "" }, "no reason was kept")
	if got := s.Cause(); got != "permission denied" {
		t.Errorf("cause = %q", got)
	}
	if s.Opened() {
		t.Error("Opened is true for a part that refused")
	}
	cancel()
	<-done
}

func TestOpeningClearsTheReason(t *testing.T) {
	dev := &fakeDevice{entered: make(chan struct{}, 2)}
	var tries atomic.Int32
	open := func() (Device, error) {
		if tries.Add(1) < 2 {
			return nil, errors.New("no such bus")
		}
		return dev, nil
	}
	s := NewSampler(open, time.Hour, nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); s.Run(ctx) }()

	waitFor(t, s.Opened, "the part never opened")
	if got := s.Cause(); got != "" {
		t.Errorf("a part that is answering still carries %q", got)
	}
	cancel()
	<-done
}

func TestAPartThatStopsAnsweringIsOpenedAgain(t *testing.T) {
	// A descriptor that outlives its device — a board unplugged, an
	// adapter reset — is not mended by reading harder.
	dev := &fakeDevice{
		entered: make(chan struct{}, 16),
		give:    []Reading{{Quantity: Voltage, Value: 5.0}},
	}
	var opens atomic.Int32
	var gone atomic.Bool
	open := func() (Device, error) {
		opens.Add(1)
		if gone.Load() {
			// The board is not there any more, so neither is the
			// descriptor it would hand back.
			return nil, errors.New("no such device")
		}
		return dev, nil
	}
	s := NewSampler(open, time.Millisecond, nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); s.Run(ctx) }()

	waitFor(t, func() bool { return len(s.Latest()) == 1 }, "the first sample never landed")
	if n := opens.Load(); n != 1 {
		t.Fatalf("opened %d times before anything failed", n)
	}

	// The part goes away. After readFailuresBeforeReopen in a row the
	// sampler gives the descriptor back and asks for another.
	dev.mu.Lock()
	dev.give, dev.err = nil, errors.New("input/output error")
	dev.mu.Unlock()
	gone.Store(true)

	waitFor(t, func() bool { return opens.Load() > 1 }, "a part that stopped answering was never reopened")
	if !dev.wasClosed() {
		t.Error("the dead descriptor was not given back before reopening")
	}
	// It is back in the opening loop, so it says so rather than
	// claiming to run while its readings age.
	waitFor(t, func() bool { return !s.Opened() }, "still claims to be running with a dead part")
	if got := s.Cause(); got != "no such device" {
		t.Errorf("cause = %q, want the refusal's own words", got)
	}
	cancel()
	<-done
}

func TestAnUnchangingReasonIsSaidOnce(t *testing.T) {
	// An absent part would otherwise write an error a minute for as
	// long as it is absent, into a journal that may be in RAM.
	core, seen := observer.New(zapcore.DebugLevel)
	open := func() (Device, error) { return nil, errors.New("no such device") }
	s := NewSampler(open, time.Hour, zap.New(core))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); s.Run(ctx) }()

	waitFor(t, func() bool { return s.Cause() != "" }, "no reason was kept")
	// openBackoff is a second, so several attempts pass in this window
	// and only the first may speak.
	time.Sleep(2500 * time.Millisecond)
	cancel()
	<-done

	if n := seen.FilterMessage("sensor not open").Len(); n != 1 {
		t.Errorf("the same reason was logged %d times, want once", n)
	}
}
