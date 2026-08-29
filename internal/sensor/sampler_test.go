package sensor

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

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
	s := NewSampler(dev, time.Hour, nil)
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
	s := NewSampler(dev, time.Millisecond, nil)
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
	s := NewSampler(&fakeDevice{}, time.Hour, nil)
	s.last = []Reading{{Quantity: Voltage, Value: 3.9}}
	got := s.Latest()
	got[0].Value = 0
	if s.Latest()[0].Value != 3.9 {
		t.Fatal("a caller mutated the cache through Latest")
	}
}

func TestACadenceOfZeroTakesTheDefault(t *testing.T) {
	if s := NewSampler(&fakeDevice{}, 0, nil); s.every != DefaultInterval {
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
	s := NewSampler(dev, time.Hour, nil)
	s.sample(context.Background())
	<-dev.entered

	buf[0] = Reading{Quantity: Current, Value: 0.2}
	if got := s.Latest(); got[0].Quantity != Voltage || got[0].Value != 3.9 {
		t.Fatalf("the cache followed the driver's buffer: %+v", got[0])
	}
}
