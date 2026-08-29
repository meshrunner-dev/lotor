//go:build linux

package sx126x

// The receive loop's waiting machinery, without hardware: which phase
// the loop is in decides what may touch the bus, what wakes a sleep,
// and what happens to a transition the edge path missed. The floor
// tracker's own arithmetic is proved in floor_test.go; this file
// proves the loop actually drives it — the seam where a starvation
// model that is never fed reads as a healthy site.

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"meshrunner.dev/pkg/lora/sx126x"
)

// fakeDIO1 is a latched IRQ line whose level a test scripts.
type fakeDIO1 struct {
	mu   sync.Mutex
	high bool
	err  error
}

func (p *fakeDIO1) Get() (bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.high, p.err
}
func (p *fakeDIO1) Edges() <-chan struct{} { return nil }
func (p *fakeDIO1) Close() error           { return nil }

func TestStartReceivePassesTheChipsRefusalThrough(t *testing.T) {
	refusal := errors.New("chip stayed in standby")
	if err := newDevice(&fakeChip{startErr: refusal}).StartReceive(); !errors.Is(err, refusal) {
		t.Errorf("StartReceive = %v", err)
	}
	if err := newDevice(&fakeChip{}).StartReceive(); err != nil {
		t.Errorf("StartReceive = %v", err)
	}
}

func TestCollectPhaseTickTouchesTheBusOnceForBoth(t *testing.T) {
	// One tick is one sampling opportunity and one stats refresh; both
	// ride the same wake-up, and both reach the chip.
	c := &fakeChip{rssi: -110, stats: sx126x.Stats{Received: 7, CRCErrors: 2, HeaderErrors: 1}}
	d := newDevice(c)
	tick := time.NewTicker(time.Hour)
	defer tick.Stop()
	ticking := false // the phase must restart a stopped ticker itself
	if err := d.collectPhase(context.Background(), nil, tick, &ticking); err != nil {
		t.Fatal(err)
	}
	if !ticking {
		t.Error("the phase slept without restarting the ticker")
	}
	if c.rssiReads() != 1 {
		t.Errorf("rssi read %d times on one tick", c.rssiReads())
	}
	if s, ok := d.ChipStats(); !ok || s.Received != 7 || s.CRCErrors != 2 || s.HeaderErrors != 1 {
		t.Errorf("ChipStats = %+v, %v — the tick did not cache the counters", s, ok)
	}
}

func TestCollectPhaseEdgeWakesWithoutSampling(t *testing.T) {
	// A frame's IRQ pre-empts the tick: the caller polls again at
	// once, and no idle measurement is taken over a channel that just
	// proved itself busy.
	c := &fakeChip{}
	d := newDevice(c)
	edges := make(chan struct{}, 1)
	edges <- struct{}{}
	tick := time.NewTicker(time.Hour)
	defer tick.Stop()
	ticking := true
	if err := d.collectPhase(context.Background(), edges, tick, &ticking); err != nil {
		t.Fatal(err)
	}
	if c.rssiReads() != 0 {
		t.Error("an edge wake-up measured the floor anyway")
	}
}

func TestCollectPhaseHonoursCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	tick := time.NewTicker(time.Hour)
	defer tick.Stop()
	ticking := true
	err := newDevice(&fakeChip{}).collectPhase(ctx, nil, tick, &ticking)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("collectPhase = %v", err)
	}
}

func TestRestPhaseLevelHighMeansLookAgainOnceThenSleep(t *testing.T) {
	// The level outlives a missed edge: high before the sleep means an
	// IRQ latched since Poll, so the phase returns without sleeping.
	// But a line stuck high must not spin on the GPIO — the second
	// pass skips the level and takes the bounded sleep.
	d := newDevice(&fakeChip{})
	pin := &fakeDIO1{high: true}
	d.dio1 = pin
	rechecked := false
	start := time.Now()
	if err := d.restPhase(context.Background(), time.Hour, &rechecked); err != nil {
		t.Fatal(err)
	}
	if !rechecked {
		t.Error("a high level did not arm the recheck guard")
	}
	if time.Since(start) > time.Second {
		t.Error("a high level slept instead of returning to Poll")
	}
	if err := d.restPhase(context.Background(), 5*time.Millisecond, &rechecked); err != nil {
		t.Fatal(err)
	}
	if rechecked {
		t.Error("the guard survived the sleep — the next rest would skip its level check")
	}
}

func TestRestPhaseSurfacesALevelReadFailure(t *testing.T) {
	fault := errors.New("gpio gave way")
	d := newDevice(&fakeChip{})
	d.dio1 = &fakeDIO1{err: fault}
	rechecked := false
	if err := d.restPhase(context.Background(), time.Hour, &rechecked); !errors.Is(err, fault) {
		t.Errorf("restPhase = %v", err)
	}
}

func TestRestPhaseWakesOnEdgeWatchdogAndCancellation(t *testing.T) {
	// Three of the four ways out of the parked sleep; the fourth — the
	// rest deadline itself — is the bounded timer the level test used.
	c := &fakeChip{events: make(chan struct{}, 1)}
	d := newDevice(c)
	d.dio1 = &fakeDIO1{}

	c.events <- struct{}{}
	rechecked := false
	if err := d.restPhase(context.Background(), time.Hour, &rechecked); err != nil {
		t.Errorf("edge wake-up = %v", err)
	}

	wd := make(chan time.Time, 1)
	wd <- time.Now()
	d.watchdog = wd
	if err := d.restPhase(context.Background(), time.Hour, &rechecked); err != nil {
		t.Errorf("watchdog wake-up = %v", err)
	}
	d.watchdog = nil

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := d.restPhase(ctx, time.Hour, &rechecked); !errors.Is(err, context.Canceled) {
		t.Errorf("cancellation = %v", err)
	}
}

func TestSampleFloorFeedsDenialsNotJustSamples(t *testing.T) {
	// Every blocked path is still an opportunity the tracker must hear
	// about — this is what makes continuous reception age an attempt.
	// A tripped preamble detector, a latched frame, a status read that
	// failed, and an RSSI refusal (own TX, standby) all deny without
	// ever reading a level off a busy channel.
	cases := []struct {
		name      string
		chip      *fakeChip
		rssiReads int
	}{
		{"preamble detected", &fakeChip{inProgress: [2]bool{true, false}}, 0},
		{"frame latched unread", &fakeChip{inProgress: [2]bool{false, true}}, 0},
		{"status read failed", &fakeChip{ripErr: errors.New("busy pin stuck")}, 0},
		{"rssi refused", &fakeChip{rssiErr: sx126x.ErrNotReceiving}, 1},
	}
	for _, tc := range cases {
		d := newDevice(tc.chip)
		d.sampleFloor()
		if d.floor.attempt.IsZero() {
			t.Errorf("%s: no attempt opened — this starvation would never be counted", tc.name)
		}
		if got := tc.chip.rssiReads(); got != tc.rssiReads {
			t.Errorf("%s: %d rssi reads, want %d", tc.name, got, tc.rssiReads)
		}
		if len(d.floor.samples) != 0 {
			t.Errorf("%s: a denied opportunity contributed a sample", tc.name)
		}
	}
}

func TestRefreshStatsPacesTheBusAndKeepsTheLastGoodRead(t *testing.T) {
	c := &fakeChip{stats: sx126x.Stats{Received: 3}}
	d := newDevice(c)
	d.refreshStats()
	d.refreshStats()
	if c.statsCalls != 1 {
		t.Errorf("%d bus reads inside one pacing window", c.statsCalls)
	}
	if s, ok := d.ChipStats(); !ok || s.Received != 3 {
		t.Errorf("ChipStats = %+v, %v", s, ok)
	}
	// A failed refresh keeps the cache: diagnostics go stale, they do
	// not vanish. Poll owns surfacing a bus that is truly sick.
	d.statsRead = time.Time{}
	c.statsErr = errors.New("bus gave way")
	d.refreshStats()
	if s, ok := d.ChipStats(); !ok || s.Received != 3 {
		t.Errorf("ChipStats after a failed refresh = %+v, %v", s, ok)
	}
}

func TestReceiveIsBimodal(t *testing.T) {
	// The whole loop, both phases: while a batch collects, the tick
	// paces measurements; once it converges, the wait is purely
	// edge-driven — not one bus access for the floor — until a frame's
	// IRQ wakes it. This is the integration RAD-005's counter depends
	// on: a loop that never fed the tracker at rest, or kept measuring
	// through it, would skew both the floor and the starvation story.
	c := &fakeChip{events: make(chan struct{}, 1), rssi: -110}
	d := newDevice(c)
	d.dio1 = &fakeDIO1{}
	// One sample short of convergence, so the first tick publishes.
	now := time.Now()
	d.floor.attempt = now
	for range floorSamples - 1 {
		d.floor.samples = append(d.floor.samples, -110)
	}

	type result struct {
		payload int
		err     error
	}
	got := make(chan result, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	go func() {
		f, err := d.Receive(ctx)
		got <- result{len(f.Payload), err}
	}()

	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, ok := d.NoiseFloor(); ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the collect phase never converged the batch")
		}
		time.Sleep(time.Millisecond)
	}
	reads := c.rssiReads()

	// At rest nothing is clocked: give the loop time to misbehave.
	time.Sleep(100 * time.Millisecond)
	if c.rssiReads() != reads {
		t.Error("the rest phase kept measuring the floor")
	}

	c.mu.Lock()
	c.pollFrame = &sx126x.RxFrame{Payload: []byte{1, 2, 3}}
	c.mu.Unlock()
	c.events <- struct{}{}
	select {
	case r := <-got:
		if r.err != nil || r.payload != 3 {
			t.Errorf("Receive = %d bytes, %v", r.payload, r.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the edge did not wake the rest phase")
	}
	if nf, ok := d.NoiseFloor(); !ok || nf.DBm != -110 {
		t.Errorf("NoiseFloor = %+v, %v", nf, ok)
	}
	if d.NoiseStarved() != 0 {
		t.Errorf("NoiseStarved = %d on an idle channel", d.NoiseStarved())
	}
}
