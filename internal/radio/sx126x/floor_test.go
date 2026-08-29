package sx126x

import (
	"testing"
	"time"
)

func feed(t *floorTracker, at time.Time, rssi float64, n int) {
	for range n {
		t.sample(rssi, at)
	}
}

func TestFloorConvergesOnFirstBatch(t *testing.T) {
	tr := &floorTracker{}
	now := time.Now()
	if _, ok := tr.value(); ok {
		t.Fatal("floor known before any sample")
	}
	for i := range floorSamples {
		if got := tr.sample(-105, now); got != (i == floorSamples-1) {
			t.Fatalf("sample %d reported converged = %v", i, got)
		}
	}
	nf, ok := tr.value()
	if !ok || nf.DBm != -105 || nf.SpreadDB != 0 {
		t.Fatalf("floor = %+v, want median -105 spread 0", nf)
	}
	if !nf.At.Equal(now) {
		t.Errorf("floor dated %v, want %v", nf.At, now)
	}
}

func TestFloorMedianShrugsOffImpulses(t *testing.T) {
	// Impulsive bursts inside the gate would drag a mean; the median
	// does not move, and the spread names the impulsiveness instead.
	tr := &floorTracker{}
	now := time.Now()
	feed(tr, now, -105, floorSamples)
	later := now.Add(floorRestEvery)
	feed(tr, later, -105, 50)
	feed(tr, later, -93, 14) // within the gate (-105 + 14 = -91)
	nf, _ := tr.value()
	if nf.DBm != -105 {
		t.Fatalf("median floor = %v, moved by impulses", nf.DBm)
	}
	if nf.SpreadDB != 12 {
		t.Fatalf("spread = %v, want 12 (p90 -93 over median -105)", nf.SpreadDB)
	}
}

func TestFloorAbandonsStaleBatches(t *testing.T) {
	// A starved channel must not let a batch span epochs: past the age
	// bound the partial batch is dropped, counted, and restarted.
	tr := &floorTracker{}
	now := time.Now()
	feed(tr, now, -105, 10)
	later := now.Add(floorBatchMaxAge + time.Second)
	feed(tr, later, -100, floorSamples)
	nf, ok := tr.value()
	if !ok || nf.DBm != -100 {
		t.Fatalf("floor = %+v, want -100 from the fresh batch alone", nf)
	}
	if got := tr.starvedCount(); got != 1 {
		t.Fatalf("starved = %d, want 1", got)
	}
}

func TestFloorPhasesFollowTheTracker(t *testing.T) {
	// The receive loop's two phases derive from tracker state: a fresh
	// tracker collects, a completed batch rests until its deadline.
	tr := &floorTracker{}
	now := time.Now()
	if !tr.collecting(now) {
		t.Fatal("a fresh tracker must want samples")
	}
	feed(tr, now, -105, floorSamples)
	if tr.collecting(now.Add(time.Millisecond)) {
		t.Fatal("a completed batch must rest")
	}
	if left := tr.restLeft(now); left <= 0 || left > floorRestEvery {
		t.Fatalf("restLeft = %v, want within (0, %v]", left, floorRestEvery)
	}
	if !tr.collecting(now.Add(floorRestEvery)) {
		t.Fatal("the rest elapsed: sampling must resume")
	}
	// A partial batch keeps collecting even inside the rest window.
	feed(tr, now.Add(floorRestEvery), -105, 1)
	if !tr.collecting(now.Add(floorRestEvery)) {
		t.Fatal("a partial batch must keep collecting")
	}
}

func TestFloorGateRejectsTraffic(t *testing.T) {
	// Once a floor is known, a strong carrier the preamble detector
	// missed must not enter the batch at all.
	tr := &floorTracker{}
	now := time.Now()
	feed(tr, now, -105, floorSamples)
	later := now.Add(floorRestEvery)
	feed(tr, later, -60, floorSamples) // all rejected: above floor + gate
	feed(tr, later, -103, floorSamples)
	if nf, _ := tr.value(); nf.DBm != -103 {
		t.Fatalf("floor = %v, want -103 (the -60 carrier must not count)", nf.DBm)
	}
}

func TestFloorClampsBelow(t *testing.T) {
	tr := &floorTracker{}
	feed(tr, time.Now(), -140, floorSamples)
	if nf, _ := tr.value(); nf.DBm != floorClampDBm {
		t.Fatalf("floor = %v, want the %d clamp", nf.DBm, floorClampDBm)
	}
}

func TestFloorRestsBetweenBatches(t *testing.T) {
	tr := &floorTracker{}
	now := time.Now()
	feed(tr, now, -105, floorSamples)
	// Immediately after a batch, new samples must not start another —
	// -100 passes the gate, only the rest holds it back.
	feed(tr, now.Add(time.Millisecond), -100, floorSamples)
	if nf, _ := tr.value(); nf.DBm != -105 {
		t.Fatalf("floor = %v, refreshed before the rest elapsed", nf.DBm)
	}
	later := now.Add(floorRestEvery + time.Millisecond)
	feed(tr, later, -100, floorSamples)
	if nf, _ := tr.value(); nf.DBm != -100 {
		t.Fatalf("floor = %v, want -100 after the rest", nf.DBm)
	}
}

func TestStarvationIsCountedWhileItLasts(t *testing.T) {
	// A non-LoRa carrier parked above the gate rejects every sample.
	// The counter exists to report precisely that, so it must move
	// during the starvation — not at the first quiet sample after the
	// trouble is over, when nobody needs telling any more.
	t0 := time.Unix(1_000_000, 0) // a real epoch: the zero time is the tracker's "no attempt" sentinel
	tr := &floorTracker{}
	// Converge a first batch so the gate has a floor to work from.
	for i := range floorSamples {
		tr.sample(-105, t0.Add(time.Duration(i)*sampleEvery))
	}
	if _, ok := tr.value(); !ok {
		t.Fatal("the first batch did not converge")
	}
	// A second batch opens on a quiet sample, once the rest between
	// batches has elapsed, and then the carrier arrives.
	last := t0.Add(time.Duration(floorSamples-1) * sampleEvery)
	at := last.Add(floorRestEvery + time.Second)
	if tr.collecting(at) == false {
		t.Fatalf("the tracker is still resting at %v", at.Sub(t0))
	}
	tr.sample(-104, at)
	if tr.starvedCount() != 0 {
		t.Fatalf("starved = %d before any starvation", tr.starvedCount())
	}
	// Loud samples past the batch's maximum age: the batch is
	// abandoned and counted, while the channel is still occupied.
	at = at.Add(floorBatchMaxAge + time.Second)
	tr.sample(-60, at)
	if tr.starvedCount() != 1 {
		t.Errorf("starved = %d during the starvation, want 1", tr.starvedCount())
	}
	// It keeps counting for as long as the occupation lasts, one
	// abandoned batch at a time rather than once per rejected sample.
	tr.sample(-60, at.Add(time.Second))
	if got := tr.starvedCount(); got != 1 {
		t.Errorf("starved = %d — a rejected sample must not open a batch to abandon", got)
	}
	// And the quiet channel converges again afterwards.
	quiet := at.Add(2 * time.Second)
	for i := range floorSamples {
		tr.sample(-105, quiet.Add(time.Duration(i)*sampleEvery))
	}
	if nf, ok := tr.value(); !ok || nf.DBm > -100 {
		t.Errorf("the floor did not come back: %+v ok=%v", nf, ok)
	}
}

func TestStarvationNeedsNoQuietSampleToBeSeen(t *testing.T) {
	// The review's residuals: a carrier already strong when the
	// window opens used to leave the counter at zero forever — no
	// batch existed to expire — and after one abandoned batch the
	// following windows were never recounted. The attempt model
	// counts every expired window, samples or none.
	t0 := time.Unix(2_000_000, 0)
	tr := &floorTracker{}
	for i := range floorSamples {
		tr.sample(-105, t0.Add(time.Duration(i)*sampleEvery))
	}
	last := t0.Add(time.Duration(floorSamples-1) * sampleEvery)

	// The carrier precedes the first quiet sample of the next window:
	// every opportunity is gated, and the attempt still ages.
	at := last.Add(floorRestEvery + time.Second)
	tr.sample(-60, at) // opens the attempt, gated
	if got := tr.starvedCount(); got != 0 {
		t.Fatalf("starved = %d before any window expired", got)
	}
	at = at.Add(floorBatchMaxAge + time.Second)
	tr.sample(-60, at)
	if got := tr.starvedCount(); got != 1 {
		t.Errorf("starved = %d: a carrier that precedes the first quiet sample is invisible", got)
	}
	// The second expired window counts too: starvation is ongoing.
	at = at.Add(floorBatchMaxAge + time.Second)
	tr.sample(-60, at)
	if got := tr.starvedCount(); got != 2 {
		t.Errorf("second expired window: starved = %d, want 2", got)
	}

	// Continuous LoRa reception never yields a sample at all: the
	// denied opportunities age the attempt the same way.
	busy := at.Add(floorRestEvery + time.Second)
	tr.denied(busy)
	tr.denied(busy.Add(floorBatchMaxAge + time.Second))
	if got := tr.starvedCount(); got != 3 {
		t.Errorf("continuous reception: starved = %d, want 3", got)
	}

	// And the calm channel converges again afterwards.
	quiet := busy.Add(2 * (floorBatchMaxAge + time.Second))
	for i := range floorSamples {
		tr.sample(-105, quiet.Add(time.Duration(i)*sampleEvery))
	}
	if nf, ok := tr.value(); !ok || nf.DBm > -100 {
		t.Errorf("the floor did not come back: %+v ok=%v", nf, ok)
	}
}
