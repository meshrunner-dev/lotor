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
