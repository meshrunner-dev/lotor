package sx126x

import (
	"testing"
	"time"
)

func TestFloorConvergesOnFirstBatch(t *testing.T) {
	tr := &floorTracker{}
	now := time.Now()
	if _, ok := tr.value(); ok {
		t.Fatal("floor known before any sample")
	}
	for range floorSamples {
		tr.sample(-105, now)
	}
	nf, ok := tr.value()
	if !ok || nf.DBm != -105 {
		t.Fatalf("floor = %v %v, want -105", nf.DBm, ok)
	}
	if !nf.At.Equal(now) {
		t.Errorf("floor dated %v, want %v", nf.At, now)
	}
}

func TestFloorGateRejectsTraffic(t *testing.T) {
	// Once a floor is known, a strong carrier the preamble detector
	// missed must not drag it up.
	tr := &floorTracker{}
	now := time.Now()
	for range floorSamples {
		tr.sample(-105, now)
	}
	later := now.Add(floorRestEvery)
	for range floorSamples {
		tr.sample(-60, later) // all rejected: above floor + gate
	}
	for range floorSamples {
		tr.sample(-103, later) // accepted: within the gate
	}
	nf, _ := tr.value()
	if nf.DBm != -103 {
		t.Fatalf("floor = %v, want -103 (the -60 carrier must not count)", nf.DBm)
	}
}

func TestFloorClampsBelow(t *testing.T) {
	tr := &floorTracker{}
	now := time.Now()
	for range floorSamples {
		tr.sample(-140, now)
	}
	if nf, _ := tr.value(); nf.DBm != floorClampDBm {
		t.Fatalf("floor = %v, want the %d clamp", nf.DBm, floorClampDBm)
	}
}

func TestFloorRestsBetweenBatches(t *testing.T) {
	tr := &floorTracker{}
	now := time.Now()
	for range floorSamples {
		tr.sample(-105, now)
	}
	// Immediately after a batch, new samples must not start another —
	// -100 passes the gate, only the rest holds it back.
	for range floorSamples {
		tr.sample(-100, now.Add(time.Millisecond))
	}
	if nf, _ := tr.value(); nf.DBm != -105 {
		t.Fatalf("floor = %v, refreshed before the rest elapsed", nf.DBm)
	}
	// After the rest, they do.
	later := now.Add(floorRestEvery + time.Millisecond)
	for range floorSamples {
		tr.sample(-100, later)
	}
	if nf, _ := tr.value(); nf.DBm != -100 {
		t.Fatalf("floor = %v, want -100 after the rest", nf.DBm)
	}
}
