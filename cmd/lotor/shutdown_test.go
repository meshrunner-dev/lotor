package main

import (
	"testing"
	"time"
)

func TestWaitUntilBoundsAStuckShutdownPhase(t *testing.T) {
	blocked := make(chan struct{})
	started := time.Now()
	if waitUntil(started.Add(20*time.Millisecond), func() { <-blocked }) {
		t.Fatal("blocked shutdown phase reported completion")
	}
	if elapsed := time.Since(started); elapsed > 200*time.Millisecond {
		t.Fatalf("shutdown bound took %s", elapsed)
	}
	close(blocked)
}

func TestWaitUntilReportsCompletedPhase(t *testing.T) {
	if !waitUntil(time.Now().Add(time.Second), func() {}) {
		t.Fatal("completed shutdown phase reached the deadline")
	}
}
