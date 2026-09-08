package meshcore

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"meshrunner.dev/lotor/internal/station"
	"meshrunner.dev/pkg/meshcore/companion"
)

func TestStationShutdownJoinsCommandsFromReplacedClients(t *testing.T) {
	store := &blockingStationState{started: make(chan struct{}), release: make(chan struct{})}
	var release sync.Once
	defer release.Do(func() { close(store.release) })
	spec := testSpec(t)
	spec.State = store
	built, err := build(spec)
	if err != nil {
		t.Fatal(err)
	}
	svc := requireService(t, built)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- svc.Run(ctx) }()
	addr := awaitListener(t, svc)
	first, err := (&net.Dialer{}).DialContext(t.Context(), "tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = first.Close() }()
	payload, err := companion.MarshalCommand(companion.SetAdvertName{Name: "Persisting"})
	if err != nil {
		t.Fatal(err)
	}
	if err := companion.WriteFrame(first, companion.ToDevice, payload); err != nil {
		t.Fatal(err)
	}
	select {
	case <-store.started:
	case <-time.After(time.Second):
		t.Fatal("the first client's persistence did not start")
	}
	second, err := (&net.Dialer{}).DialContext(t.Context(), "tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = second.Close() }()
	deadline := time.Now().Add(time.Second)
	for {
		svc.mu.Lock()
		replaced := svc.generation == 2
		svc.mu.Unlock()
		if replaced {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the second client did not replace the first")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	select {
	case err := <-done:
		t.Fatalf("Run returned while the replaced client's command was still persisting: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	release.Do(func() { close(store.release) })
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run did not return after the outstanding command completed")
	}
	if svc.Info().State != station.StateStopped {
		t.Fatal("the joined station did not publish its stopped state")
	}
}

func TestStationListenerFailureCancelsItsWorkers(t *testing.T) {
	built, err := build(testSpec(t))
	if err != nil {
		t.Fatal(err)
	}
	svc := requireService(t, built)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- svc.Run(ctx) }()
	_ = awaitListener(t, svc)
	svc.mu.Lock()
	listener := svc.listener
	svc.mu.Unlock()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("a failed listener was reported as an ordinary shutdown")
		}
	case <-time.After(time.Second):
		t.Fatal("a listener failure left Run waiting for its workers")
	}
	if ctx.Err() != nil || svc.Info().State != station.StateError {
		t.Fatal("a listener failure lost its lifecycle error")
	}
}
