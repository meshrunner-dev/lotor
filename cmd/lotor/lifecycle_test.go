package main

// The manager's lifecycle transactions, driven end to end against an
// in-memory store and the real assembly chain: what persists must be
// running, what is refused must leave store, file and relay exactly
// as they were. The radio devices never open on a test box — the
// sessions live in the error/retry state, which the supervisor's own
// suite covers; here the subject is the transaction around them.

import (
	"context"
	"strings"
	"testing"
	"time"

	"go.uber.org/zap"

	"meshrunner.dev/lotor/internal/bus"
	"meshrunner.dev/lotor/internal/confdb"
	"meshrunner.dev/lotor/internal/config"
)

// lifecycleManager builds a manager over sampleFile and an in-memory
// store seeded with it, ready to start relays for real.
func lifecycleManager(t *testing.T) *manager {
	t.Helper()
	f := sampleFile()
	store, err := confdb.Open(context.Background(), confdb.Memory)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if err := store.ImportFile(context.Background(), f, "test"); err != nil {
		t.Fatal(err)
	}
	m := newManager(store, f, bus.New(), nil, buildKinds(), zap.NewNop())
	ctx, cancel := context.WithCancel(context.Background())
	m.ctx = ctx
	m.air = make(chan airOrder, 4)
	t.Cleanup(func() {
		m.mu.Lock()
		for name := range m.running {
			m.stopRelay(name)
		}
		for name := range m.observers {
			m.stopObserver(name)
		}
		m.mu.Unlock()
		cancel()
		m.wg.Wait()
	})
	return m
}

func TestApplyTypedPersistsAndBounces(t *testing.T) {
	m := lifecycleManager(t)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.startRelay(m.ctx, "meshcore-868")
	before := m.running["meshcore-868"]
	if before == nil {
		t.Fatal("relay did not start")
	}

	msg, err := m.applyTyped(context.Background(), confdb.KindRelay, "meshcore-868",
		map[string]any{"node_name": "new name"}, nil, "test", "set")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(msg, "restarting") {
		t.Errorf("msg = %q", msg)
	}
	if m.running["meshcore-868"] == before {
		t.Error("the relay was not bounced")
	}
	// The store holds the change: a restart would read it back.
	persisted, err := m.store.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	got := persisted.Relays["meshcore-868"].Layered.Overrides["eu-868-narrow"]["node_name"]
	if got != "new name" {
		t.Errorf("store holds node_name %v", got)
	}
	// And the view followed the successor.
	m.viewMu.RLock()
	defer m.viewMu.RUnlock()
	if m.cfgs["meshcore-868"]["node_name"] != "new name" {
		t.Errorf("view holds %v", m.cfgs["meshcore-868"]["node_name"])
	}
}

func TestApplyTypedRefusalLeavesEverythingIntact(t *testing.T) {
	m := lifecycleManager(t)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.startRelay(m.ctx, "meshcore-868")
	before := m.running["meshcore-868"]
	revs, err := m.store.Revisions(context.Background(), 100)
	if err != nil {
		t.Fatal(err)
	}

	// on-air without an identity: the preflight refuses before the
	// store hears about it, and the running relay never notices.
	_, err = m.applyTyped(context.Background(), confdb.KindRelay, "meshcore-868",
		map[string]any{"tx.mode": config.TXOnAir}, nil, "test", "set")
	if err == nil || !strings.Contains(err.Error(), "identity") {
		t.Fatalf("refusal = %v", err)
	}
	if m.running["meshcore-868"] != before {
		t.Error("a refused mutation bounced the relay")
	}
	if m.file.Relays["meshcore-868"].TX != nil {
		t.Error("the refused gate landed in the live file")
	}
	after, err := m.store.Revisions(context.Background(), 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != len(revs) {
		t.Errorf("revisions %d -> %d — a refusal wrote history", len(revs), len(after))
	}
	persisted, err := m.store.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if persisted.Relays["meshcore-868"].TX != nil {
		t.Error("the refused gate persisted")
	}
}

func TestCreateAndRemoveRunTheWholeTransaction(t *testing.T) {
	m := lifecycleManager(t)

	// A radio first — one owner per radio — then the relay on it.
	if _, err := m.Create(context.Background(), confdb.KindRadio, "slot2",
		map[string]string{"driver": "sx126x-spi", "profile": "rak6421-13300x-slot1",
			"spi": "/dev/spidev0.1"}, "test"); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Create(context.Background(), confdb.KindRelay, "mc2",
		map[string]string{"protocol": "meshcore", "radio": "slot2",
			"profile": "eu-868-narrow", "node_name": "second"}, "test"); err != nil {
		t.Fatal(err)
	}
	m.mu.Lock()
	_, live := m.running["mc2"]
	m.mu.Unlock()
	if !live {
		t.Fatal("created relay is not running")
	}
	persisted, err := m.store.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := persisted.Relays["mc2"]; !ok {
		t.Fatal("created relay did not persist")
	}

	// Remove tears it down everywhere: runtime, views, store.
	if _, err := m.Remove(context.Background(), confdb.KindRelay, "mc2", "test"); err != nil {
		t.Fatal(err)
	}
	m.mu.Lock()
	_, live = m.running["mc2"]
	m.mu.Unlock()
	if live {
		t.Error("removed relay still runs")
	}
	m.viewMu.RLock()
	_, view := m.infos["mc2"]
	m.viewMu.RUnlock()
	if view {
		t.Error("removed relay still has a view")
	}
	persisted, err = m.store.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := persisted.Relays["mc2"]; ok {
		t.Error("removed relay still persisted")
	}
	// The radio is unclaimed again and removable.
	if _, err := m.Remove(context.Background(), confdb.KindRadio, "slot2", "test"); err != nil {
		t.Fatal(err)
	}
}

func TestCreateRefusalIsInvisible(t *testing.T) {
	m := lifecycleManager(t)
	// A relay on a claimed radio: refused by validation, and neither
	// the store nor the runtime ever hear of it.
	_, err := m.Create(context.Background(), confdb.KindRelay, "mc2",
		map[string]string{"protocol": "meshcore", "radio": "slot1",
			"profile": "eu-868-narrow"}, "test")
	if err == nil {
		t.Fatal("two relays share one radio")
	}
	m.mu.Lock()
	_, live := m.running["mc2"]
	_, inFile := m.file.Relays["mc2"]
	m.mu.Unlock()
	if live || inFile {
		t.Error("a refused creation left traces")
	}
	persisted, err := m.store.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := persisted.Relays["mc2"]; ok {
		t.Error("a refused creation persisted")
	}
}

func TestManagerStartBringsUpTheTree(t *testing.T) {
	f := sampleFile()
	f.MQTT = map[string]config.MQTT{
		"obs": {Layered: config.Layered{Overrides: map[string]map[string]any{
			config.CustomProfile: {"url": "tcp://127.0.0.1:1", "iata": "PAR"},
		}}},
	}
	store, err := confdb.Open(context.Background(), confdb.Memory)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	m := newManager(store, f, bus.New(), nil, buildKinds(), zap.NewNop())
	ctx, cancel := context.WithCancel(context.Background())
	m.Start(ctx)
	t.Cleanup(func() {
		cancel()
		waited := make(chan struct{})
		go func() { defer close(waited); m.Wait() }()
		select {
		case <-waited:
		case <-time.After(5 * time.Second):
			t.Error("the manager's goroutines did not drain on cancel")
		}
	})

	m.mu.Lock()
	defer m.mu.Unlock()
	if _, live := m.running["meshcore-868"]; !live {
		t.Error("Start did not launch the relay")
	}
	// The observer cannot come up before the relay face it needs, but
	// its refusal must be a recorded cause, not silence.
	_, obsLive := m.observers["obs"]
	if !obsLive && m.obsCause["obs"] == "" {
		t.Error("observer neither runs nor says why not")
	}
	m.viewMu.RLock()
	defer m.viewMu.RUnlock()
	if _, ok := m.infos["meshcore-868"]; !ok {
		t.Error("Start left no relay view")
	}
}
