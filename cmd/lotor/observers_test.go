package main

// The observers-follow-topology contract: an empty relay= means "the
// only relay", a phrase whose meaning changes when relays are created
// or removed — and an explicit relay= is a claim, refused like a
// relay's claim on a radio. Each scenario asserts the running state
// equals what a restart would produce, which is the whole point.

import (
	"context"
	"strings"
	"testing"

	"go.uber.org/zap"

	"meshrunner.dev/lotor/internal/bus"
	"meshrunner.dev/lotor/internal/cli"
	"meshrunner.dev/lotor/internal/confdb"
	"meshrunner.dev/lotor/internal/config"
)

// observerFile builds a topology of n relays (each with its own
// radio, per the single-owner rule) and one observer; relay names an
// explicit target, empty stays implicit.
func observerFile(n int, relay string) *config.File {
	f := &config.File{
		Radios: map[string]config.Radio{},
		Relays: map[string]config.Relay{},
		MQTT: map[string]config.MQTT{
			"obs": {Layered: config.Layered{Overrides: map[string]map[string]any{
				config.CustomProfile: {
					"url": "tcp://127.0.0.1:1", "iata": "PAR", "relay": relay,
				},
			}}},
		},
	}
	names := []string{"mc-a", "mc-b"}[:n]
	slots := []string{"slot1", "slot2"}[:n]
	for i, name := range names {
		f.Radios[slots[i]] = config.Radio{Driver: "sx126x-spi", Layered: config.Layered{
			Profile: "rak6421-13300x-slot1",
			Overrides: map[string]map[string]any{
				"rak6421-13300x-slot1": {"spi": "/dev/spidev0." + string(rune('0'+i))},
			},
		}}
		f.Relays[name] = config.Relay{Protocol: "meshcore", Radio: slots[i],
			Layered: config.Layered{Profile: "eu-868-narrow"}}
	}
	return f
}

// seedStore persists the file's relays so Remove has rows to delete.
func seedStore(t *testing.T, m *manager, store *confdb.Store) {
	t.Helper()
	for name := range m.file.Relays {
		section, err := objectSection(m.file, confdb.KindRelay, name)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.Replace(context.Background(), confdb.KindRelay, name,
			section, "test", "add", nil); err != nil {
			t.Fatal(err)
		}
	}
}

// observerManager wires just enough of a manager for the observer
// lifecycle: live relay faces with identities, and a broker URL that
// dials nothing (paho retries in the background, which is exactly the
// connecting state).
func observerManager(t *testing.T, f *config.File) *manager {
	t.Helper()
	m := &manager{
		file: f, bus: bus.New(), log: zap.NewNop(),
		running:   map[string]*managedRelay{},
		observers: map[string]*managedObserver{},
		obsCause:  map[string]string{},
		infos:     map[string]cli.RelayInfo{},
		radios:    map[string]cli.RadioInfo{},
		traces:    map[string][]config.Trace{},
		cfgs:      map[string]map[string]any{},
	}
	for name := range f.Relays {
		m.infos[name] = cli.RelayInfo{
			Name: name, Driver: "sx126x-spi", NodeName: name,
			Identity: strings.Repeat("ab", 32),
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	m.ctx = ctx
	t.Cleanup(func() {
		m.mu.Lock()
		for name := range m.observers {
			m.stopObserver(name)
		}
		m.mu.Unlock()
		cancel()
	})
	return m
}

func TestObserverStopsWhenASecondRelayAppears(t *testing.T) {
	// One relay, implicit observer: running. A second relay makes
	// "the only relay" unanswerable — a restart would refuse to start
	// the observer, so the reconciliation stops it now.
	m := observerManager(t, observerFile(1, ""))
	m.mu.Lock()
	defer m.mu.Unlock()
	m.startObserver(m.ctx, "obs")
	if _, live := m.observers["obs"]; !live {
		t.Fatalf("observer did not start: %v", m.obsCause["obs"])
	}

	two := observerFile(2, "")
	m.file.Relays["mc-b"] = two.Relays["mc-b"]
	m.file.Radios["slot2"] = two.Radios["slot2"]
	m.reconcileObservers()

	if _, live := m.observers["obs"]; live {
		t.Fatal("observer still runs on an ambiguous relay=")
	}
	if cause := m.obsCause["obs"]; !strings.Contains(cause, "relay=") {
		t.Errorf("cause = %q", cause)
	}
}

func TestObserverStopsWithItsImplicitRelay(t *testing.T) {
	// One relay, implicit observer: removing the relay must not leave
	// the observer publishing the captured face of a ghost.
	m := observerManager(t, observerFile(1, ""))
	store, err := confdb.Open(context.Background(), confdb.Memory, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	m.store = store
	seedStore(t, m, store)
	m.mu.Lock()
	m.startObserver(m.ctx, "obs")
	m.mu.Unlock()

	if _, err := m.Remove(context.Background(), confdb.KindRelay, "mc-a", "test"); err != nil {
		t.Fatal(err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if _, live := m.observers["obs"]; live {
		t.Fatal("observer outlived the only relay")
	}
	if cause := m.obsCause["obs"]; cause == "" {
		t.Error("no cause recorded for the stop")
	}
}

func TestObserverStartsWhenAmbiguityLifts(t *testing.T) {
	// Two relays, implicit observer: down for ambiguity. Removing one
	// relay makes "the only relay" answerable again — a restart would
	// start the observer, so the reconciliation does too.
	m := observerManager(t, observerFile(2, ""))
	store, err := confdb.Open(context.Background(), confdb.Memory, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	m.store = store
	seedStore(t, m, store)
	m.mu.Lock()
	m.startObserver(m.ctx, "obs") // refuses: ambiguous
	if _, live := m.observers["obs"]; live {
		t.Fatal("observer started on an ambiguous relay=")
	}
	m.mu.Unlock()

	if _, err := m.Remove(context.Background(), confdb.KindRelay, "mc-b", "test"); err != nil {
		t.Fatal(err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	h, live := m.observers["obs"]
	if !live {
		t.Fatalf("observer still down: %v", m.obsCause["obs"])
	}
	if h.relay != "mc-a" {
		t.Errorf("observer watches %q, want mc-a", h.relay)
	}
	if cause := m.obsCause["obs"]; cause != "" {
		t.Errorf("stale cause survived the start: %q", cause)
	}
}

func TestRemoveRefusesARelayAnObserverClaims(t *testing.T) {
	// An explicit relay= is a reference; removal is refused the way a
	// claimed radio's is, instead of leaving the reference dangling.
	m := observerManager(t, observerFile(1, "mc-a"))
	store, err := confdb.Open(context.Background(), confdb.Memory, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	m.store = store

	_, err = m.Remove(context.Background(), confdb.KindRelay, "mc-a", "test")
	if err == nil || !strings.Contains(err.Error(), "claims this relay") {
		t.Fatalf("removal of a claimed relay: %v", err)
	}
	if _, still := m.file.Relays["mc-a"]; !still {
		t.Fatal("the refusal did not keep the relay")
	}
}

func TestObserverStatusCarriesItsCause(t *testing.T) {
	// The status view says down and why, from the same recorded cause
	// the reconciliation writes — never a fabricated "connecting" for
	// an observer that was refused.
	m := observerManager(t, observerFile(2, ""))
	m.mu.Lock()
	m.startObserver(m.ctx, "obs")
	m.mu.Unlock()

	infos := m.MQTTInfos()
	if len(infos) != 1 {
		t.Fatalf("MQTTInfos = %+v", infos)
	}
	row := infos[0]
	if row.Connected != nil {
		t.Error("a refused observer pretends to have a broker session")
	}
	if !strings.Contains(row.Down, "relay=") {
		t.Errorf("Down = %q", row.Down)
	}
}
