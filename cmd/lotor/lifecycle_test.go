package main

// The manager's lifecycle transactions, driven end to end against an
// in-memory store and the real assembly chain: what persists must be
// running, what is refused must leave store, file and relay exactly
// as they were. The radio devices never open on a test box — the
// sessions live in the error/retry state, which the supervisor's own
// suite covers; here the subject is the transaction around them.

import (
	"context"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"

	"meshrunner.dev/lotor/internal/bus"
	"meshrunner.dev/lotor/internal/confdb"
	"meshrunner.dev/lotor/internal/config"
	"meshrunner.dev/lotor/internal/radio"
	"meshrunner.dev/lotor/internal/station"
	"meshrunner.dev/pkg/meshcore/companion"
)

var lifecycleDriverOnce sync.Once

type lifecycleRadio struct{}

func (*lifecycleRadio) Envelope() radio.Envelope {
	return radio.Envelope{FreqRangeLowHz: 400_000_000, FreqRangeHiHz: 930_000_000}
}
func (*lifecycleRadio) Configure(radio.Waveform) error { return nil }
func (*lifecycleRadio) StartReceive() error            { return nil }
func (*lifecycleRadio) Receive(ctx context.Context) (radio.Frame, error) {
	<-ctx.Done()
	return radio.Frame{}, ctx.Err()
}
func (*lifecycleRadio) NoiseFloor() (radio.NoiseFloor, bool) { return radio.NoiseFloor{}, false }
func (*lifecycleRadio) NoiseStarved() uint64                 { return 0 }
func (*lifecycleRadio) ChipStats() (radio.ChipStats, bool)   { return radio.ChipStats{}, false }
func (*lifecycleRadio) Airtime(int) time.Duration            { return time.Millisecond }
func (*lifecycleRadio) Transmit(context.Context, []byte, int8) (radio.TxReport, error) {
	return radio.TxReport{}, nil
}
func (*lifecycleRadio) AssessChannel(context.Context, float64) (bool, error) { return false, nil }
func (*lifecycleRadio) Close() error                                         { return nil }

func registerLifecycleDriver() {
	lifecycleDriverOnce.Do(func() {
		radio.Register("lifecycle-radio", radio.Driver{
			Presets: map[string]map[string]any{"lifecycle-board": {}},
			Inspect: func(map[string]any) (radio.Envelope, error) {
				return (&lifecycleRadio{}).Envelope(), nil
			},
			Open: func(map[string]any, *zap.Logger) (radio.Device, error) {
				return &lifecycleRadio{}, nil
			},
		})
	})
}

// lifecycleManager builds a manager over sampleFile and an in-memory
// store seeded with it, ready to start relays for real.
func lifecycleManager(t *testing.T) *manager {
	t.Helper()
	f := sampleFile()
	store, err := confdb.Open(context.Background(), confdb.Memory, 0)
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
		for name := range m.stations {
			m.stopStation(name)
		}
		m.mu.Unlock()
		cancel()
		m.wg.Wait()
	})
	return m
}

func freeTCPAddr(t *testing.T) string {
	t.Helper()
	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	return addr
}

func TestCreateMutateAndRemoveDetachedStation(t *testing.T) {
	m := lifecycleManager(t)
	addr := freeTCPAddr(t)
	msg, err := m.Create(context.Background(), confdb.KindStation, "alice",
		map[string]string{
			"protocol": "meshcore", "listen": addr, "profile": "eu-868-narrow",
			"identity": "new", "node_name": "Alice", "tx_power_dbm": "14",
		}, "test")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(msg, "listening") {
		t.Fatalf("create = %q", msg)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		infos := m.StationInfos()
		if len(infos) == 1 && infos[0].State == "running" {
			if infos[0].RF != "detached" || infos[0].Listen != addr {
				t.Fatalf("station info = %+v", infos[0])
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("station did not listen: %+v", infos)
		}
		time.Sleep(time.Millisecond)
	}

	if _, err := m.Mutate(context.Background(), confdb.KindStation, "alice",
		map[string]string{"node_name": "Alice Two"}, nil, "test"); err != nil {
		t.Fatal(err)
	}
	persisted, err := m.store.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got := persisted.Stations["alice"].Layered.Overrides["eu-868-narrow"]["node_name"]; got != "Alice Two" {
		t.Fatalf("persisted node_name = %v", got)
	}
	if _, err := m.Remove(context.Background(), confdb.KindStation, "alice", "test"); err != nil {
		t.Fatal(err)
	}
	if len(m.StationInfos()) != 0 {
		t.Fatal("removed station remains visible")
	}
}

func TestStationRadioMutationKeepsCompanionConnection(t *testing.T) {
	registerLifecycleDriver()
	m := lifecycleManager(t)
	if _, err := m.Create(context.Background(), confdb.KindRadio, "virtual-slot",
		map[string]string{"driver": "lifecycle-radio", "profile": "lifecycle-board"}, "test"); err != nil {
		t.Fatal(err)
	}
	addr := freeTCPAddr(t)
	if _, err := m.Create(context.Background(), confdb.KindStation, "alice",
		map[string]string{
			"protocol": "meshcore", "listen": addr, "profile": "eu-868-narrow",
			"identity": "new", "node_name": "Alice", "tx_power_dbm": "14",
		}, "test"); err != nil {
		t.Fatal(err)
	}
	var (
		conn net.Conn
		err  error
	)
	deadline := time.Now().Add(time.Second)
	for conn == nil && time.Now().Before(deadline) {
		conn, err = (&net.Dialer{Timeout: 50 * time.Millisecond}).DialContext(t.Context(), "tcp", addr)
		if err != nil {
			time.Sleep(time.Millisecond)
		}
	}
	if conn == nil {
		t.Fatal(err)
	}
	defer conn.Close()
	queryStation := func() {
		t.Helper()
		payload, marshalErr := companion.MarshalCommand(companion.DeviceQuery{TargetVersion: 3})
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		if writeErr := companion.WriteFrame(conn, companion.ToDevice, payload); writeErr != nil {
			t.Fatal(writeErr)
		}
		if _, readErr := companion.ReadFrame(conn, companion.ToApplication); readErr != nil {
			t.Fatal(readErr)
		}
	}
	queryStation()
	before := m.stations["alice"].service
	if _, err := m.Mutate(context.Background(), confdb.KindStation, "alice",
		map[string]string{"radio": "virtual-slot"}, nil, "test"); err != nil {
		t.Fatal(err)
	}
	if m.stations["alice"].service != before {
		t.Fatal("radio attachment restarted the station service")
	}
	queryStation()
	infos := m.StationInfos()
	if len(infos) != 1 || !infos[0].Connected || infos[0].Radio != "virtual-slot" {
		t.Fatalf("attached station = %+v", infos)
	}
	if _, err := m.Mutate(context.Background(), confdb.KindStation, "alice",
		nil, []string{"radio"}, "test"); err != nil {
		t.Fatal(err)
	}
	if m.stations["alice"].service != before {
		t.Fatal("radio detach restarted the station service")
	}
	queryStation()
	infos = m.StationInfos()
	if infos[0].RF != string(station.RFDetached) || !infos[0].Connected {
		t.Fatalf("detached station = %+v", infos[0])
	}
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

func TestCreateRadioRefusesAProfileUnknownToThisBinary(t *testing.T) {
	m := lifecycleManager(t)
	_, err := m.Create(context.Background(), confdb.KindRadio, "g3",
		map[string]string{
			"driver":  "sx126x-spi",
			"profile": "profile-this-binary-does-not-know",
		}, "test")
	if err == nil || !strings.Contains(err.Error(), "unknown profile") {
		t.Fatalf("unknown radio profile refusal = %v", err)
	}
	if _, exists := m.file.Radios["g3"]; exists {
		t.Fatal("the refused radio reached the manager's live file")
	}
	persisted, loadErr := m.store.Load(context.Background())
	if loadErr != nil {
		t.Fatal(loadErr)
	}
	if _, exists := persisted.Radios["g3"]; exists {
		t.Fatal("the refused radio reached the configuration store")
	}
}

func TestCreateObserverRequiresIATAEvenWithoutATopicPlaceholder(t *testing.T) {
	m := lifecycleManager(t)
	_, err := m.Create(context.Background(), confdb.KindMQTT, "paris",
		map[string]string{
			"url":   "wss://broker.example:8084",
			"topic": "private/{device}/{type}",
			"relay": "meshcore-868",
		}, "test")
	if err == nil || !strings.Contains(err.Error(), "iata= is required") {
		t.Fatalf("observer without iata refusal = %v", err)
	}
	if _, exists := m.file.MQTT["paris"]; exists {
		t.Fatal("the refused observer reached the manager's live file")
	}
	persisted, loadErr := m.store.Load(context.Background())
	if loadErr != nil {
		t.Fatal(loadErr)
	}
	if _, exists := persisted.MQTT["paris"]; exists {
		t.Fatal("the refused observer reached the configuration store")
	}
}

func TestManagerStartBringsUpTheTree(t *testing.T) {
	f := sampleFile()
	f.MQTT = map[string]config.MQTT{
		"obs": {Layered: config.Layered{Overrides: map[string]map[string]any{
			config.CustomProfile: {"url": "tcp://127.0.0.1:1", "iata": "PAR"},
		}}},
	}
	store, err := confdb.Open(context.Background(), confdb.Memory, 0)
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
