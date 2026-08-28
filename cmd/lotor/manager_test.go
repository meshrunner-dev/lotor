package main

import (
	"encoding/json"
	"strings"
	"testing"

	"meshrunner.dev/lotor/internal/confdb"
	"meshrunner.dev/lotor/internal/config"
)

func sampleFile() *config.File {
	return &config.File{
		Radios: map[string]config.Radio{
			"slot1": {Driver: "sx126x-spi", Layered: config.Layered{
				Profile: "rak6421-13300x-slot1",
				Overrides: map[string]map[string]any{
					"rak6421-13300x-slot1": {"spi": "/dev/spidev0.0"},
				},
			}},
		},
		Relays: map[string]config.Relay{
			"meshcore-868": {Protocol: "meshcore", Radio: "slot1", Layered: config.Layered{
				Profile: "eu-868-narrow",
				Overrides: map[string]map[string]any{
					"eu-868-narrow": {"node_name": "old name", "tx_power_dbm": 0},
				},
			}},
		},
	}
}

func TestApplyChangesEditsTheOverrideScope(t *testing.T) {
	f := sampleFile()
	change, relayName, err := applyChanges(f, "relay", "meshcore-868",
		map[string]any{"node_name": "new name", "session_limit": 12}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if relayName != "meshcore-868" {
		t.Fatalf("owner = %q", relayName)
	}
	ov := f.Relays["meshcore-868"].Layered.Overrides["eu-868-narrow"]
	if ov["node_name"] != "new name" || ov["session_limit"] != 12 {
		t.Fatalf("override scope = %v", ov)
	}
	// The revision knows the before: node_name had one, session_limit
	// did not — which is what makes its undo an unset.
	if change["node_name"].Old != "old name" || change["session_limit"].Old != nil {
		t.Fatalf("change = %+v", change)
	}
}

func TestApplyChangesUnsetLetsThePresetShowThrough(t *testing.T) {
	f := sampleFile()
	change, _, err := applyChanges(f, "relay", "meshcore-868",
		nil, []string{"node_name"})
	if err != nil {
		t.Fatal(err)
	}
	if _, still := f.Relays["meshcore-868"].Layered.Overrides["eu-868-narrow"]["node_name"]; still {
		t.Fatal("the override survived its unset")
	}
	if change["node_name"].Old != "old name" || change["node_name"].New != nil {
		t.Fatalf("change = %+v", change)
	}
	// Unsetting what is not set is a mistake worth naming.
	if _, _, err := applyChanges(f, "relay", "meshcore-868", nil, []string{"node_name"}); err == nil {
		t.Fatal("a second unset found something to remove")
	}
}

func TestApplyChangesGuardsWhatARelayIs(t *testing.T) {
	f := sampleFile()
	for _, attr := range []string{"protocol", "radio"} {
		if _, _, err := applyChanges(f, "relay", "meshcore-868",
			map[string]any{attr: "other"}, nil); err == nil {
			t.Errorf("set %s went through — that is surgery, not tuning", attr)
		}
	}
	if _, _, err := applyChanges(f, "relay", "meshcore-868",
		nil, []string{"profile"}); err == nil {
		t.Error("unset profile went through")
	}
}

func TestApplyChangesReachesTheTransmitBlock(t *testing.T) {
	f := sampleFile()
	_, _, err := applyChanges(f, "relay", "meshcore-868",
		map[string]any{"tx.mode": "shadow", "tx.queue_depth": 16}, nil)
	if err != nil {
		t.Fatal(err)
	}
	tx := f.Relays["meshcore-868"].TX
	if tx == nil || tx.Mode != "shadow" || tx.QueueDepth != 16 {
		t.Fatalf("tx = %+v", tx)
	}
	// The block was created by the first dotted set; validation still
	// owns the values.
	if err := f.Validate(false); err != nil {
		t.Fatal(err)
	}
}

func TestRadioChangesNameTheirOwner(t *testing.T) {
	f := sampleFile()
	_, owner, err := applyChanges(f, "radio", "slot1",
		map[string]any{"spi": "/dev/spidev0.1"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if owner != "meshcore-868" {
		t.Fatalf("owner = %q — the owning relay must bounce", owner)
	}
	if _, _, err := applyChanges(f, "radio", "slot1",
		map[string]any{"driver": "other"}, nil); err == nil {
		t.Fatal("set driver went through")
	}
}

func TestCloneFileIsDeep(t *testing.T) {
	f := sampleFile()
	c, err := cloneFile(f)
	if err != nil {
		t.Fatal(err)
	}
	c.Relays["meshcore-868"].Layered.Overrides["eu-868-narrow"]["node_name"] = "mutated"
	if f.Relays["meshcore-868"].Layered.Overrides["eu-868-narrow"]["node_name"] != "old name" {
		t.Fatal("the clone shares the original's maps")
	}
}

func TestUndoCoercionsSurviveJSON(t *testing.T) {
	// A revision's old values come back from JSON: numbers as float64.
	f := sampleFile()
	if _, _, err := applyChanges(f, "relay", "meshcore-868",
		map[string]any{"tx.queue_depth": float64(16)}, nil); err != nil {
		t.Fatal(err)
	}
	if f.Relays["meshcore-868"].TX.QueueDepth != 16 {
		t.Fatal("float64 did not coerce into the int field")
	}
	if _, _, err := applyChanges(f, "relay", "meshcore-868",
		map[string]any{"tx.queue_depth": 16.5}, nil); err == nil ||
		!strings.Contains(err.Error(), "whole number") {
		t.Fatal("a fractional queue depth went through")
	}
}

func TestObserverDisableIsStructural(t *testing.T) {
	f := &config.File{MQTT: map[string]config.MQTT{
		"lab": {Layered: config.Layered{Overrides: map[string]map[string]any{
			config.CustomProfile: {"url": "tcp://127.0.0.1:1883"},
		}}},
	}}
	change, owner, err := applyChanges(f, "mqtt", "lab",
		map[string]any{"disabled": true}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if owner != "" {
		t.Errorf("disabling an observer bounced relay %q", owner)
	}
	if !f.MQTT["lab"].Disabled {
		t.Error("disabled did not land on the field")
	}
	// The flag is item metadata, never a broker parameter: the
	// override scope must not have caught it.
	if _, leaked := f.MQTT["lab"].Layered.Overrides[config.CustomProfile]["disabled"]; leaked {
		t.Error("disabled leaked into the override scope")
	}
	if c := change["disabled"]; c.New != any(true) {
		t.Errorf("revision records %v", c)
	}
	if _, _, err := applyChanges(f, "mqtt", "lab", nil, []string{"disabled"}); err != nil {
		t.Fatal(err)
	}
	if f.MQTT["lab"].Disabled {
		t.Error("unset did not clear the flag")
	}
}

func TestObserverParamsWantABuildableTopic(t *testing.T) {
	mq := config.MQTT{Layered: config.Layered{Overrides: map[string]map[string]any{
		config.CustomProfile: {"url": "wss://broker.example:8084"},
	}}}
	if _, err := resolveMQTTParams(mq); err == nil ||
		!strings.Contains(err.Error(), "empty level") {
		t.Errorf("iata hole accepted: %v", err)
	}
	mq.Layered.Overrides[config.CustomProfile]["iata"] = "PAR"
	if _, err := resolveMQTTParams(mq); err != nil {
		t.Errorf("with iata: %v", err)
	}
}

func TestObserverIATANormalizesAtTheDoor(t *testing.T) {
	f := &config.File{MQTT: map[string]config.MQTT{
		"lab": {Layered: config.Layered{Overrides: map[string]map[string]any{
			config.CustomProfile: {"url": "tcp://127.0.0.1:1883"},
		}}},
	}}
	if _, _, err := applyChanges(f, "mqtt", "lab",
		map[string]any{"iata": "p@r"}, nil); err == nil {
		t.Error("a bad code reached the store")
	}
	change, _, err := applyChanges(f, "mqtt", "lab",
		map[string]any{"iata": "par"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := f.MQTT["lab"].Layered.Overrides[config.CustomProfile]["iata"]; got != "PAR" {
		t.Errorf("store holds %v, want PAR", got)
	}
	if change["iata"].New != any("PAR") {
		t.Errorf("revision records %v", change["iata"])
	}
}

func TestNoSecretReachesTheRevision(t *testing.T) {
	// guest_password is the case a named list misses: declared secret
	// beside identity, and never masked until the schema was asked.
	m := &manager{kinds: buildKinds(), file: sampleFile()}
	change := map[string]confdb.Change{
		"identity":       {Old: "0011", New: "2233"},
		"guest_password": {Old: "hunter2", New: "swordfish"},
		"node_name":      {Old: "old name", New: "new name"},
	}
	m.maskSecrets(m.file, "relay", "meshcore-868", change)

	for _, attr := range []string{"identity", "guest_password"} {
		if got := change[attr]; got.Old != maskedChange || got.New != maskedChange {
			t.Errorf("%s recorded as %v → %v, want both masked", attr, got.Old, got.New)
		}
	}
	if got := change["node_name"]; got.Old != "old name" || got.New != "new name" {
		t.Errorf("an ordinary attribute was masked: %v → %v", got.Old, got.New)
	}
}

func TestMaskingLeavesAnAbsentValueAbsent(t *testing.T) {
	// A first set has no old value, and an unset has no new one. Those
	// nils are what undo reads to know it should unset, so masking
	// must not invent a value where there was none.
	m := &manager{kinds: buildKinds(), file: sampleFile()}
	change := map[string]confdb.Change{"identity": {New: "2233"}}
	m.maskSecrets(m.file, "relay", "meshcore-868", change)
	if got := change["identity"]; got.Old != nil || got.New != maskedChange {
		t.Errorf("first set recorded as %v → %v", got.Old, got.New)
	}
}

func TestUndoRefusesASecretItCannotRestore(t *testing.T) {
	// Measured on the MQTT password before this guard existed: undo
	// set it to the literal "<secret>" and reported success.
	_, _, err := undoValues(7, map[string]confdb.Change{
		"password": {Old: maskedChange, New: maskedChange},
	})
	if err == nil {
		t.Fatal("undo replayed a masked secret")
	}
	for _, want := range []string{"revision 7", "password", "set it by hand"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %q", err, want)
		}
	}
	// The first change to a secret records no old value, so undoing it
	// unsets — that case is restorable and must keep working.
	typed, unset, err := undoValues(8, map[string]confdb.Change{
		"password": {New: maskedChange},
	})
	if err != nil || len(typed) != 0 || len(unset) != 1 || unset[0] != "password" {
		t.Errorf("first-set undo = %v %v %v, want an unset", typed, unset, err)
	}
	// And an ordinary attribute is unaffected.
	typed, _, err = undoValues(9, map[string]confdb.Change{
		"node_name": {Old: "old name", New: "new name"},
	})
	if err != nil || typed["node_name"] != "old name" {
		t.Errorf("ordinary undo = %v %v", typed, err)
	}
}

func TestOrphanOverridesCanStillBeUnset(t *testing.T) {
	// A key stored by a past shape of the software — the schema no
	// longer names it, and refusing the unset stranded it in the
	// store with no door but sqlite.
	m := &manager{kinds: buildKinds(), file: &config.File{MQTT: map[string]config.MQTT{
		"lab": {Layered: config.Layered{Overrides: map[string]map[string]any{
			config.CustomProfile: {"url": "tcp://127.0.0.1:1883", "neighbors": true},
		}}},
	}}}
	if _, err := m.parseChanges("mqtt", "lab", nil, []string{"neighbors"}); err != nil {
		t.Fatalf("orphan unset refused: %v", err)
	}
	// A name no shape ever stored stays refused.
	if _, err := m.parseChanges("mqtt", "lab", nil, []string{"never_was"}); err == nil {
		t.Error("an invented name passed the door")
	}
	change, _, err := applyChanges(m.file, "mqtt", "lab", nil, []string{"neighbors"})
	if err != nil {
		t.Fatal(err)
	}
	if _, held := m.file.MQTT["lab"].Layered.Overrides[config.CustomProfile]["neighbors"]; held {
		t.Error("the orphan survived its unset")
	}
	// The revision cannot know whether a past shape's key was secret,
	// so its value travels masked.
	m.maskSecrets(m.file, "mqtt", "lab", change)
	if change["neighbors"].Old != maskedChange {
		t.Errorf("orphan value recorded in the clear: %+v", change["neighbors"])
	}
}

func TestObserverShapeHeals(t *testing.T) {
	old := `{"disabled":false,"profile":"","overrides":{"custom":{` +
		`"url":"tcp://127.0.0.1:1883","iata":"par","neighbors":true,` +
		`"status":false,"neighbors_interval":"2h"}}}`
	healed, changed, err := healObserverAttrs(old)
	if err != nil || !changed {
		t.Fatalf("heal: %v changed=%v", err, changed)
	}
	var o map[string]any
	if err := json.Unmarshal([]byte(healed), &o); err != nil {
		t.Fatal(err)
	}
	overrides, ok := o["overrides"].(map[string]any)
	if !ok {
		t.Fatalf("overrides gone: %v", o)
	}
	kv, ok := overrides["custom"].(map[string]any)
	if !ok {
		t.Fatalf("custom scope gone: %v", overrides)
	}
	// The explicit old interval wins over the consent default, the
	// silenced heartbeat keeps its silence, the code speaks uppercase.
	if kv["neighbours_interval"] != "2h" || kv["status_interval"] != "0s" || kv["iata"] != "PAR" {
		t.Errorf("healed wrong: %v", kv)
	}
	for _, gone := range []string{"neighbors", "neighbors_interval", "status"} {
		if _, held := kv[gone]; held {
			t.Errorf("%s survived", gone)
		}
	}
	// The settled shape passes untouched.
	if _, changed, _ := healObserverAttrs(healed); changed {
		t.Error("a settled shape was rewritten")
	}
}

func TestTypedObserversLiftIntoTheLayers(t *testing.T) {
	// The exact shape the lab stored before the layering — the row a
	// strict load refuses whole, taking the daemon's boot with it.
	old := `{"iata":"par","password":"","raw":false,"relay":"","status":false,` +
		`"status_interval":"30s","token":"","topic":"","tx":"","types":[],` +
		`"url":"tcp://127.0.0.1:1883"}`
	healed, changed, err := healTypedObserver(old)
	if err != nil || !changed {
		t.Fatalf("lift: %v changed=%v", err, changed)
	}
	var o map[string]any
	if err := json.Unmarshal([]byte(healed), &o); err != nil {
		t.Fatal(err)
	}
	overrides, ok := o["overrides"].(map[string]any)
	if !ok {
		t.Fatalf("no overrides: %v", o)
	}
	kv, ok := overrides["custom"].(map[string]any)
	if !ok {
		t.Fatalf("no custom scope: %v", overrides)
	}
	// Zero values dropped, the silenced heartbeat kept its silence
	// through the settlement, the region code lifted uppercase.
	if kv["url"] != "tcp://127.0.0.1:1883" || kv["iata"] != "PAR" || kv["status_interval"] != "0s" {
		t.Errorf("lifted wrong: %v", kv)
	}
	for _, gone := range []string{"password", "token", "topic", "tx", "raw", "types", "relay", "status"} {
		if _, held := kv[gone]; held {
			t.Errorf("zero value %s survived", gone)
		}
	}
	// A layered row passes untouched.
	if _, changed, _ := healTypedObserver(healed); changed {
		t.Error("a layered row was rewritten")
	}
	// And the lifted shape decodes under the strict door.
	if _, err := resolveMQTTParams(mustDecodeMQTT(t, healed)); err != nil {
		t.Errorf("lifted shape still refused: %v", err)
	}
}

// mustDecodeMQTT round-trips healed JSON through the same strict door
// the loader uses.
func mustDecodeMQTT(t *testing.T, attrs string) config.MQTT {
	t.Helper()
	var o map[string]any
	if err := json.Unmarshal([]byte(attrs), &o); err != nil {
		t.Fatal(err)
	}
	mq, err := config.Decode[config.MQTT](o)
	if err != nil {
		t.Fatalf("strict decode: %v", err)
	}
	return mq
}

func TestRevisionSecretsScrubWhereverTheyNest(t *testing.T) {
	// Three real shapes from the journal: a plain change, the set that
	// predates the mask, and the whole-object keepsake of a remove.
	var c any
	raw := `{"identity":{"new":"deadbeef"},"node_name":{"new":"x"},` +
		`"object":{"old":{"overrides":{"eu":{"guest_password":"hunter2","spi":"/dev/spidev0.0"}}}}}`
	if err := json.Unmarshal([]byte(raw), &c); err != nil {
		t.Fatal(err)
	}
	if !scrubSecrets(c) {
		t.Fatal("nothing scrubbed")
	}
	out, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	for _, leaked := range []string{"deadbeef", "hunter2"} {
		if strings.Contains(string(out), leaked) {
			t.Errorf("%s survived: %s", leaked, out)
		}
	}
	for _, kept := range []string{`"node_name":{"new":"x"}`, "/dev/spidev0.0"} {
		if !strings.Contains(string(out), kept) {
			t.Errorf("%s lost: %s", kept, out)
		}
	}
	// Idempotent: a masked journal stays put.
	if scrubSecrets(c) {
		t.Error("a masked journal was rewritten")
	}
}
