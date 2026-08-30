package main

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"maps"
	"time"

	"go.uber.org/zap"

	"meshrunner.dev/lotor/internal/bus"
	"meshrunner.dev/lotor/internal/cli"
	"meshrunner.dev/lotor/internal/confdb"
	"meshrunner.dev/lotor/internal/config"
	"meshrunner.dev/lotor/internal/protocol"
	enginemc "meshrunner.dev/lotor/internal/protocol/meshcore"
	"meshrunner.dev/lotor/internal/radio"
	"meshrunner.dev/lotor/internal/relay"
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

func TestRadioProfilesCompleteBeforeTheDriverIsTyped(t *testing.T) {
	var profiles func(string) []string
	var drivers []string
	for _, kind := range buildKinds() {
		if kind.Name == confdb.KindRadio {
			profiles = kind.Profiles
			for _, attr := range kind.Attrs {
				if attr.Name == "driver" {
					drivers = attr.Enum
				}
			}
			break
		}
	}
	if profiles == nil {
		t.Fatal("radio kind exposes no profile catalog")
	}
	want := "lyra-zerow-station-g3"
	got := profiles("")
	found := false
	for _, profile := range got {
		if profile == want {
			found = true
		}
	}
	if !found {
		t.Errorf("radio profiles before driver = %v, missing %q", got, want)
	}
	want = "sx126x-spi"
	found = false
	for _, driver := range drivers {
		if driver == want {
			found = true
		}
	}
	if !found {
		t.Errorf("radio driver choices = %v, missing %q", drivers, want)
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

func TestObserverParamsRequireIATARegardlessOfTopic(t *testing.T) {
	mq := config.MQTT{Layered: config.Layered{Overrides: map[string]map[string]any{
		config.CustomProfile: {
			"url":   "wss://broker.example:8084",
			"topic": "private/{device}/{type}",
		},
	}}}
	if _, err := resolveMQTTParams(mq); err == nil ||
		!strings.Contains(err.Error(), "iata= is required") {
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
	if !scrubSecretsIn(c, secretKeys) {
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
	if scrubSecretsIn(c, secretKeys) {
		t.Error("a masked journal was rewritten")
	}
}

func TestViewsAnswerWhileTheLifecycleLockIsHeld(t *testing.T) {
	// The discipline the freeze taught: everything a joined goroutine
	// calls — the observer's health, the neighbourhood round, an
	// over-the-air get — must answer while mu is held, because the
	// holder of mu may be waiting for that very goroutine to die.
	m := &manager{kinds: buildKinds(), file: &config.File{},
		infos:  map[string]cli.RelayInfo{},
		radios: map[string]cli.RadioInfo{},
		traces: map[string][]config.Trace{},
	}
	m.viewMu.Lock()
	m.infos["mc"] = cli.RelayInfo{Name: "mc", TXMode: config.TXOnAir}
	m.traces["relay mc"] = []config.Trace{
		{Key: "protocol", Value: "meshcore", Source: "config"},
		{Key: "node_name", Value: "Raccoon City", Source: "override:eu"},
	}
	m.viewMu.Unlock()

	m.mu.Lock()
	defer m.mu.Unlock()

	done := make(chan string, 3)
	go func() {
		h := m.observerHealth("mc")()
		done <- "health " + h.Repeat
	}()
	go func() {
		v := m.relayValue("mc", "node_name")
		done <- "get " + v
	}()
	go func() {
		done <- m.otaGet("mc", "name")
	}()
	seen := map[string]bool{}
	for range 3 {
		select {
		case v := <-done:
			seen[v] = true
		case <-time.After(2 * time.Second):
			t.Fatalf("a view blocked behind the lifecycle lock — got only %v", seen)
		}
	}
	if !seen["health on"] || !seen["get Raccoon City"] || !seen["> Raccoon City"] {
		t.Errorf("views answered wrong: %v", seen)
	}
}

func TestOTASetIsHonestAboutGarbage(t *testing.T) {
	m := &manager{kinds: buildKinds(), file: &config.File{},
		infos: map[string]cli.RelayInfo{}, radios: map[string]cli.RadioInfo{},
		traces: map[string][]config.Trace{
			"relay mc": {
				{Key: "protocol", Value: "meshcore", Source: "config"},
				{Key: "node_name", Value: "Raccoon City", Source: "override:eu"},
			},
		},
		air: make(chan airOrder, 4),
	}
	// The engine's own judgement needs the whole resolved shape: a
	// real band preset, the name assembly insists on.
	builder, err := protocol.Lookup("meshcore")
	if err != nil {
		t.Fatal(err)
	}
	cfg := map[string]any{"node_name": "Raccoon City"}
	maps.Copy(cfg, builder.Presets["eu-868-narrow"])
	m.cfgs = map[string]map[string]any{"mc": cfg}
	// A value the schema refuses earns its refusal now — short, the
	// reference's ERR shape — not a false ok and a journal line the
	// admin cannot see.
	out := m.runOTA("mc", "air:test", "air:test", "set tx banana")
	if !strings.HasPrefix(out, "ERR: ") || len(out) > 70 {
		t.Errorf("garbage got %q", out)
	}
	if len(m.air) != 0 {
		t.Fatal("garbage reached the air channel")
	}
	// A sound value is queued and answered with the reference's own
	// two bytes: a reply is airtime.
	if out := m.runOTA("mc", "air:test", "air:test", "set tx 6"); out != "OK" {
		t.Errorf("sound value got %q", out)
	}
	o := <-m.air
	if o.set["tx_power_dbm"] != "6" || o.principal != "air:test" {
		t.Errorf("order = %+v", o)
	}
	// Unknown words earn the reference's exact answer, no echo.
	if out := m.runOTA("mc", "air:test", "air:test", "set warp 9"); out != "Unknown command" {
		t.Errorf("unknown setting got %q", out)
	}
	if out := m.runOTA("mc", "air:test", "air:test", "get name"); out != "> Raccoon City" {
		t.Errorf("get shape: %q", out)
	}

	// A grant needs the whole key — the reference's exact words —
	// where a removal below may use a prefix.
	if out := m.runOTA("mc", "air:test", "air:test", "setperm abcd 3"); out != "Err - invalid params" {
		t.Errorf("short setperm key got %q", out)
	}
	full := strings.Repeat("ab", 32)
	if out := m.runOTA("mc", "air:test", "air:test", "setperm "+full+" 3"); out != "OK" {
		t.Errorf("setperm got %q", out)
	}
	g := <-m.air
	if !g.grant || g.perms != enginemc.PermAdmin || len(g.pubKey) != 32 {
		t.Errorf("grant order = %+v", g)
	}
	// The byte travels whole: read-only stays read-only, not admin.
	if out := m.runOTA("mc", "air:test", "air:test", "setperm "+full+" 1"); out != "OK" {
		t.Errorf("setperm read-only got %q", out)
	}
	if g := <-m.air; g.perms != enginemc.PermReadOnly {
		t.Errorf("read-only flattened: %+v", g)
	}
	// A guest role is the reference's word for removal — and removal
	// alone may name its entry by prefix.
	if out := m.runOTA("mc", "air:test", "air:test", "setperm "+full+" 0"); out != "OK" {
		t.Errorf("setperm revoke got %q", out)
	}
	if g := <-m.air; !g.grant || g.perms != enginemc.PermGuest {
		t.Errorf("revoke order = %+v", g)
	}
	if out := m.runOTA("mc", "air:test", "air:test", "setperm abcd 0"); out != "OK" {
		t.Errorf("prefix removal got %q", out)
	}
	if g := <-m.air; g.perms != enginemc.PermGuest || len(g.pubKey) != 2 {
		t.Errorf("prefix removal order = %+v", g)
	}
	// The host keeps its own clock: sync is already true and answers
	// in the OK-with-detail shape; setting it wears the ERR shape.
	if out := m.runOTA("mc", "air:test", "air:test", "clock sync"); out != "OK - clock already synced (system time)" {
		t.Errorf("clock sync got %q", out)
	}
	if out := m.runOTA("mc", "air:test", "air:test", "time 1756400000"); out != "ERR: clock not settable (system time)" {
		t.Errorf("time got %q", out)
	}
	// The reference gates set freq to the serial port; ours is the
	// console, and the air is refused.
	if out := m.runOTA("mc", "air:test", "air:test", "set freq 869618000"); out != "ERR: console only" {
		t.Errorf("set freq got %q", out)
	}
	// Reading freq stays open, like the reference's get — and speaks
	// megahertz, the wire's unit, where the store keeps hertz.
	m.traces["relay mc"] = append(m.traces["relay mc"],
		config.Trace{Key: "tx.mode", Value: "on-air", Source: "config"},
		config.Trace{Key: "frequency_hz", Value: 869618000, Source: "profile:eu"})
	if out := m.runOTA("mc", "air:test", "air:test", "get repeat"); out != "> on" {
		t.Errorf("get repeat got %q", out)
	}
	if out := m.runOTA("mc", "air:test", "air:test", "get freq"); out != "> 869.618" {
		t.Errorf("get freq got %q", out)
	}
	// A lesser gate is not repeating: the ladder collapses to off.
	for i, tr := range m.traces["relay mc"] {
		if tr.Key == "tx.mode" {
			m.traces["relay mc"][i].Value = "on-air-zero-hop"
		}
	}
	if out := m.runOTA("mc", "air:test", "air:test", "get repeat"); out != "> off" {
		t.Errorf("get repeat (zero-hop) got %q", out)
	}
	// The neighbourhood verbs: refresh queues a scan off the engine's
	// goroutine, purge — the argument-less remove — sweeps the table.
	// The pre-flight reads the live view: a relay that cannot key the
	// radio, or one already scanning, is refused to the admin's face
	// rather than answered OK and refused in the journal.
	m.infos["mc"] = cli.RelayInfo{Discover: func() (<-chan cli.Neighbour, time.Time, error) {
		return nil, time.Time{}, nil
	}}
	if out := m.runOTA("mc", "air:test", "air:test", "discover.neighbors"); out != "Err - transmit gate is dry" {
		t.Errorf("dry gate got %q", out)
	}
	info := m.infos["mc"]
	info.TXMode = config.TXOnAir
	info.ScanWindow = func() (time.Time, bool) { return time.Now().Add(42 * time.Second), true }
	m.infos["mc"] = info
	if out := m.runOTA("mc", "air:test", "air:test", "discover.neighbors"); out != "Err - scanning, 42s left" {
		t.Errorf("busy scan got %q", out)
	}
	info.ScanWindow = func() (time.Time, bool) { return time.Time{}, false }
	m.infos["mc"] = info
	if out := m.runOTA("mc", "air:test", "air:test", "discover.neighbors"); out != "OK - Discover sent" {
		t.Errorf("discover got %q", out)
	}
	if o := <-m.air; !o.discover {
		t.Errorf("discover order = %+v", o)
	}
	if out := m.runOTA("mc", "air:test", "air:test", "discover.neighbors now"); out != "Err - discover.neighbors has no options" {
		t.Errorf("discover with options got %q", out)
	}
	var removed [][]byte
	info.RemoveNeighbours = func(prefix []byte) int {
		removed = append(removed, prefix)
		return len(removed)
	}
	m.infos["mc"] = info
	if out := m.runOTA("mc", "air:test", "air:test", "neighbor.remove"); out != "OK" {
		t.Errorf("purge got %q", out)
	}
	if out := m.runOTA("mc", "air:test", "air:test", "neighbor.remove de247e"); out != "OK" {
		t.Errorf("prefix remove got %q", out)
	}
	if out := m.runOTA("mc", "air:test", "air:test", "neighbor.remove zz"); out != "ERR: bad pubkey" {
		t.Errorf("bad hex got %q", out)
	}
	if len(removed) != 2 || len(removed[0]) != 0 || len(removed[1]) != 3 {
		t.Errorf("removals = %v", removed)
	}
	// Owner info: the wire carries newlines as bars, both ways, and a
	// value nobody set reads as empty — never as a word this daemon
	// invented, which the app would show as if it were the setting.
	if out := m.runOTA("mc", "air:test", "air:test", "get owner.info"); out != "> " {
		t.Errorf("unset owner.info got %q", out)
	}
	if out := m.runOTA("mc", "air:test", "air:test", "set owner.info Raton|laveur"); out != "OK" {
		t.Errorf("set owner.info got %q", out)
	}
	if o := <-m.air; o.set["owner_info"] != "Raton\nlaveur" {
		t.Errorf("bars did not become newlines: %+v", o.set)
	}
	m.traces["relay mc"] = append(m.traces["relay mc"],
		config.Trace{Key: "owner_info", Value: "Raton\nlaveur", Source: "config"})
	if out := m.runOTA("mc", "air:test", "air:test", "get owner.info"); out != "> Raton|laveur" {
		t.Errorf("newlines did not become bars: %q", out)
	}
	// The delay knobs read back the default actually in force when
	// nobody set them: an empty answer here would show on the asker's
	// screen as "no jitter", which is not what the relay runs on.
	if out := m.runOTA("mc", "air:test", "air:test", "get txdelay"); out != "> 0.5" {
		t.Errorf("unset txdelay got %q", out)
	}
	if out := m.runOTA("mc", "air:test", "air:test", "get direct.txdelay"); out != "> 0.3" {
		t.Errorf("unset direct.txdelay got %q", out)
	}
	if out := m.runOTA("mc", "air:test", "air:test", "get rxdelay"); out != "> 0" {
		t.Errorf("unset rxdelay got %q", out)
	}
	if out := m.runOTA("mc", "air:test", "air:test", "set txdelay 0.7"); out != "OK" {
		t.Errorf("set txdelay got %q", out)
	}
	if o := <-m.air; o.set["tx_delay_factor"] != "0.7" {
		t.Errorf("txdelay order = %+v", o.set)
	}
	// Out of the reference's range: refused now, the bound named.
	if out := m.runOTA("mc", "air:test", "air:test", "set txdelay 2.5"); !strings.HasPrefix(out, "ERR: ") ||
		!strings.Contains(out, "0..2") {
		t.Errorf("txdelay 2.5 got %q", out)
	}
	if out := m.runOTA("mc", "air:test", "air:test", "set rxdelay 21"); !strings.HasPrefix(out, "ERR: ") ||
		!strings.Contains(out, "0..20") {
		t.Errorf("rxdelay 21 got %q", out)
	}
	m.traces["relay mc"] = append(m.traces["relay mc"],
		config.Trace{Key: "rx_delay_base", Value: "12", Source: "config"})
	if out := m.runOTA("mc", "air:test", "air:test", "get rxdelay"); out != "> 12" {
		t.Errorf("set rxdelay read back %q", out)
	}
	// The width and orbit knobs read back this node's own defaults —
	// mode 1 and minimal, the two deliberate steps past the reference.
	if out := m.runOTA("mc", "air:test", "air:test", "get path.hash.mode"); out != "> 1" {
		t.Errorf("unset path.hash.mode got %q", out)
	}
	if out := m.runOTA("mc", "air:test", "air:test", "get loop.detect"); out != "> minimal" {
		t.Errorf("unset loop.detect got %q", out)
	}
	if out := m.runOTA("mc", "air:test", "air:test", "set loop.detect strict"); out != "OK" {
		t.Errorf("set loop.detect got %q", out)
	}
	if o := <-m.air; o.set["loop_detect"] != "strict" {
		t.Errorf("loop.detect order = %+v", o.set)
	}
	if out := m.runOTA("mc", "air:test", "air:test", "set path.hash.mode 3"); !strings.HasPrefix(out, "ERR: ") ||
		!strings.Contains(out, "0, 1 or 2") {
		t.Errorf("path.hash.mode 3 got %q", out)
	}
	if out := m.runOTA("mc", "air:test", "air:test", "set loop.detect banana"); !strings.HasPrefix(out, "ERR: ") ||
		!strings.Contains(out, "strict") {
		t.Errorf("loop.detect banana got %q", out)
	}
	// The deliberate absences answer like any unknown word.
	for _, cmd := range []string{"reboot", "clkreboot", "tempradio 869525000 62500 8 8 10", "poweroff"} {
		if out := m.runOTA("mc", "air:test", "air:test", cmd); out != otaUnknown {
			t.Errorf("%q got %q", cmd, out)
		}
	}
}

func TestDeepCheckRefusesAStillbornGate(t *testing.T) {
	// Every precondition the assembly's arming enforces must refuse
	// here, pre-persistence — or a mutation opening the transmit gate
	// replaces a running relay with a corpse it validated too late.
	seed := strings.Repeat("11", 32)
	onAir := func(f *config.File) map[string]any {
		rl := f.Relays["meshcore-868"]
		rl.TX = &config.TX{Mode: config.TXOnAir}
		f.Relays["meshcore-868"] = rl
		return rl.Layered.Overrides["eu-868-narrow"]
	}
	cases := []struct {
		name string
		mut  func(ov map[string]any)
		want string
	}{
		{"no identity", func(map[string]any) {}, "node identity"},
		{"no node name", func(ov map[string]any) {
			ov["identity"] = seed
			delete(ov, "node_name")
		}, "node_name"},
		{"no duty ceiling", func(ov map[string]any) {
			ov["identity"] = seed
			ov["duty_cycle_pct"] = 0.0
		}, "duty_cycle_pct"},
	}
	for _, c := range cases {
		f := sampleFile()
		c.mut(onAir(f))
		err := deepCheck(f, confdb.KindRelay, "meshcore-868", "meshcore-868")
		if err == nil {
			t.Errorf("%s: on-air accepted", c.name)
		} else if !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: refused for the wrong reason: %v", c.name, err)
		}
	}
	// A radio mutation that costs a transmit prerequisite is judged
	// through the claiming relay's own preflight.
	f0 := sampleFile()
	onAir(f0)["identity"] = seed
	f0.Radios["slot1"].Layered.Overrides["rak6421-13300x-slot1"]["chip"] = ""
	err := deepCheck(f0, confdb.KindRadio, "slot1", "meshcore-868")
	if err == nil || !strings.Contains(err.Error(), "chip") {
		t.Errorf("chipless radio under an on-air relay: %v", err)
	}
	// With every prerequisite present, the same gate passes — the
	// preflight refuses stillbirths, not transmission.
	f := sampleFile()
	onAir(f)["identity"] = seed
	if err := deepCheck(f, confdb.KindRelay, "meshcore-868", "meshcore-868"); err != nil {
		t.Fatalf("sound on-air refused: %v", err)
	}
	// And creation runs the same judgement: shadow needs the pipeline
	// armed too, and this relay cannot arm it.
	f = sampleFile()
	rl := f.Relays["meshcore-868"]
	rl.TX = &config.TX{Mode: config.TXShadow}
	f.Relays["meshcore-868"] = rl
	if err := preflight("meshcore-868", rl, f.Radios["slot1"]); err == nil {
		t.Fatal("shadow without identity passed preflight")
	}
}

func TestStillbornReplacesTheWholeView(t *testing.T) {
	// A successor that fails assembly must not inherit its
	// predecessor's face: resolved config, provenance and the radio
	// claim all describe a relay that no longer runs.
	f := sampleFile()
	f.Radios["slot1"] = config.Radio{Driver: "no-such-driver"}
	m := &manager{
		file: f, bus: bus.New(), log: zap.NewNop(),
		running: map[string]*managedRelay{},
		infos: map[string]cli.RelayInfo{"meshcore-868": {
			Name: "meshcore-868", State: func() string { return "running" },
		}},
		radios: map[string]cli.RadioInfo{"slot1": {Name: "slot1", Relay: "meshcore-868"}},
		cfgs:   map[string]map[string]any{"meshcore-868": {"node_name": "old name"}},
		traces: map[string][]config.Trace{
			"relay meshcore-868": {{Key: "node_name", Value: "old name", Source: "config"}},
			"radio slot1":        {{Key: "spi", Value: "/dev/spidev0.0", Source: "config"}},
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	m.startRelay(ctx, "meshcore-868")
	t.Cleanup(func() {
		cancel()
		<-m.running["meshcore-868"].done
	})

	m.viewMu.RLock()
	defer m.viewMu.RUnlock()
	if info := m.infos["meshcore-868"]; info.State() != relay.StateError || info.Err() == "" {
		t.Errorf("stillborn info: state %q, err %q", info.State(), info.Err())
	}
	if _, stale := m.cfgs["meshcore-868"]; stale {
		t.Error("the predecessor's resolved config survived")
	}
	if _, stale := m.radios["slot1"]; stale {
		t.Error("the predecessor's radio claim survived")
	}
	for _, key := range []string{"relay meshcore-868", "radio slot1"} {
		if _, stale := m.traces[key]; stale {
			t.Errorf("the predecessor's %s provenance survived", key)
		}
	}
}

func TestAMutationCannotOutrunTheBoard(t *testing.T) {
	// A dBm figure the schema likes and the board cannot serve: the
	// mutation is refused, and nothing reaches the store.
	f := sampleFile()
	f.Relays["meshcore-868"].Layered.Overrides["eu-868-narrow"]["tx_power_dbm"] = "30"
	err := deepCheck(f, confdb.KindRelay, "meshcore-868", "meshcore-868")
	if err == nil {
		t.Fatal("30 dBm passed a 22 dBm board")
	}
	if !strings.Contains(err.Error(), "22 dBm cap") {
		t.Errorf("error = %v", err)
	}
	// A frequency the board does not serve is the same judgement.
	f = sampleFile()
	f.Relays["meshcore-868"].Layered.Overrides["eu-868-narrow"]["frequency_hz"] = 433000000
	if err := deepCheck(f, confdb.KindRelay, "meshcore-868", "meshcore-868"); err == nil {
		t.Fatal("433 MHz passed an 868 MHz board")
	}
	// A scope nobody has selected is judged too: the day an operator
	// switches profile is not the day to find out the board cannot
	// key what the profile asks.
	builder, err := protocol.Lookup("meshcore")
	if err != nil {
		t.Fatal(err)
	}
	f = sampleFile()
	spare := map[string]any{"tx_power_dbm": "30"}
	maps.Copy(spare, builder.Presets["eu-868-narrow"])
	f.Relays["meshcore-868"].Layered.Overrides["custom"] = spare
	err = deepCheck(f, confdb.KindRelay, "meshcore-868", "meshcore-868")
	if err == nil {
		t.Fatal("an unselected scope smuggled 30 dBm past the board")
	}
	if !strings.Contains(err.Error(), "22 dBm cap") {
		t.Errorf("unselected scope refused for the wrong reason: %v", err)
	}
	// What the board can serve still resolves.
	f = sampleFile()
	f.Relays["meshcore-868"].Layered.Overrides["eu-868-narrow"]["tx_power_dbm"] = "-9"
	if err := deepCheck(f, confdb.KindRelay, "meshcore-868", "meshcore-868"); err != nil {
		t.Fatalf("-9 dBm refused: %v", err)
	}
}

func TestOTASetRefusesWhatTheBoardCannotDo(t *testing.T) {
	builder, err := protocol.Lookup("meshcore")
	if err != nil {
		t.Fatal(err)
	}
	cfg := map[string]any{"node_name": "Raccoon City"}
	maps.Copy(cfg, builder.Presets["eu-868-narrow"])
	m := &manager{kinds: buildKinds(), file: &config.File{},
		infos: map[string]cli.RelayInfo{"mc": {Name: "mc", Radio: "sx"}},
		radios: map[string]cli.RadioInfo{"sx": {Name: "sx", Envelope: radio.Envelope{
			MaxTxPowerDBm: 22, MaxTxPowerSet: true,
			ChipMinDBm: -9, ChipMaxDBm: 22,
			FreqRangeLowHz: 850_000_000, FreqRangeHiHz: 930_000_000,
		}}},
		traces: map[string][]config.Trace{
			"relay mc": {{Key: "protocol", Value: "meshcore", Source: "config"}},
		},
		cfgs: map[string]map[string]any{"mc": cfg},
		air:  make(chan airOrder, 4),
	}
	// The admin hears the board's own refusal, on the air, at once,
	// and the order never leaves for the manager's goroutine.
	out := m.runOTA("mc", "air:test", "air:test", "set tx 30")
	if !strings.HasPrefix(out, "ERR: ") || !strings.Contains(out, "22 dBm cap") {
		t.Errorf("over the cap got %q", out)
	}
	if len(m.air) != 0 {
		t.Fatal("a value the board refuses reached the air channel")
	}
	// freq is console-only, so the air cannot reach the frequency
	// check; TestAMutationCannotOutrunTheBoard judges that path.
	// What the board can serve still travels.
	if out := m.runOTA("mc", "air:test", "air:test", "set tx 6"); out != "OK" {
		t.Errorf("sound value got %q", out)
	}
	if o := <-m.air; o.set["tx_power_dbm"] != "6" {
		t.Errorf("order = %+v", o)
	}
}

func TestProfileMovesBeforeItsOverrides(t *testing.T) {
	// "set profile=x k=v" on one line: the profile decides which scope
	// the override lands in, so it must apply first whatever order the
	// map yields — this used to be a coin flip per invocation.
	for range 20 {
		next := &config.File{Relays: map[string]config.Relay{"mc": {
			Protocol: "meshcore", Radio: "r",
			Layered: config.Layered{Profile: "eu-868-narrow"},
		}}}
		change, err := applyRelayChanges(next, "mc", map[string]any{
			"profile":   "custom",
			"node_name": "new name",
		}, nil)
		if err != nil {
			t.Fatal(err)
		}
		rc := next.Relays["mc"]
		if rc.Layered.Profile != "custom" {
			t.Fatalf("profile = %q", rc.Layered.Profile)
		}
		if v := rc.Layered.Overrides["custom"]["node_name"]; v != "new name" {
			t.Fatalf("override landed in %v, want the NEW profile's scope", rc.Layered.Overrides)
		}
		if len(change) != 2 {
			t.Fatalf("change = %v", change)
		}
	}
}

func TestSocketPathRefusesWhatIsNotASocket(t *testing.T) {
	// The instance lock says no daemon owns this config; it says
	// nothing about what sits at the socket path. Deleting it blind
	// destroyed whatever it was — the config database included, when
	// the two paths were set equal.
	//
	// Bound from inside the directory rather than by its full path: a
	// unix address carries the path in a fixed array — 104 bytes on
	// darwin, 108 on linux — and macOS hands out temporary
	// directories under /var/folders/... long enough to overflow it.
	// bind resolves a relative path against the working directory, so
	// only "console.sock" travels in the address.
	t.Chdir(t.TempDir())
	path := "console.sock"
	if err := os.WriteFile(path, []byte("somebody's data"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := listenConsole(context.Background(), path); err == nil ||
		!strings.Contains(err.Error(), "not a socket") {
		t.Fatalf("a regular file at the socket path: %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("the file was destroyed anyway: %v", err)
	}
	// A stale socket IS a previous life's leftover: replaced quietly.
	ln, err := listenConsole(context.Background(), path+"2")
	if err != nil {
		t.Fatal(err)
	}
	_ = ln.Close() // leaves the socket file behind, stale
	ln, err = listenConsole(context.Background(), path+"2")
	if err != nil {
		t.Fatalf("a stale socket was refused: %v", err)
	}
	_ = ln.Close()
}

func TestALiveSocketIsNotALeftover(t *testing.T) {
	// The instance lock proves nobody shares our config — nothing
	// more. A daemon on another base, or another program entirely,
	// may own this path; unlinking its live socket silently cut every
	// client off it.
	//
	// Bound from inside the directory, for the reason
	// TestSocketPathRefusesWhatIsNotASocket gives.
	t.Chdir(t.TempDir())
	path := "console.sock"
	first, err := listenConsole(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = first.Close() }()
	go func() {
		for {
			c, err := first.Accept()
			if err != nil {
				return
			}
			_ = c.Close()
		}
	}()
	if _, err := listenConsole(context.Background(), path); err == nil ||
		!strings.Contains(err.Error(), "alive") {
		t.Fatalf("a live socket was replaced: %v", err)
	}
}

func TestAnUnprobeableSocketIsLeftStanding(t *testing.T) {
	// EACCES, a timeout, a cancelled context — none of them proves
	// the absence of a listener. Only ECONNREFUSED does; everything
	// else must refuse the bind and leave the socket exactly where
	// its possible owner put it.
	if os.Geteuid() == 0 {
		t.Skip("root ignores socket modes; the probe cannot fail with EACCES")
	}
	// Bound from inside the directory, for the reason
	// TestSocketPathRefusesWhatIsNotASocket gives.
	t.Chdir(t.TempDir())
	path := "console.sock"
	first, err := listenConsole(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = first.Close() }()
	if err := os.Chmod(path, 0o000); err != nil {
		t.Fatal(err)
	}
	if _, err := listenConsole(context.Background(), path); err == nil ||
		!strings.Contains(err.Error(), "cannot be probed") {
		t.Fatalf("an unprobeable live socket was replaced: %v", err)
	}
	if _, err := os.Lstat(path); err != nil {
		t.Fatalf("the live socket was unlinked anyway: %v", err)
	}
}
