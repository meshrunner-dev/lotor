package main

import (
	"strings"
	"testing"

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
