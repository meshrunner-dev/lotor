package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

var catalog = map[string]map[string]any{
	"board-a": {"spi": "/dev/spidev0.0", "reset_pin": 16, "max_tx_power_dbm": 22},
	"board-b": {"spi": "/dev/spidev1.0", "reset_pin": 5},
}

func TestResolveMergesProfileAndItsScope(t *testing.T) {
	l := Layered{
		Profile: "board-a",
		Overrides: map[string]map[string]any{
			"board-a": {"reset_pin": 99},
			"board-b": {"reset_pin": 42}, // other scope: must not leak
		},
	}
	got, traces, err := l.Resolve(catalog)
	if err != nil {
		t.Fatal(err)
	}
	if got["reset_pin"] != 99 {
		t.Errorf("reset_pin = %v, want the override 99", got["reset_pin"])
	}
	if got["spi"] != "/dev/spidev0.0" {
		t.Errorf("spi = %v, want the preset value", got["spi"])
	}
	provenance := map[string]string{}
	for _, tr := range traces {
		provenance[tr.Key] = tr.Source
	}
	if provenance["reset_pin"] != "override:board-a" {
		t.Errorf("reset_pin provenance = %s", provenance["reset_pin"])
	}
	if provenance["spi"] != "profile:board-a" {
		t.Errorf("spi provenance = %s", provenance["spi"])
	}
}

func TestSwitchingProfilesCarriesNothingOver(t *testing.T) {
	l := Layered{
		Profile: "board-b",
		Overrides: map[string]map[string]any{
			"board-a": {"reset_pin": 99},
		},
	}
	got, _, err := l.Resolve(catalog)
	if err != nil {
		t.Fatal(err)
	}
	if got["reset_pin"] != 5 {
		t.Errorf("reset_pin = %v: board-a's override leaked into board-b", got["reset_pin"])
	}
}

func TestCustomProfileStartsEmpty(t *testing.T) {
	l := Layered{
		Profile: "custom",
		Overrides: map[string]map[string]any{
			"custom": {"spi": "/dev/spidev9.9"},
		},
	}
	got, _, err := l.Resolve(catalog)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got["spi"] != "/dev/spidev9.9" {
		t.Errorf("custom resolve = %v", got)
	}
}

func TestUnknownProfileAndScopeAreErrors(t *testing.T) {
	if _, _, err := (Layered{Profile: "board-x"}).Resolve(catalog); err == nil {
		t.Error("unknown profile accepted")
	}
	l := Layered{
		Profile:   "board-a",
		Overrides: map[string]map[string]any{"board-x": {"k": 1}},
	}
	if _, _, err := l.Resolve(catalog); err == nil {
		t.Error("unknown override scope accepted")
	}
}

func TestDecodeRejectsUnknownKeys(t *testing.T) {
	type dst struct {
		SPI string `yaml:"spi"`
	}
	if _, err := Decode[dst](map[string]any{"spi": "x", "spy": "typo"}); err == nil {
		t.Error("unknown key accepted")
	}
	d, err := Decode[dst](map[string]any{"spi": "x"})
	if err != nil || d.SPI != "x" {
		t.Errorf("decode = %+v, %v", d, err)
	}
}

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestLoadValidatesWiring(t *testing.T) {
	good := `
radios:
  slot1:
    driver: sx126x-spi
    profile: custom
relays:
  mesh:
    protocol: meshcore
    radio: slot1
`
	if _, err := Load(writeConfig(t, good)); err != nil {
		t.Fatalf("valid config refused: %v", err)
	}

	cases := map[string]string{
		"dangling radio": `
radios: {}
relays:
  mesh: {protocol: meshcore, radio: nope}
`,
		"two relays, one radio": `
radios:
  slot1: {driver: sx126x-spi}
relays:
  a: {protocol: meshcore, radio: slot1}
  b: {protocol: meshcore, radio: slot1}
`,
		"unknown top-level key": `
radioz: {}
relays:
  mesh: {protocol: meshcore, radio: slot1}
`,
		"missing driver": `
radios:
  slot1: {}
relays:
  mesh: {protocol: meshcore, radio: slot1}
`,
	}
	for name, body := range cases {
		if _, err := Load(writeConfig(t, body)); err == nil {
			t.Errorf("%s: accepted", name)
		} else if strings.Contains(err.Error(), "panic") {
			t.Errorf("%s: %v", name, err)
		}
	}
}

func TestBareBlocksAreNotSilent(t *testing.T) {
	// A bare cli: key means "with defaults", never "nothing".
	withCLI := `
radios:
  slot1: {driver: sx126x-spi}
relays:
  mesh: {protocol: meshcore, radio: slot1}
cli:
`
	f, err := Load(writeConfig(t, withCLI))
	if err != nil {
		t.Fatal(err)
	}
	if f.CLI == nil || f.CLI.Listen != DefaultCLIListen {
		t.Errorf("bare cli: block gave %+v", f.CLI)
	}
	// A bare sentinel: key cannot guess a journal path: loud error.
	withSentinel := `
radios:
  slot1: {driver: sx126x-spi}
relays:
  mesh: {protocol: meshcore, radio: slot1}
sentinel:
`
	if _, err := Load(writeConfig(t, withSentinel)); err == nil ||
		!strings.Contains(err.Error(), "journal") {
		t.Errorf("bare sentinel: block: %v", err)
	}
}

func TestMultipleDocumentsAreRefused(t *testing.T) {
	body := `
radios:
  slot1: {driver: sx126x-spi}
relays:
  mesh: {protocol: meshcore, radio: slot1}
---
radios: {}
`
	if _, err := Load(writeConfig(t, body)); err == nil ||
		!strings.Contains(err.Error(), "documents") {
		t.Errorf("second document: %v", err)
	}
}

func TestTinyRetentionIsRefused(t *testing.T) {
	body := `
radios:
  slot1: {driver: sx126x-spi}
relays:
  mesh: {protocol: meshcore, radio: slot1}
sentinel:
  journal: ":memory:"
  retention: 5s
`
	if _, err := Load(writeConfig(t, body)); err == nil {
		t.Error("5s retention accepted — every prune would wipe the journal")
	}
}

func TestCatalogCannotHijackCustom(t *testing.T) {
	l := Layered{Profile: "custom"}
	_, _, err := l.Resolve(map[string]map[string]any{"custom": {"spi": "hijacked"}})
	if err == nil {
		t.Error("a preset named custom was accepted")
	}
}

func TestConsoleSocketResolution(t *testing.T) {
	// No cli block: the local console is a base function, on by default.
	f := &File{}
	if path, explicit := f.ConsoleSocket(); path != DefaultConsoleSocket || explicit {
		t.Errorf("absent block resolves to %q explicit=%v", path, explicit)
	}
	// A bare block keeps the default too.
	f.CLI = &CLI{}
	if path, _ := f.ConsoleSocket(); path != DefaultConsoleSocket {
		t.Errorf("bare block resolves to %q", path)
	}
	// Explicitly empty disables; a set path is a kept promise.
	off, custom := "", "/tmp/x.sock"
	f.CLI.Socket = &off
	if path, explicit := f.ConsoleSocket(); path != "" || !explicit {
		t.Errorf("empty socket resolves to %q explicit=%v", path, explicit)
	}
	f.CLI.Socket = &custom
	if path, explicit := f.ConsoleSocket(); path != custom || !explicit {
		t.Errorf("custom socket resolves to %q explicit=%v", path, explicit)
	}
}

func TestTXBlockNormalizes(t *testing.T) {
	tx := &TX{}
	if err := tx.Normalize(); err != nil || tx.Mode != TXDry || tx.LBTExhausted != "transmit" {
		t.Fatalf("defaults = %+v, %v", tx, err)
	}
	if err := (&TX{Mode: "hot"}).Normalize(); err == nil {
		t.Error("unknown mode accepted")
	}
	if err := (&TX{LBTExhausted: "retry"}).Normalize(); err == nil {
		t.Error("unknown lbt_exhausted accepted")
	}
	if (&Relay{}).TXMode() != TXDry {
		t.Error("absent block should read dry")
	}
}

func TestTXRejectsANegativeThreshold(t *testing.T) {
	if err := (&TX{LBTThresholdDB: -6}).Normalize(); err == nil {
		t.Error("a negative lbt_threshold_db silently disables the RSSI stage")
	}
}

func TestASensorsCadenceIsBounded(t *testing.T) {
	// A bus shared with a radio is not a thing to read a thousand
	// times a second, and every other cadence here says its range.
	base := func(every time.Duration) *File {
		return &File{Sensors: map[string]Sensor{
			"bat": {Driver: "ina219", SampleInterval: every},
		}}
	}
	for _, c := range []struct {
		what  string
		every time.Duration
		ok    bool
	}{
		{"a millisecond hammers the bus", time.Millisecond, false},
		{"a day is not watching", 24 * time.Hour, false},
		{"backwards", -time.Second, false},
		{"the floor", MinSampleInterval, true},
		{"the ceiling", MaxSampleInterval, true},
		{"zero takes the default", 0, true},
		{"a working cadence", 30 * time.Second, true},
	} {
		err := base(c.every).Validate(false)
		if c.ok && err != nil {
			t.Errorf("%s: %v", c.what, err)
		}
		if !c.ok && err == nil {
			t.Errorf("%s: accepted %s", c.what, c.every)
		}
	}
}
