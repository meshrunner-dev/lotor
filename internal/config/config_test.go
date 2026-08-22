package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
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
