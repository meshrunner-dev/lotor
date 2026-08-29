package sx126x

import (
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestPinReadsBothForms(t *testing.T) {
	for _, c := range []struct {
		text string
		want Pin
	}{
		// The form every preset and every configuration written before
		// the grammar existed speaks.
		{"16", Pin{Offset: 16}},
		{"0", Pin{Offset: 0}},
		// The form a split board needs.
		{"gpiochip1:25", Pin{Chip: "gpiochip1", Offset: 25}},
		{"gpio2:3", Pin{Chip: "gpio2", Offset: 3}},
	} {
		var p Pin
		if err := yaml.Unmarshal([]byte(c.text), &p); err != nil {
			t.Fatalf("pin %q: %v", c.text, err)
		}
		if p != c.want {
			t.Errorf("pin %q = %+v, want %+v", c.text, p, c.want)
		}
	}
}

func TestPinRefusesWhatItCannotMean(t *testing.T) {
	for _, text := range []string{
		"banana",
		"gpiochip1:xx",
		":25",  // a colon promising a chip that is not there
		"-1",   // a line offset is not negative
		"1:-2", // nor when it is qualified
	} {
		var p Pin
		if err := yaml.Unmarshal([]byte(text), &p); err == nil {
			t.Errorf("pin %q parsed as %+v", text, p)
		} else if !strings.Contains(err.Error(), "pin:") {
			t.Errorf("pin %q: %v — the error does not name the grammar", text, err)
		}
	}
}

func TestSettingsResolveTheBoardChip(t *testing.T) {
	// A pin naming no chip takes the board's; one that names its own
	// keeps it. The preset's ints and a split line in one config.
	s, err := settingsFrom(map[string]any{
		"spi":         "/dev/spidev0.0",
		"gpiochip":    "gpiochip0",
		"reset_pin":   "gpiochip1:25",
		"busy_pin":    12,
		"dio1_pin":    5,
		"enable_pins": []any{13, "gpio2:3"},
	})
	if err != nil {
		t.Fatalf("settings: %v", err)
	}
	if got := *s.ResetPin; got != (Pin{Chip: "gpiochip1", Offset: 25}) {
		t.Errorf("reset pin = %+v", got)
	}
	if got := *s.BusyPin; got != (Pin{Chip: "gpiochip0", Offset: 12}) {
		t.Errorf("busy pin = %+v", got)
	}
	if got := *s.DIO1Pin; got != (Pin{Chip: "gpiochip0", Offset: 5}) {
		t.Errorf("dio1 pin = %+v", got)
	}
	want := []Pin{{Chip: "gpiochip0", Offset: 13}, {Chip: "gpio2", Offset: 3}}
	if len(s.EnablePins) != len(want) {
		t.Fatalf("enable pins = %+v", s.EnablePins)
	}
	for i := range want {
		if s.EnablePins[i] != want[i] {
			t.Errorf("enable pin %d = %+v, want %+v", i, s.EnablePins[i], want[i])
		}
	}
}

func TestBoardChipDefaultsWhenNobodySaid(t *testing.T) {
	s, err := settingsFrom(map[string]any{
		"spi": "/dev/spidev0.0", "reset_pin": 16, "busy_pin": 24, "dio1_pin": 22,
	})
	if err != nil {
		t.Fatalf("settings: %v", err)
	}
	if *s.ResetPin != (Pin{Chip: "gpiochip0", Offset: 16}) {
		t.Errorf("reset pin = %+v", *s.ResetPin)
	}
}

func TestAPinNobodyNamedIsRefused(t *testing.T) {
	// Offset 0 is a real line on every chip, so an absent pin cannot
	// quietly become one: it is the wrong line on most boards, and the
	// symptom is a radio that never answers.
	base := map[string]any{
		"spi": "/dev/spidev0.0", "reset_pin": 16, "busy_pin": 24, "dio1_pin": 22,
	}
	for _, missing := range []string{"reset_pin", "busy_pin", "dio1_pin"} {
		cfg := map[string]any{}
		for k, v := range base {
			if k != missing {
				cfg[k] = v
			}
		}
		_, err := settingsFrom(cfg)
		if err == nil {
			t.Errorf("settings without %s were accepted", missing)
		} else if !strings.Contains(err.Error(), missing+" is required") {
			t.Errorf("without %s: %v", missing, err)
		}
	}
}
