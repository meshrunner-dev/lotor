package sx126x

import (
	"strings"
	"testing"

	"meshrunner.dev/lotor/internal/radio"
)

// board is a configuration the preset would produce, for a test to
// bend one field of.
func board() map[string]any {
	return map[string]any{
		"spi":              "/dev/spidev0.0",
		"gpiochip":         "gpiochip0",
		"reset_pin":        16,
		"busy_pin":         24,
		"dio1_pin":         22,
		"enable_pins":      []any{12, 13},
		"chip":             chipSX1262,
		"tcxo":             "1.8",
		"max_tx_power_dbm": 22,
		"frequency_range":  []any{850_000_000, 930_000_000},
	}
}

// meshcoreWaveform is the band this daemon actually runs.
func meshcoreWaveform() radio.Waveform {
	return radio.Waveform{
		FrequencyHz: 869_618_000, SpreadingFactor: 8, BandwidthHz: 62_500,
		CodingRate: 8, Preamble: 32, SyncWord: 0x12, CRC: true,
	}
}

func TestWaveformCheckMatchesConfigure(t *testing.T) {
	// The preflight and the hardware conversion must accept and refuse
	// exactly the same waveforms: what passes a dry run and then fails
	// at every Configure is a relay that opens its radio, fails,
	// closes and starts over for as long as the configuration stands.
	cases := []struct {
		name string
		bend func(*radio.Waveform)
		ok   bool
	}{
		{"the band we run", func(*radio.Waveform) {}, true},
		{"sf 5, the low bound", func(w *radio.Waveform) { w.SpreadingFactor = 5 }, true},
		{"sf 12, the high bound", func(w *radio.Waveform) { w.SpreadingFactor = 12 }, true},
		{"sf 4, below", func(w *radio.Waveform) { w.SpreadingFactor = 4 }, false},
		{"sf 13, above", func(w *radio.Waveform) { w.SpreadingFactor = 13 }, false},
		{"bandwidth off the table", func(w *radio.Waveform) { w.BandwidthHz = 12345 }, false},
		{"coding rate 4/4", func(w *radio.Waveform) { w.CodingRate = 4 }, false},
		{"coding rate 4/9", func(w *radio.Waveform) { w.CodingRate = 9 }, false},
		{"no preamble", func(w *radio.Waveform) { w.Preamble = 0 }, false},
		// The trap: -1 narrowed to uint16 is 65535, a length the chip
		// runs happily — a different network from the written one.
		{"negative preamble", func(w *radio.Waveform) { w.Preamble = -1 }, false},
		{"preamble past the field", func(w *radio.Waveform) { w.Preamble = 65536 }, false},
		{"preamble at the field's edge", func(w *radio.Waveform) { w.Preamble = 65535 }, true},
		{"no sync word", func(w *radio.Waveform) { w.SyncWord = 0 }, false},
		{"no frequency", func(w *radio.Waveform) { w.FrequencyHz = 0 }, false},
	}
	for _, c := range cases {
		w := meshcoreWaveform()
		c.bend(&w)
		err := CheckWaveform(w)
		if (err == nil) != c.ok {
			t.Errorf("%s: CheckWaveform = %v, want ok=%v", c.name, err, c.ok)
		}
		// The equivalence itself: the conversion Configure runs agrees
		// with the preflight, waveform for waveform.
		p, perr := paramsFrom(w)
		converts := perr == nil && p.Validate() == nil
		if converts != c.ok {
			t.Errorf("%s: the hardware conversion says %v, the preflight says %v",
				c.name, converts, c.ok)
		}
	}
}

func TestBoardSettingsAreJudgedWithoutHardware(t *testing.T) {
	// Everything Open would refuse deterministically belongs here,
	// where a mutation can still be turned down: past this point the
	// only failures left are hardware's own.
	cases := []struct {
		name string
		bend func(map[string]any)
		want string // empty: accepted
	}{
		{"the preset", func(map[string]any) {}, ""},
		{"unknown chip", func(c map[string]any) { c["chip"] = "sx999" }, "unknown chip"},
		{"no chip is receive-only, not an error", func(c map[string]any) { delete(c, "chip") }, ""},
		{"unsupported tcxo rail", func(c map[string]any) { c["tcxo"] = "2.5" }, "tcxo"},
		{"no tcxo", func(c map[string]any) { delete(c, "tcxo") }, ""},
		{"negative watchdog", func(c map[string]any) { c["dio1_watchdog"] = "-1s" }, "cadence is positive"},
		{"zero watchdog leaves it off", func(c map[string]any) { c["dio1_watchdog"] = "0s" }, ""},
		{"spi clock past the part", func(c map[string]any) { c["spi_hz"] = 20_000_000 }, "specified to"},
		{"spi clock at the ceiling", func(c map[string]any) { c["spi_hz"] = 16_000_000 }, ""},
		{"inverted frequency range", func(c map[string]any) {
			c["frequency_range"] = []any{930_000_000, 850_000_000}
		}, "inverted"},
		{"enable pin duplicating reset", func(c map[string]any) {
			c["enable_pins"] = []any{16}
		}, "one line serves one role"},
		{"two roles on one line", func(c map[string]any) { c["busy_pin"] = 22 }, "one line serves one role"},
		{"enable pins colliding with each other", func(c map[string]any) {
			c["enable_pins"] = []any{12, 12}
		}, "one line serves one role"},
	}
	for _, c := range cases {
		cfg := board()
		c.bend(cfg)
		_, err := Inspect(cfg)
		switch {
		case c.want == "" && err != nil:
			t.Errorf("%s: refused: %v", c.name, err)
		case c.want != "" && err == nil:
			t.Errorf("%s: accepted", c.name)
		case c.want != "" && err != nil && !strings.Contains(err.Error(), c.want):
			t.Errorf("%s: refused for the wrong reason: %v", c.name, err)
		}
	}
}

func TestTheThreeDoorsAgree(t *testing.T) {
	// Inspect, CheckTransmit and the settings Open resolves are one
	// judgement: a board Inspect accepts and Open cannot use is the
	// gap this closes.
	cfg := board()
	cfg["chip"] = "sx999"
	if _, err := Inspect(cfg); err == nil {
		t.Error("Inspect accepted a chip no PA table covers")
	}
	if err := checkTransmit(cfg); err == nil {
		t.Error("CheckTransmit accepted it")
	}
	if _, err := settingsFrom(cfg); err == nil {
		t.Error("the settings Open resolves accepted it")
	}
	// And a sound board passes all three.
	sound := board()
	if _, err := Inspect(sound); err != nil {
		t.Errorf("Inspect: %v", err)
	}
	if err := checkTransmit(sound); err != nil {
		t.Errorf("CheckTransmit: %v", err)
	}
	s, err := settingsFrom(sound)
	if err != nil {
		t.Fatalf("settingsFrom: %v", err)
	}
	if s.DIO1Watchdog != 0 {
		t.Errorf("watchdog = %v, want off by default", s.DIO1Watchdog)
	}
	if s.SPIHz != 2_000_000 {
		t.Errorf("spi_hz = %d, want the 2 MHz default", s.SPIHz)
	}
}
