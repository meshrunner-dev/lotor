package sx126x

import (
	"strings"
	"testing"

	"meshrunner.dev/lotor/internal/radio"
	"meshrunner.dev/pkg/lora/sx126x"
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
		// The synthesiser's own range, the remediation review's residual:
		// a bound only Configure knew let 100 MHz pass the dry run.
		{"below the synthesiser", func(w *radio.Waveform) { w.FrequencyHz = 149_999_999 }, false},
		{"at the synthesiser floor", func(w *radio.Waveform) { w.FrequencyHz = 150_000_000 }, true},
		{"at the synthesiser ceiling", func(w *radio.Waveform) { w.FrequencyHz = 960_000_000 }, true},
		{"above the synthesiser", func(w *radio.Waveform) { w.FrequencyHz = 960_000_001 }, false},
	}
	for _, c := range cases {
		w := meshcoreWaveform()
		c.bend(&w)
		err := CheckWaveform(w)
		if (err == nil) != c.ok {
			t.Errorf("%s: CheckWaveform = %v, want ok=%v", c.name, err, c.ok)
		}
		// The equivalence itself: the judgement Configure runs agrees
		// with the preflight, waveform for waveform — the library's
		// whole judgement, not the modulation half alone.
		p, perr := paramsFrom(w)
		converts := perr == nil && sx126x.ValidateParams(p) == nil
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

func TestThePowerCeilingIsJudgedAgainstThePart(t *testing.T) {
	// A ceiling the part cannot reach, or an auto that resolves to
	// one, used to pass every door and fail at the first frame.
	cases := []struct {
		name string
		chip string
		cap  any
		want string // empty: accepted
	}{
		{"the lab board", chipSX1262, 22, ""},
		{"sx1262 at its floor", chipSX1262, -9, ""},
		{"sx1262 below its floor", chipSX1262, -10, "outside the sx1262 range"},
		{"sx1262 above its ceiling", chipSX1262, 23, "outside the sx1262 range"},
		{"the impossible 127", chipSX1262, 127, "outside the sx1262 range"},
		{"sx1261 stops at 15", chipSX1261, 22, "outside the sx1261 range"},
		{"sx1261 at 15", chipSX1261, 15, ""},
		{"a ceiling of exactly 0 dBm", chipSX1262, 0, ""},
	}
	for _, c := range cases {
		cfg := board()
		cfg["chip"] = c.chip
		cfg["max_tx_power_dbm"] = c.cap
		err := checkTransmit(cfg)
		switch {
		case c.want == "" && err != nil:
			t.Errorf("%s: refused: %v", c.name, err)
		case c.want != "" && err == nil:
			t.Errorf("%s: accepted", c.name)
		case c.want != "" && err != nil && !strings.Contains(err.Error(), c.want):
			t.Errorf("%s: refused for the wrong reason: %v", c.name, err)
		}
	}

	// The driver library's sentinel is not a power: write the plain
	// figure instead of the value that means something else there.
	cfg := board()
	cfg["max_tx_power_dbm"] = -128
	if _, err := Inspect(cfg); err == nil || !strings.Contains(err.Error(), "sentinel") {
		t.Errorf("the -128 sentinel was taken as a power: %v", err)
	}

	// Absent and zero now read differently, which is the whole point:
	// a board topping out at 0 dBm is a board, and one that declared
	// nothing is receive-only.
	absent := board()
	delete(absent, "max_tx_power_dbm")
	env, err := Inspect(absent)
	if err != nil {
		t.Fatal(err)
	}
	if env.MaxTxPowerSet {
		t.Error("an undeclared ceiling reads as declared")
	}
	if err := checkTransmit(absent); err == nil {
		t.Error("transmit was allowed with no ceiling declared")
	}
	zero := board()
	zero["max_tx_power_dbm"] = 0
	if env, err = Inspect(zero); err != nil {
		t.Fatal(err)
	}
	if !env.MaxTxPowerSet || env.MaxTxPowerDBm != 0 {
		t.Errorf("a 0 dBm ceiling reads as %+v", env)
	}
	// And the part's own range travels with it, so a resolved auto is
	// judged before a frame discovers it.
	if env.ChipMinDBm != -9 || env.ChipMaxDBm != 22 {
		t.Errorf("chip range = %d..%d", env.ChipMinDBm, env.ChipMaxDBm)
	}
}

func TestAutoResolvesAgainstThePart(t *testing.T) {
	// auto takes the board's ceiling, so an impossible ceiling is an
	// impossible power — judged here, not at the first keying.
	w := meshcoreWaveform()
	env := radio.Envelope{
		MaxTxPowerDBm: 22, MaxTxPowerSet: true, ChipMinDBm: -9, ChipMaxDBm: 22,
	}
	if err := env.Permits(w, 0, false); err != nil {
		t.Errorf("auto under a reachable ceiling: %v", err)
	}
	if err := env.Permits(w, -5, true); err != nil {
		t.Errorf("an explicit power inside the range: %v", err)
	}
	if err := env.Permits(w, -10, true); err == nil {
		t.Error("an explicit power below the part's floor was allowed")
	}
	// A ceiling no part can key: auto resolves to it and is refused.
	impossible := radio.Envelope{
		MaxTxPowerDBm: 127, MaxTxPowerSet: true, ChipMinDBm: -9, ChipMaxDBm: 22,
	}
	err := impossible.Permits(w, 0, false)
	if err == nil {
		t.Fatal("auto resolved to 127 dBm unchallenged")
	}
	if !strings.Contains(err.Error(), "auto resolves") {
		t.Errorf("refusal does not name the resolution: %v", err)
	}
	// With no chip range published, nothing is checked against it.
	unknown := radio.Envelope{MaxTxPowerDBm: 127, MaxTxPowerSet: true}
	if err := unknown.Permits(w, 0, false); err != nil {
		t.Errorf("an undeclared part was judged anyway: %v", err)
	}
}

func TestLibraryTxCapSpeaksTheDriverDialect(t *testing.T) {
	// The one translation to the library's vocabulary, where zero
	// means "transmit disabled" and a named sentinel carries the
	// 0 dBm ceiling. It lives at the seam so the configuration can
	// mean the plain thing — and it deserves its own proof.
	cap22, capZero := int8(22), int8(0)
	cases := []struct {
		name string
		s    Settings
		want int8
	}{
		{"undeclared disables transmit", Settings{}, 0},
		{"a plain ceiling passes through", Settings{MaxTxPowerDBm: &cap22}, 22},
		{"a zero ceiling becomes the sentinel", Settings{MaxTxPowerDBm: &capZero}, sx126x.MaxTxPowerZero},
	}
	for _, c := range cases {
		if got := c.s.libraryTxCap(); got != c.want {
			t.Errorf("%s: %d, want %d", c.name, got, c.want)
		}
	}
}

func TestGpiochipSpellingsCollapse(t *testing.T) {
	// The GPIO library reads "gpiochip0" and "/dev/gpiochip0" as the
	// same chip; two spellings of one line used to pass the
	// uniqueness check and fail at acquisition.
	cfg := board()
	cfg["reset_pin"] = "gpiochip0:16"
	cfg["busy_pin"] = "/dev/gpiochip0:16"
	if _, err := Inspect(cfg); err == nil {
		t.Fatal("two spellings of the same GPIO line were accepted")
	} else if !strings.Contains(err.Error(), "one line serves one role") {
		t.Errorf("refused for the wrong reason: %v", err)
	}
	// And the canonical form is what settings carry from then on.
	ok := board()
	ok["reset_pin"] = "/dev/gpiochip1:16"
	s, err := settingsFrom(ok)
	if err != nil {
		t.Fatal(err)
	}
	if s.ResetPin.Chip != "gpiochip1" {
		t.Errorf("chip spelled %q after resolve", s.ResetPin.Chip)
	}
}
