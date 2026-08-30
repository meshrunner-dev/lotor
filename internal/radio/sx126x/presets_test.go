package sx126x

import (
	"maps"
	"testing"

	"meshrunner.dev/lotor/internal/config"
)

func TestEveryPresetIsABoardThatInspects(t *testing.T) {
	// A preset plus the one thing it deliberately leaves to the
	// operator — the SPI node — must be a complete, judgeable board.
	// The composition goes through config.Decode, the same strict
	// door the real layering uses, so a preset holding a key the
	// schema no longer speaks fails here rather than at an operator's
	// first boot.
	for name, preset := range Presets() {
		cfg := map[string]any{"spi": "/dev/spidev0.0"}
		maps.Copy(cfg, preset)
		typed, err := config.Decode[map[string]any](cfg)
		if err != nil {
			t.Errorf("preset %s does not decode: %s", name, err)
			continue
		}
		env, err := Inspect(typed)
		if err != nil {
			t.Errorf("preset %s does not inspect: %s", name, err)
			continue
		}
		if !env.MaxTxPowerSet {
			t.Errorf("preset %s states no transmit ceiling", name)
		}
	}
}

func TestTheZero2WKitRidesTheSecondCharacterDevice(t *testing.T) {
	// This SoC's 288-line pinctrl is gpiochip1 to the character
	// device and base 0 to sysfs, so the numbers other daemons print
	// are this bank's offsets and nothing may resolve onto the
	// default chip — that one is the 32-line PL block, and a reset
	// driven there would toggle an unrelated line.
	cfg := map[string]any{"spi": "/dev/spidev1.0"}
	maps.Copy(cfg, Presets()["orangepi-zero2w-station-g3"])
	typed, err := config.Decode[map[string]any](cfg)
	if err != nil {
		t.Fatal(err)
	}
	s, err := settingsFrom(typed)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range []struct {
		name string
		pin  *Pin
		want int
	}{
		{"reset", s.ResetPin, 76},
		{"busy", s.BusyPin, 228},
		{"dio1", s.DIO1Pin, 261},
	} {
		if p.pin.Chip != "gpiochip1" || p.pin.Offset != p.want {
			t.Errorf("%s = %s:%d, want gpiochip1:%d", p.name, p.pin.Chip, p.pin.Offset, p.want)
		}
	}
}

func TestStationG3KitResolvesItsSplitBanks(t *testing.T) {
	// The kit's one peculiarity: reset lives on the SoC's second GPIO
	// bank while busy and DIO1 ride the default — the per-pin chip
	// spelling must survive the whole settings path, or the driver
	// would reset a line on the wrong bank.
	cfg := map[string]any{"spi": "/dev/spidev0.0"}
	maps.Copy(cfg, Presets()["lyra-zerow-station-g3"])
	typed, err := config.Decode[map[string]any](cfg)
	if err != nil {
		t.Fatal(err)
	}
	s, err := settingsFrom(typed)
	if err != nil {
		t.Fatal(err)
	}
	if s.ResetPin.Chip != "gpiochip1" || s.ResetPin.Offset != 25 {
		t.Errorf("reset = %s:%d, want gpiochip1:25", s.ResetPin.Chip, s.ResetPin.Offset)
	}
	if s.BusyPin.Chip != "gpiochip0" || s.BusyPin.Offset != 12 {
		t.Errorf("busy = %s:%d, want gpiochip0:12", s.BusyPin.Chip, s.BusyPin.Offset)
	}
	if s.DIO1Pin.Chip != "gpiochip0" || s.DIO1Pin.Offset != 5 {
		t.Errorf("dio1 = %s:%d, want gpiochip0:5", s.DIO1Pin.Chip, s.DIO1Pin.Offset)
	}
	env := s.envelope()
	if env.MaxTxPowerDBm != 22 || env.ChipMinDBm != -9 || env.ChipMaxDBm != 22 {
		t.Errorf("envelope = max %d chip %d..%d, want 22 within -9..22",
			env.MaxTxPowerDBm, env.ChipMinDBm, env.ChipMaxDBm)
	}
	if env.FreqRangeLowHz != 850_000_000 || env.FreqRangeHiHz != 930_000_000 {
		t.Errorf("band = %d..%d Hz", env.FreqRangeLowHz, env.FreqRangeHiHz)
	}
}
