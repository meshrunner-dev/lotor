// Package sx126x adapts the SX126x SPI driver library to the daemon's
// radio seam: it decodes the resolved hardware configuration strictly,
// opens the chip (the library verifies the part answers before
// anything else runs), and exposes the board's envelope.
package sx126x

import (
	"errors"
	"fmt"
	"time"

	"meshrunner.dev/lotor/internal/config"
	"meshrunner.dev/lotor/internal/radio"
)

// Settings is the driver's hardware configuration: the attachment
// (bus, pins) and the board's envelope. Waveform choices do not
// belong here — they arrive per relay through Configure.
type Settings struct {
	SPI   string `yaml:"spi"`
	SPIHz uint32 `yaml:"spi_hz"`

	GPIOChip string `yaml:"gpiochip"`
	ResetPin int    `yaml:"reset_pin"`
	BusyPin  int    `yaml:"busy_pin"`
	DIO1Pin  int    `yaml:"dio1_pin"`

	// EnablePins are held high for the board's whole session (front-end
	// module enables); the chip steers TX/RX itself when DIO2RFSwitch
	// is set.
	EnablePins   []int  `yaml:"enable_pins"`
	DIO2RFSwitch bool   `yaml:"dio2_rf_switch"`
	TCXO         string `yaml:"tcxo"` // "", "1.6", "1.8", "3.3", ...
	DCDC         bool   `yaml:"dcdc"`
	RXBoosted    bool   `yaml:"rx_boosted"`

	// Chip names the exact part (sx1261/sx1262/sx1268); required for
	// transmit later, optional while receive-only.
	Chip string `yaml:"chip"`

	// DIO1Watchdog optionally re-polls the chip while the receive loop
	// sleeps between noise-floor batches. Off by default: the DIO1
	// level check already catches every event that happened before the
	// sleep, so this only insures a DIO1 transition degraded
	// electrically while asleep — a board-wiring doubt, which is why
	// the knob lives here, next to the pin it guards.
	DIO1Watchdog time.Duration `yaml:"dio1_watchdog"`

	// Envelope: what the board physically allows. Zero means
	// undeclared at the seam — and, passed through to the driver
	// library, transmit disabled: the two readings agree for a
	// receive-only daemon, and a transmit path will require the cap
	// to be declared.
	MaxTxPowerDBm    int8     `yaml:"max_tx_power_dbm"`
	FrequencyRangeHz []uint32 `yaml:"frequency_range"`
}

// The parts this driver's transmit tables cover. The SX126x version
// register cannot tell them apart, so the integrator declares which
// one is on the board — and the PA tables differ destructively
// between them.
const (
	chipSX1261 = "sx1261"
	chipSX1262 = "sx1262"
	chipSX1268 = "sx1268"
)

var knownChips = map[string]bool{chipSX1261: true, chipSX1262: true, chipSX1268: true}

// checkTransmit reports why this radio could not key as configured.
func checkTransmit(cfg map[string]any) error {
	s, err := settingsFrom(cfg)
	if err != nil {
		return err
	}
	if s.Chip == "" {
		return errors.New(
			"sx126x-spi: transmitting needs chip: (" + chipSX1261 + ", " + chipSX1262 +
				" or " + chipSX1268 + ") — " +
				"the version register cannot identify the part, and the PA tables differ")
	}
	if !knownChips[s.Chip] {
		return fmt.Errorf("sx126x-spi: unknown chip %q", s.Chip)
	}
	if s.MaxTxPowerDBm == 0 {
		return errors.New("sx126x-spi: transmitting needs max_tx_power_dbm declared")
	}
	return nil
}

func settingsFrom(cfg map[string]any) (Settings, error) {
	s, err := config.Decode[Settings](cfg)
	if err != nil {
		return s, fmt.Errorf("sx126x-spi settings: %w", err)
	}
	if s.SPI == "" {
		return s, errors.New("sx126x-spi settings: spi device path is required")
	}
	if len(s.FrequencyRangeHz) != 0 && len(s.FrequencyRangeHz) != 2 {
		return s, errors.New("sx126x-spi settings: frequency_range wants [low, high]")
	}
	if s.SPIHz == 0 {
		s.SPIHz = 2_000_000
	}
	if s.GPIOChip == "" {
		s.GPIOChip = "gpiochip0"
	}
	return s, nil
}

// Inspect is the driver's config dry run: strict decode plus the
// board's envelope, no hardware touched.
func Inspect(cfg map[string]any) (radio.Envelope, error) {
	s, err := settingsFrom(cfg)
	if err != nil {
		return radio.Envelope{}, err
	}
	return s.envelope(), nil
}

func (s Settings) envelope() radio.Envelope {
	e := radio.Envelope{MaxTxPowerDBm: s.MaxTxPowerDBm}
	if len(s.FrequencyRangeHz) == 2 {
		e.FreqRangeLowHz, e.FreqRangeHiHz = s.FrequencyRangeHz[0], s.FrequencyRangeHz[1]
	}
	return e
}
