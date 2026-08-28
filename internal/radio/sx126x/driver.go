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

	// GPIOChip is the board's chip: what a pin means when it names no
	// chip of its own. Every line below is a Pin, so any one of them
	// may sit on another chip where the board wires it that way.
	//
	// The three the chip cannot run without are pointers, so that
	// absent and zero read differently: offset 0 is a real line on
	// every chip, and a configuration that forgot to name a pin must
	// be told so rather than handed whatever line 0 is wired to.
	GPIOChip string `yaml:"gpiochip"`
	ResetPin *Pin   `yaml:"reset_pin"`
	BusyPin  *Pin   `yaml:"busy_pin"`
	DIO1Pin  *Pin   `yaml:"dio1_pin"`

	// EnablePins are held high for the board's whole session (front-end
	// module enables); the chip steers TX/RX itself when DIO2RFSwitch
	// is set.
	EnablePins   []Pin  `yaml:"enable_pins"`
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
	// From here every pin names its chip, so nothing downstream has to
	// remember which default applied.
	if s.ResetPin, err = requirePin("reset_pin", s.ResetPin, s.GPIOChip); err != nil {
		return s, err
	}
	if s.BusyPin, err = requirePin("busy_pin", s.BusyPin, s.GPIOChip); err != nil {
		return s, err
	}
	if s.DIO1Pin, err = requirePin("dio1_pin", s.DIO1Pin, s.GPIOChip); err != nil {
		return s, err
	}
	for i := range s.EnablePins {
		s.EnablePins[i] = s.EnablePins[i].Resolve(s.GPIOChip)
	}
	return s, nil
}

// requirePin insists the line was named — the chip does not run
// without it — and fills in the board's chip when the pin named none.
func requirePin(name string, p *Pin, chip string) (*Pin, error) {
	if p == nil {
		return nil, fmt.Errorf("sx126x-spi settings: %s is required", name)
	}
	resolved := p.Resolve(chip)
	return &resolved, nil
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
