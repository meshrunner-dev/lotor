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
	"meshrunner.dev/pkg/lora"
	"meshrunner.dev/pkg/lora/sx126x"
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

	// Envelope: what the board physically allows. The ceiling is a
	// pointer so that absent and zero read differently — a board
	// whose front end tops out at exactly 0 dBm is a real board, and
	// a lone zero cannot say whether it means that or means nobody
	// declared one. Absent leaves the relay receive-only: transmit
	// requires the integrator to commit to a figure.
	MaxTxPowerDBm    *int8    `yaml:"max_tx_power_dbm"`
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
	if s.MaxTxPowerDBm == nil {
		return errors.New("sx126x-spi: transmitting needs max_tx_power_dbm declared")
	}
	// A ceiling the part cannot reach is a relay that refuses its
	// first frame rather than its configuration.
	chip, err := chipFrom(s.Chip)
	if err != nil {
		return err
	}
	lo, hi := chip.PowerRange()
	if ceiling := *s.MaxTxPowerDBm; ceiling < lo || ceiling > hi {
		return fmt.Errorf("sx126x-spi: max_tx_power_dbm %d is outside the %s range (%d..%d)",
			ceiling, s.Chip, lo, hi)
	}
	return nil
}

// maxSPIHz is the SX126x bus ceiling from the datasheet. Past it the
// chip does not answer reliably, which reads as a dead radio rather
// than as the configuration error it is.
const maxSPIHz = 16_000_000

// settingsFrom is the canonical judgement of a board configuration:
// everything about it that can be decided without touching hardware.
// Inspect, CheckTransmit and Open all come through here, so what the
// preflight accepts is what Open can carry out — leaving Open with
// only the failures hardware really owns: an absent device, a line
// another process holds, a bus fault, the chip's own verdict.
func settingsFrom(cfg map[string]any) (Settings, error) {
	s, err := config.Decode[Settings](cfg)
	if err != nil {
		return s, fmt.Errorf("sx126x-spi settings: %w", err)
	}
	if s.SPI == "" {
		return s, errors.New("sx126x-spi settings: spi device path is required")
	}
	if s.SPIHz == 0 {
		s.SPIHz = 2_000_000
	}
	if s.SPIHz > maxSPIHz {
		return s, fmt.Errorf("sx126x-spi settings: spi_hz %d — the part is specified to %d",
			s.SPIHz, maxSPIHz)
	}
	// The library reads -128 in its own configuration as "a ceiling of
	// exactly 0 dBm". Here it would be a power of -128 dBm, which no
	// part keys: the two meanings must not meet silently.
	if s.MaxTxPowerDBm != nil && *s.MaxTxPowerDBm == sx126x.MaxTxPowerZero {
		return s, fmt.Errorf(
			"sx126x-spi settings: max_tx_power_dbm %d is the driver's sentinel for a 0 dBm ceiling — write 0",
			sx126x.MaxTxPowerZero)
	}
	if s.DIO1Watchdog < 0 {
		return s, fmt.Errorf(
			"sx126x-spi settings: dio1_watchdog %s — a cadence is positive, or zero to leave it off",
			s.DIO1Watchdog)
	}
	// The enums the hardware path resolves, decided here instead: an
	// unknown part or an unsupported TCXO rail is a configuration
	// error whatever the platform, and finding it only at Open makes
	// it a relay that can never start rather than a mutation refused.
	if _, err := chipFrom(s.Chip); err != nil {
		return s, fmt.Errorf("sx126x-spi settings: %w", err)
	}
	if _, err := tcxoFrom(s.TCXO); err != nil {
		return s, fmt.Errorf("sx126x-spi settings: %w", err)
	}
	if err := s.checkFrequencyRange(); err != nil {
		return s, err
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
	return s, s.checkPinsDistinct()
}

// checkFrequencyRange refuses an envelope no frequency can satisfy.
func (s Settings) checkFrequencyRange() error {
	switch {
	case len(s.FrequencyRangeHz) == 0:
		return nil
	case len(s.FrequencyRangeHz) != 2:
		return errors.New("sx126x-spi settings: frequency_range wants [low, high]")
	case s.FrequencyRangeHz[0] > s.FrequencyRangeHz[1]:
		return fmt.Errorf(
			"sx126x-spi settings: frequency_range %d..%d is inverted — no frequency is inside it",
			s.FrequencyRangeHz[0], s.FrequencyRangeHz[1])
	}
	return nil
}

// checkPinsDistinct refuses two roles wired to one line. Acquiring the
// second one fails deterministically at Open — the kernel hands a line
// to a single requester — so the configuration is wrong long before
// the hardware says so.
func (s Settings) checkPinsDistinct() error {
	roles := map[Pin]string{}
	claim := func(role string, p Pin) error {
		if other, taken := roles[p]; taken {
			return fmt.Errorf("sx126x-spi settings: %s and %s both name line %s — "+
				"one line serves one role", other, role, p)
		}
		roles[p] = role
		return nil
	}
	if err := claim("reset_pin", *s.ResetPin); err != nil {
		return err
	}
	if err := claim("busy_pin", *s.BusyPin); err != nil {
		return err
	}
	if err := claim("dio1_pin", *s.DIO1Pin); err != nil {
		return err
	}
	for i, p := range s.EnablePins {
		if err := claim(fmt.Sprintf("enable_pins[%d]", i), p); err != nil {
			return err
		}
	}
	return nil
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

// CheckWaveform is the driver's dry run over one channel choice: the
// very conversion Configure performs, and the library's own judgement
// of the result. Running it at preflight is what stops a waveform the
// chip cannot be programmed with from being persisted and handed to a
// relay that opens its radio, fails at every Configure, closes, and
// starts over after backoff for as long as the configuration stands.
func CheckWaveform(w radio.Waveform) error {
	p, err := paramsFrom(w)
	if err != nil {
		return fmt.Errorf("sx126x-spi waveform: %w", err)
	}
	// The library's own whole judgement — modulation and synthesiser
	// range both — not the modulation half alone: a frequency the
	// synthesiser cannot reach used to pass here and fail at every
	// Configure after the hardware was open.
	if err := sx126x.ValidateParams(p); err != nil {
		return fmt.Errorf("sx126x-spi waveform: %w", err)
	}
	return nil
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

// libraryTxCap states the ceiling in the driver library's own terms,
// where zero means "transmit disabled" and a named sentinel carries a
// ceiling of exactly 0 dBm. Keeping the translation here is what lets
// the configuration mean the plain thing.
func (s Settings) libraryTxCap() int8 {
	switch {
	case s.MaxTxPowerDBm == nil:
		return 0 // undeclared: the library refuses to transmit
	case *s.MaxTxPowerDBm == 0:
		return sx126x.MaxTxPowerZero
	default:
		return *s.MaxTxPowerDBm
	}
}

func (s Settings) envelope() radio.Envelope {
	var e radio.Envelope
	if s.MaxTxPowerDBm != nil {
		e.MaxTxPowerDBm, e.MaxTxPowerSet = *s.MaxTxPowerDBm, true
	}
	// The part's own range travels with the board's, so a power
	// resolved from the ceiling is judged before a frame discovers it.
	// An undeclared chip leaves the two equal, which reads as unknown.
	if chip, err := chipFrom(s.Chip); err == nil && chip != sx126x.ChipUnset {
		e.ChipMinDBm, e.ChipMaxDBm = chip.PowerRange()
	}
	if len(s.FrequencyRangeHz) == 2 {
		e.FreqRangeLowHz, e.FreqRangeHiHz = s.FrequencyRangeHz[0], s.FrequencyRangeHz[1]
	}
	return e
}

// The pure conversions: what a configured value means to the library.
// They live here rather than beside the hardware because the preflight
// runs them too — the same function, so what a dry run accepts is
// exactly what Configure and Open can carry out.

func tcxoFrom(s string) (sx126x.TCXOVoltage, error) {
	switch s {
	case "":
		return sx126x.TCXONone, nil
	case "1.6":
		return sx126x.TCXO1V6, nil
	case "1.8":
		return sx126x.TCXO1V8, nil
	case "3.3":
		return sx126x.TCXO3V3, nil
	}
	return sx126x.TCXONone, fmt.Errorf("unsupported tcxo voltage %q", s)
}

func chipFrom(s string) (sx126x.ChipVariant, error) {
	switch s {
	case "":
		return sx126x.ChipUnset, nil
	case chipSX1261:
		return sx126x.SX1261, nil
	case chipSX1262:
		return sx126x.SX1262, nil
	case chipSX1268:
		return sx126x.SX1268, nil
	}
	return sx126x.ChipUnset, fmt.Errorf("unknown chip %q", s)
}

func paramsFrom(w radio.Waveform) (lora.Params, error) {
	// The preamble is checked before the narrowing, not after: -1 cast
	// to uint16 is 65535, a length the library accepts and the chip
	// happily runs — a different network from the one the operator
	// wrote down, configured without complaint.
	if w.Preamble <= 0 || w.Preamble > 65535 {
		return lora.Params{}, fmt.Errorf(
			"preamble %d symbols out of range — the chip's field holds 1..65535", w.Preamble)
	}
	p := lora.Params{
		Frequency: w.FrequencyHz,
		Preamble:  uint16(w.Preamble),
		SyncWord:  w.SyncWord,
		CRC:       w.CRC,
	}
	switch w.SpreadingFactor {
	case 5, 6, 7, 8, 9, 10, 11, 12:
		p.SF = lora.SpreadingFactor(w.SpreadingFactor)
	default:
		return p, fmt.Errorf("spreading factor %d out of range", w.SpreadingFactor)
	}
	switch w.BandwidthHz {
	case 7810:
		p.BW = lora.BW7810
	case 15630:
		p.BW = lora.BW15630
	case 31250:
		p.BW = lora.BW31250
	case 62500:
		p.BW = lora.BW62500
	case 125000:
		p.BW = lora.BW125000
	case 250000:
		p.BW = lora.BW250000
	case 500000:
		p.BW = lora.BW500000
	default:
		return p, fmt.Errorf("unsupported bandwidth %d Hz", w.BandwidthHz)
	}
	switch w.CodingRate {
	case 5:
		p.CR = lora.CR5
	case 6:
		p.CR = lora.CR6
	case 7:
		p.CR = lora.CR7
	case 8:
		p.CR = lora.CR8
	default:
		return p, fmt.Errorf("coding rate 4/%d out of range", w.CodingRate)
	}
	return p, nil
}
