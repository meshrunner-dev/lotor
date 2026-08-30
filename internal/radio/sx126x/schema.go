package sx126x

// The driver's configuration vocabulary, declared beside the Settings
// struct it describes and pinned to it by test.

import "meshrunner.dev/lotor/internal/schema"

// The attribute names, shared by the schema and the preset catalog:
// one spelling, wherever a board is described.
const (
	attrSPI        = "spi"
	attrSPIHz      = "spi_hz"
	attrGPIOChip   = "gpiochip"
	attrResetPin   = "reset_pin"
	attrBusyPin    = "busy_pin"
	attrDIO1Pin    = "dio1_pin"
	attrEnablePins = "enable_pins"
	attrDIO2RF     = "dio2_rf_switch"
	attrTCXO       = "tcxo"
	attrDCDC       = "dcdc"
	attrRXBoosted  = "rx_boosted"
	attrChip       = "chip"
	attrDIO1WD     = "dio1_watchdog"
	attrMaxTxPower = "max_tx_power_dbm"
	attrFreqRange  = "frequency_range"
)

// Schema describes every attribute this driver accepts. Waveform
// choices are absent on purpose: they belong to the relay, and arrive
// through Configure.
func Schema() []schema.Attr {
	return []schema.Attr{
		{Name: attrSPI, Type: schema.String,
			Doc: "the SPI device node, e.g. /dev/spidev0.0"},
		{Name: attrSPIHz, Type: schema.Int,
			Doc: "SPI clock in hertz (0 takes the driver default)"},
		{Name: attrGPIOChip, Type: schema.String,
			Doc: "the chip a pin naming none sits on (empty takes gpiochip0)"},
		{Name: attrResetPin, Type: schema.String,
			Doc: "NRESET line: offset, or chip:offset to name another chip"},
		{Name: attrBusyPin, Type: schema.String,
			Doc: "BUSY line: offset, or chip:offset to name another chip"},
		{Name: attrDIO1Pin, Type: schema.String,
			Doc: "DIO1 interrupt line: offset, or chip:offset to name another chip"},
		{Name: attrEnablePins, Type: schema.Words,
			Doc: "lines held high for the whole session (front-end enables)"},
		{Name: attrDIO2RF, Type: schema.Bool,
			Doc: "let the chip steer the RF switch through DIO2"},
		{Name: attrTCXO, Type: schema.String,
			Doc: `TCXO supply on DIO3, volts as text ("1.8"); empty for a crystal`},
		{Name: attrDCDC, Type: schema.Bool,
			Doc: "use the DC-DC regulator instead of the LDO"},
		{Name: attrRXBoosted, Type: schema.Bool,
			Doc: "boosted-sensitivity receive, at some current cost"},
		{Name: attrChip, Type: schema.String,
			Enum: []string{chipSX1261, chipSX1262, chipSX1268},
			Doc:  "the exact part — the version register cannot tell them apart, the integrator must"},
		{Name: attrDIO1WD, Type: schema.Duration,
			Doc: "re-poll cadence insuring a degraded DIO1 line (0 = off)"},
		{Name: attrMaxTxPower, Type: schema.Int,
			Doc: "the board's transmit ceiling, chip-side dBm — an envelope, not a choice"},
		{Name: attrFreqRange, Type: schema.Ints,
			Doc: "the frequencies the board serves, hertz [low, high]"},
	}
}
