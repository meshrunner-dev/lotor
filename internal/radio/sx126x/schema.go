package sx126x

// The driver's configuration vocabulary, declared beside the Settings
// struct it describes and pinned to it by test.

import "meshrunner.dev/lotor/internal/schema"

// Schema describes every attribute this driver accepts. Waveform
// choices are absent on purpose: they belong to the relay, and arrive
// through Configure.
func Schema() []schema.Attr {
	return []schema.Attr{
		{Name: "spi", Type: schema.String,
			Doc: "the SPI device node, e.g. /dev/spidev0.0"},
		{Name: "spi_hz", Type: schema.Int,
			Doc: "SPI clock in hertz (0 takes the driver default)"},
		{Name: "gpiochip", Type: schema.String,
			Doc: "the chip a pin naming none sits on (empty takes gpiochip0)"},
		{Name: "reset_pin", Type: schema.String,
			Doc: "NRESET line: offset, or chip:offset to name another chip"},
		{Name: "busy_pin", Type: schema.String,
			Doc: "BUSY line: offset, or chip:offset to name another chip"},
		{Name: "dio1_pin", Type: schema.String,
			Doc: "DIO1 interrupt line: offset, or chip:offset to name another chip"},
		{Name: "enable_pins", Type: schema.Words,
			Doc: "lines held high for the whole session (front-end enables)"},
		{Name: "dio2_rf_switch", Type: schema.Bool,
			Doc: "let the chip steer the RF switch through DIO2"},
		{Name: "tcxo", Type: schema.String,
			Doc: `TCXO supply on DIO3, volts as text ("1.8"); empty for a crystal`},
		{Name: "dcdc", Type: schema.Bool,
			Doc: "use the DC-DC regulator instead of the LDO"},
		{Name: "rx_boosted", Type: schema.Bool,
			Doc: "boosted-sensitivity receive, at some current cost"},
		{Name: "chip", Type: schema.String,
			Enum: []string{chipSX1261, chipSX1262, chipSX1268},
			Doc:  "the exact part — the version register cannot tell them apart, the integrator must"},
		{Name: "dio1_watchdog", Type: schema.Duration,
			Doc: "re-poll cadence insuring a degraded DIO1 line (0 = off)"},
		{Name: "max_tx_power_dbm", Type: schema.Int,
			Doc: "the board's transmit ceiling, chip-side dBm — an envelope, not a choice"},
		{Name: "frequency_range", Type: schema.Ints,
			Doc: "the frequencies the board serves, hertz [low, high]"},
	}
}
