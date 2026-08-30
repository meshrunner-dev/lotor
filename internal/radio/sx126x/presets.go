package sx126x

// presets are the known board profiles. A preset states what the PCB
// fixes — pins, enables, oscillator, RF switch wiring, the envelope —
// and nothing the host kernel names: bus paths and gpiochip labels
// vary per machine and belong in the config's override scopes.
var presets = map[string]map[string]any{
	// RAK6421 Pi HAT with the 868 MHz SX1262 module in slot 1:
	// chip-driven RF switch on DIO2, both front-end enables held high,
	// 1.8 V TCXO on DIO3. Pin numbers are BCM lines, fixed by the HAT
	// connector.
	"rak6421-13300x-slot1": {
		attrResetPin:   16,
		attrBusyPin:    24,
		attrDIO1Pin:    22,
		attrEnablePins: []int{12, 13},
		attrDIO2RF:     true,
		attrTCXO:       tcxo1V8,
		attrDCDC:       true,
		attrRXBoosted:  true,
		attrChip:       chipSX1262,
		attrMaxTxPower: int8(22),
		attrFreqRange:  []uint32{850_000_000, 930_000_000},
	},
	// Station G3 station kit: the BQ35LORA900V1M 900 MHz daughterboard
	// — an SX1262 behind a power amplifier and an LNA — on the G3
	// carrier, driven over the Luckfox Lyra Zero W header. The PA
	// takes the chip's own drive up to its full 22 dBm; DIO2 steers
	// the RF switch, DIO3 feeds the 1.8 V TCXO. Reset is wired to the
	// SoC's SECOND GPIO bank — a device-tree fact of this SBC, which
	// is why that one pin names its chip — while busy and DIO1 ride
	// the default bank. The SPI node stays with the operator's
	// override scope, as always.
	"lyra-zerow-station-g3": {
		attrResetPin:   "gpiochip1:25",
		attrBusyPin:    12,
		attrDIO1Pin:    5,
		attrDIO2RF:     true,
		attrTCXO:       tcxo1V8,
		attrDCDC:       true,
		attrRXBoosted:  true,
		attrChip:       chipSX1262,
		attrMaxTxPower: int8(22),
		attrFreqRange:  []uint32{850_000_000, 930_000_000},
	},
}

// Presets exposes the catalog to the registry.
func Presets() map[string]map[string]any { return presets }
