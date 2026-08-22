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
		"reset_pin":        16,
		"busy_pin":         24,
		"dio1_pin":         22,
		"enable_pins":      []int{12, 13},
		"dio2_rf_switch":   true,
		"tcxo":             "1.8",
		"dcdc":             true,
		"rx_boosted":       true,
		"chip":             "sx1262",
		"max_tx_power_dbm": int8(22),
		"frequency_range":  []uint32{850_000_000, 930_000_000},
	},
}

// Presets exposes the catalog to the registry.
func Presets() map[string]map[string]any { return presets }
