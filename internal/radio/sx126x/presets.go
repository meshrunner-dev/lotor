package sx126x

// presets are the known board profiles. Each carries the attachment
// and the board's envelope; a config file patches them through its
// override scopes without ever editing them.
var presets = map[string]map[string]any{
	// RAK6421 Pi HAT with the 868 MHz SX1262 module in slot 1:
	// chip-driven RF switch on DIO2, both front-end enables held high,
	// 1.8 V TCXO on DIO3.
	"rak6421-13300x-slot1": {
		"spi":              "/dev/spidev0.0",
		"gpiochip":         "gpiochip0",
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
