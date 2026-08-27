package meshcore

// presets are the band profiles this protocol ships with.
// The values are MeshCore network agreements: a relay must match its
// mesh exactly or hear nothing.
var presets = map[string]map[string]any{
	"eu-868-narrow": {
		"frequency_hz":     uint32(869_618_000),
		"spreading_factor": 8,
		"bandwidth_hz":     62_500,
		"coding_rate":      8,
		"preamble":         32,
		"sync_word":        0x12,
		"crc":              true,
		// The 869.4–869.65 MHz sub-band's regulatory ceiling: the duty
		// enforcement's budget, per sliding hour.
		"duty_cycle_pct": 10.0,
	},
}
