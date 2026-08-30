// Package meshcorecfg owns configuration shared by the MeshCore roles hosted
// by Lotor. A station and a relay meeting on one radio must resolve the exact
// same waveform, so its vocabulary and presets have one source.
package meshcorecfg

import "meshrunner.dev/lotor/internal/schema"

// WaveformSchema describes the MeshCore air agreement.
func WaveformSchema() []schema.Attr {
	return []schema.Attr{
		{Name: "frequency_hz", Type: schema.Int,
			Doc: "carrier frequency in hertz — a mesh agreement, exact"},
		{Name: "spreading_factor", Type: schema.Int,
			Doc: "LoRa spreading factor (7..12)"},
		{Name: "bandwidth_hz", Type: schema.Int,
			Doc: "LoRa bandwidth in hertz"},
		{Name: "coding_rate", Type: schema.Int,
			Doc: "LoRa coding rate denominator (5..8)"},
		{Name: "preamble", Type: schema.Int,
			Doc: "preamble length in symbols"},
		{Name: "sync_word", Type: schema.Int,
			Doc: "LoRa sync word"},
		{Name: "crc", Type: schema.Bool,
			Doc: "whether frames carry a CRC"},
	}
}

// Presets returns a fresh MeshCore waveform catalog. Callers may hand it to
// layering code without sharing mutable maps across registries.
func Presets() map[string]map[string]any {
	return map[string]map[string]any{
		"eu-868-narrow": {
			"frequency_hz":     uint32(869_618_000),
			"spreading_factor": 8,
			"bandwidth_hz":     62_500,
			"coding_rate":      8,
			"preamble":         32,
			"sync_word":        0x12,
			"crc":              true,
			"duty_cycle_pct":   10.0,
		},
	}
}
