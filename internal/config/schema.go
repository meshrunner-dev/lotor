package config

// The structural attributes: what an object IS — which protocol, on
// which radio, behind which gate — as opposed to what its choice then
// contributes. Declared beside the structs they describe and pinned
// to them by test. The nested tx block flattens to dotted names, the
// shape a one-line console command can reach.

import (
	"meshrunner.dev/lotor/internal/product"
	"meshrunner.dev/lotor/internal/schema"
)

// RelayAttrs describes a relay's own structure.
func RelayAttrs() []schema.Attr {
	return []schema.Attr{
		{Name: "protocol", Type: schema.String,
			Doc: "the protocol this relay speaks (chooses the rest of its attributes)"},
		{Name: "radio", Type: schema.String,
			Doc: "the radio this relay owns — one owner per radio"},
		{Name: attrProfile, Type: schema.String,
			Doc: `the band preset; "custom" starts from nothing`},
		{Name: "noise_history", Type: schema.Bool,
			Doc: "archive this relay's noise floor (measurement always runs; this is the disk)"},
		{Name: "tx.mode", Type: schema.String,
			Enum: []string{TXDry, TXShadow, TXOnAirZeroHop, TXOnAir},
			Doc:  "the transmit gate; absent block means dry, the receive-only posture"},
		{Name: "tx.lbt_threshold_db", Type: schema.Float,
			Doc: "margin above the noise floor that marks the channel busy (0 disables the RSSI stage)"},
		{Name: "tx.lbt_exhausted", Type: schema.String,
			Enum: []string{LBTTransmit, LBTDrop},
			Doc:  "what a channel busy past the bounded wait earns"},
		{Name: "tx.queue_depth", Type: schema.Int,
			Doc: "outbound queue bound, 1..63 (0 takes 32)"},
		{Name: "tx.cad", Type: schema.Bool,
			Doc: "listen with the radio's own activity detection before keying " +
				"(unset leaves it on — the reference ships it off)"},
	}
}

// attrProfile is the layering knob every layered kind carries.
const attrProfile = "profile"

// RadioAttrs describes a radio's own structure.
func RadioAttrs() []schema.Attr {
	return []schema.Attr{
		{Name: "driver", Type: schema.String,
			Doc: "the driver that speaks to this transceiver (chooses the rest of its attributes)"},
		{Name: attrProfile, Type: schema.String,
			Doc: `the board preset; "custom" starts from nothing`},
	}
}

// SensorAttrs describes what every sensor declares, whatever it is.
// The driver contributes the rest — a bus, an address, whatever the
// part needs to be found.
func SensorAttrs() []schema.Attr {
	return []schema.Attr{
		{Name: "driver", Type: schema.String,
			Doc: "the driver that speaks to this part (chooses the rest of its attributes)"},
		{Name: "sample_interval", Type: schema.Duration,
			Doc: "how often the part is read, on its own goroutine (1s..1h; 0 takes 30s)"},
	}
}

// SentinelAttrs describes the observation instantiation.
func SentinelAttrs() []schema.Attr {
	return []schema.Attr{
		{Name: "journal", Type: schema.String,
			Doc: `the journal's SQLite path, or ":memory:" for RAM-only observation`},
		{Name: "retention", Type: schema.Duration,
			Doc: "how far back the journal reaches (0 takes 720h)"},
		{Name: "max_frames", Type: schema.Int,
			Doc: "row bound on top of retention, frames table alone"},
		{Name: "metrics_retention", Type: schema.Duration,
			Doc: "how long the consolidated hourly/daily metric tiers reach (0 takes two years)"},
	}
}

// SystemAttrs describes what this installation calls itself.
func SystemAttrs() []schema.Attr {
	return []schema.Attr{
		{Name: "name", Type: schema.String, Apply: schema.Hot,
			Doc: "what this installation calls itself; empty takes the machine's hostname"},
		{Name: "log_level", Type: schema.String, Apply: schema.Hot,
			Enum: []string{"trace", "debug", "info", "warn", "error"},
			Doc:  "how deep the journal speaks, applied live; empty takes the boot flag"},
	}
}

// UpdateAttrs describes where the relay looks for newer versions of
// itself.
func UpdateAttrs() []schema.Attr {
	return []schema.Attr{
		{Name: "channel", Type: schema.String,
			Doc: "what to follow: release, rc, beta, dev, or a try-<slug>"},
		{Name: "url", Type: schema.String,
			Doc: "the manifest tree; empty takes " + DefaultUpdateURL},
		{Name: "token", Type: schema.String, Secret: true,
			Doc: "bearer for artifact downloads — a private fork's assets"},
	}
}

// DefaultUpdateURL is the project's own manifest tree.
const DefaultUpdateURL = product.UpdateBase

// CLIAttrs describes the operator listener.
func CLIAttrs() []schema.Attr {
	return []schema.Attr{
		{Name: "listen", Type: schema.String,
			Doc: "the telnet address — plaintext and read-only, loopback by default"},
		{Name: "socket", Type: schema.String,
			Doc: "the local admin console socket path; empty disables it"},
	}
}
