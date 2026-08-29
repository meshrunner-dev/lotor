package meshcore

// The engine's configuration vocabulary, declared beside the params
// struct it describes and pinned to it by test: an attribute that
// exists in one and not the other fails the build gate, which is what
// keeps the console's help honest.

import "meshrunner.dev/lotor/internal/schema"

// Schema describes every attribute a meshcore relay accepts.
func Schema() []schema.Attr {
	out := append(waveformSchema(), relayingSchema()...)
	return append(out, policySchema()...)
}

// waveformSchema is the air side: a mesh agreement, exact or the
// relay hears nothing. The band preset supplies these; overriding
// them is leaving the mesh.
func waveformSchema() []schema.Attr {
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

// relayingSchema is the relaying behaviour: how frames move through
// this node — pacing, memory, path width, orbit armour, distance.
func relayingSchema() []schema.Attr {
	return []schema.Attr{
		{Name: "tx_delay_factor", Type: schema.Float,
			Doc: "flood relay jitter as a factor of the frame's airtime (0..2; unset takes 0.5)"},
		{Name: "direct_tx_delay_factor", Type: schema.Float,
			Doc: "routed relay jitter, same shape (0..2; unset takes 0.3)"},
		{Name: "rx_delay_base", Type: schema.Float,
			Doc: "score-staggered hold before judging a flood, exponent base (0..20; 0 holds nothing)"},

		{Name: "dedup_ttl", Type: schema.Duration,
			Doc: "how long a seen packet stays remembered; 0 keeps the reference's count-only ring"},
		{Name: "dedup_entries", Type: schema.Int,
			Doc: "how many seen packets stay remembered (0 = the reference's 160)"},

		{Name: "path_hash_mode", Type: schema.Int,
			Doc: "hash width this node's own floods declare, mode+1 bytes (0..2; unset takes 1: two-byte hashes)"},
		{Name: "loop_detect", Type: schema.String,
			Enum: []string{loopOff, loopMinimal, loopModerate, loopStrict},
			Doc:  "flood orbit gate: apparent visits of our hash tolerated per width (unset takes minimal)"},

		{Name: "flood_max_hops", Type: schema.Int,
			Doc: "hop ceiling for any flood"},
		{Name: "flood_max_unscoped_hops", Type: schema.Int,
			Doc: "hop ceiling for plain (unscoped) floods"},
		{Name: "flood_max_advert_hops", Type: schema.Int,
			Doc: "hop ceiling for flooded adverts"},
	}
}

// policySchema is the node side: what this relay does with the mesh
// it hears.
func policySchema() []schema.Attr {
	return []schema.Attr{
		{Name: "tx_power_dbm", Type: schema.String,
			Doc: `"auto" (the board's cap) or a dBm figure the cap must allow`},
		{Name: "duty_cycle_pct", Type: schema.Float,
			Doc: "airtime budget per sliding hour, percent — the band's regulatory ceiling"},

		{Name: "advert_flood_interval", Type: schema.Duration,
			Doc: "cadence of the flooded self-announcement (3h..168h; 0 takes 47h)"},
		{Name: "advert_local_interval", Type: schema.Duration,
			Doc: "cadence of the zero-hop self-announcement (1h..4h; 0 takes 2h)"},

		{Name: "node_lat", Type: schema.Float,
			Doc: "advertised latitude, degrees"},
		{Name: "node_lon", Type: schema.Float,
			Doc: "advertised longitude, degrees"},
		{Name: "node_name", Type: schema.String,
			Doc: "what this node calls itself on the air"},
		{Name: "owner_info", Type: schema.String,
			Doc: "who answers for this node, served to owner questions"},
		{Name: "identity", Type: schema.String, Secret: true,
			Doc: `the node's private key, hex — or "new" to mint one; ` +
				"whoever reads it IS the node"},

		{Name: "guest_access", Type: schema.String,
			Enum: []string{guestBlocked, guestPassword, guestOpen},
			Doc:  "whether a stranger may open a read-only session over the air"},
		{Name: "admin_password", Type: schema.String, Secret: true,
			Doc: "grants the admin role over the air — empty keeps OTA administration off"},
		{Name: "guest_password", Type: schema.String, Secret: true,
			Doc: "the guest credential, with guest_access password"},
		{Name: "session_limit", Type: schema.Int,
			Doc: "answers one guest session may make this relay emit per minute (0 takes 6)"},
	}
}
