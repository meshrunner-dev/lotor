package meshcore

import (
	"meshrunner.dev/lotor/internal/meshcorecfg"
	"meshrunner.dev/lotor/internal/schema"
)

func stationSchema() []schema.Attr {
	out := meshcorecfg.WaveformSchema()
	return append(out,
		schema.Attr{Name: "tx_power_dbm", Type: schema.Int,
			Doc: "station transmit power in dBm; the radio envelope must allow it"},
		schema.Attr{Name: "duty_cycle_pct", Type: schema.Float,
			Doc: "accounted airtime budget per sliding hour, percent"},
		schema.Attr{Name: "identity", Type: schema.String, Secret: true,
			Doc: `the station private key, hex — or "new" to mint one`},
		schema.Attr{Name: "node_name", Type: schema.String,
			Doc: "what this station calls itself on the mesh"},
		schema.Attr{Name: "pin", Type: schema.Int,
			Doc: "companion protocol PIN reported to applications"},
		schema.Attr{Name: "node_lat", Type: schema.Float,
			Doc: "advertised latitude, degrees"},
		schema.Attr{Name: "node_lon", Type: schema.Float,
			Doc: "advertised longitude, degrees"},
		schema.Attr{Name: "multi_acks", Type: schema.Int,
			Doc: "number of acknowledgement copies requested, 0..3"},
		schema.Attr{Name: "advert_loc_policy", Type: schema.Int,
			Doc: "MeshCore advert location policy byte"},
		schema.Attr{Name: "telemetry_mode", Type: schema.Int,
			Doc: "MeshCore companion telemetry permission bitfield"},
		schema.Attr{Name: "manual_add_contacts", Type: schema.Bool,
			Doc: "require the companion application to add contacts explicitly"},
		schema.Attr{Name: "path_hash_mode", Type: schema.Int,
			Doc: "path hash width mode reported to applications, 0..2"},
		schema.Attr{Name: "max_contacts", Type: schema.Int,
			Doc: "persistent contact capacity; 0 takes 100"},
		schema.Attr{Name: "max_channels", Type: schema.Int,
			Doc: "persistent channel-slot capacity; 0 takes 8"},
		schema.Attr{Name: "mailbox_capacity", Type: schema.Int,
			Doc: "offline message capacity; 0 takes the reference's 16"},
	)
}
