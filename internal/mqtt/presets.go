package mqtt

// The community brokers, as profiles: one name settles the url, the
// authentication style, the keepalive a load balancer tolerates, and
// whether the broker likes its status retained. The values are the
// observer ecosystem's public knowledge — the same table every
// observer node ships — and the layering machinery treats them
// exactly like a radio's band presets: a preset is a base, overrides
// patch it, provenance says which said what.
//
// JWT brokers authenticate the device by its own signature; the ones
// with embedded credentials publish them as community write access.
// Every TLS endpoint here chains to a public root the system store
// carries.

// balancerKeepalive is what the community brokers behind load
// balancers ask for: under the balancers' 60-second idle cut.
const balancerKeepalive = "55s"

// jwtPreset is the common device-auth shape most community brokers
// share: TLS websocket, token audience, balancer keepalive, retained
// snapshots.
func jwtPreset(host, path string) map[string]any {
	return map[string]any{
		keyURL:       "wss://" + host + ":443" + path,
		keyAudience:  host,
		keyKeepalive: balancerKeepalive,
		keyRetain:    true,
	}
}

// wsPath is where most community brokers mount their websocket.
const wsPath = "/mqtt"

// jwtHosts are the brokers that fit jwtPreset whole: host and
// websocket path, nothing else to say.
func jwtHosts() map[string][2]string {
	return map[string][2]string{
		"analyzer-us":     {"mqtt-us-v1.letsmesh.net", wsPath},
		"analyzer-eu":     {"mqtt-eu-v1.letsmesh.net", wsPath},
		"nz-analyzer":     {"meshcore-mqtt-1.baird.io", ""},
		"meshmapper":      {"mqtt.meshmapper.net", wsPath},
		"meshomatic":      {"us-east.meshomatic.net", wsPath},
		"cascadiamesh":    {"mqtt-v1.cascadiamesh.org", wsPath},
		"chimesh":         {"mqtt.chimesh.org", ""},
		"meshat-se":       {"meshcore-mqtt.meshat.se", ""},
		"coloradomesh":    {"mqtt.meshcore.coloradomesh.org", ""},
		"dutchmeshcore-1": {"collector1.dutchmeshcore.nl", wsPath},
		"dutchmeshcore-2": {"collector2.dutchmeshcore.nl", wsPath},
		"meshcore-ca-1":   {"mqtt1.meshcore.ca", wsPath},
		"meshcore-ca-2":   {"mqtt2.meshcore.ca", wsPath},
		"meshcore-fi":     {"mc-mqtt.meshcore.fi", "/"},
		"bostonmesh":      {"mqttmc01.bostonme.sh", wsPath},
		"rflab":           {"mqtt.rflab.io", ""},
		"ipnt-uk":         {"mqtt.ipnt.uk", ""},
		"flmesh":          {"mcmqtt.jntconnections.com", ""},
		"corecomms":       {"mqtt.corecomms.net", wsPath},
		"meshtexas":       {"mqtt.meshtexas.org", wsPath},
		"wcmesh":          {"mqtt.wcmesh.com", ""},
		"atvirastinklas":  {"mqtt-mc.atvirastinklas.lt", ""},
		"gomesh":          {"mqtt.gomesh.dev", ""},
		"idahomesh":       {"mqtt.idahomesh.org", wsPath},
	}
}

// specialPresets are the brokers that depart from the common shape:
// odd ports, token topics, embedded community credentials.
func specialPresets() map[string]map[string]any {
	return map[string]map[string]any{
		"ntxmesh": {
			keyURL: "wss://ntxmesh.dhovin.me:8883", keyAudience: "ntxmesh.dhovin.me",
			keyKeepalive: balancerKeepalive, keyRetain: true,
		},
		"okimesh-1": {
			keyURL: "wss://mqtt1.okimesh.org:9002" + wsPath, keyAudience: "mqtt1.okimesh.org",
			keyKeepalive: balancerKeepalive, keyRetain: true,
		},
		"okimesh-2": {
			keyURL: "wss://mqtt2.okimesh.org:9002" + wsPath, keyAudience: "mqtt2.okimesh.org",
			keyKeepalive: balancerKeepalive, keyRetain: true,
		},
		// The waev broker's real token TTL is an hour; claiming less
		// keeps fresh tokens accepted through device clock skew.
		"waev": {
			keyURL: "wss://mqtt.waev.app:443" + wsPath, keyAudience: "mqtt.waev.app",
			"token_lifetime": "55m", keyKeepalive: balancerKeepalive,
		},
		"meshrank": {
			keyURL:  "ssl://meshrank.net:8883",
			"topic": "meshrank/uplink/{token}/{device}/{type}",
		},
		"tennmesh": {
			keyURL: "tcp://mqtt.tennmesh.com:1883", keyKeepalive: balancerKeepalive, keyRetain: true,
			keyUsername: "mqttfeed", keyPassword: "tc2live",
		},
		"nashmesh": {
			keyURL: "tcp://mqtt.nashme.sh:1883", keyKeepalive: balancerKeepalive, keyRetain: true,
			keyUsername: "meshdev", keyPassword: "large4cats",
		},
		"ctmesh": {
			keyURL: "tcp://mqtt.ctmesh.org:1883", keyKeepalive: "60s", keyRetain: true,
			keyUsername: "meshdev", keyPassword: "large4cats",
		},
		"eastidahomesh": {
			keyURL: "tcp://live.eastidahomesh.com:1883", keyKeepalive: balancerKeepalive, keyRetain: true,
		},
		"inwmesh": {
			keyURL: "ssl://scope.inwmesh.org:8883", keyKeepalive: balancerKeepalive, keyRetain: true,
		},
		// The broker matches the username against the device key.
		"mesh-chaun14": {
			keyURL: "tcp://mqtt.mesh.chaun14.fr:1884", keyKeepalive: "60s", keyRetain: true,
			keyUsername: "{pubkey}",
		},
	}
}

// Presets is the catalog, keyed by profile name.
func Presets() map[string]map[string]any {
	catalog := specialPresets()
	for name, hp := range jwtHosts() {
		catalog[name] = jwtPreset(hp[0], hp[1])
	}
	return catalog
}
