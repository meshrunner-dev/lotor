package mqtt

// The observer's resolved shape: what one connection is, after the
// preset and the overrides have said their piece. The manager
// resolves the layers and decodes the map into this; the attributes
// the console offers are declared beside it so the two cannot drift.

import (
	"fmt"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"meshrunner.dev/lotor/internal/schema"
)

// Duration reads a scalar whatever its spelling: a preset writes
// "55s", the store re-encodes nanoseconds — both are the same figure,
// and refusing one of them was this week's tx_power_dbm lesson.
type Duration time.Duration

// UnmarshalYAML reads the scalar's text.
func (d *Duration) UnmarshalYAML(node *yaml.Node) error {
	if node.Value == "" {
		*d = 0
		return nil
	}
	if parsed, err := time.ParseDuration(node.Value); err == nil {
		*d = Duration(parsed)
		return nil
	}
	var ns int64
	if err := node.Decode(&ns); err != nil {
		return fmt.Errorf("%q is not a duration", node.Value)
	}
	*d = Duration(ns)
	return nil
}

// Std is the standard-library value.
func (d Duration) Std() time.Duration { return time.Duration(d) }

// NormalizeIATA holds the region code to the ecosystem's rule:
// exactly three letters or digits, spoken uppercase — it lands
// directly in topic paths, where anything else is a hole or a
// separator. Empty passes through: whether a topic needs one is the
// template's question, not this one's.
func NormalizeIATA(s string) (string, error) {
	if s == "" {
		return "", nil
	}
	if len(s) != 3 {
		return "", fmt.Errorf("iata %q — exactly three letters or digits (e.g. DEN)", s)
	}
	for _, c := range s {
		switch {
		case c >= 'A' && c <= 'Z', c >= 'a' && c <= 'z', c >= '0' && c <= '9':
		default:
			return "", fmt.Errorf("iata %q — exactly three letters or digits (e.g. DEN)", s)
		}
	}
	return strings.ToUpper(s), nil
}

// ValidOwner holds the owner key to the ecosystem's rule: exactly 64
// hex characters — a 32-byte public key — any case, kept as written.
// Empty passes: the claim is optional.
func ValidOwner(s string) error {
	if s == "" {
		return nil
	}
	if len(s) != 64 {
		return fmt.Errorf("owner %q — a public key is 64 hex characters", s)
	}
	for _, c := range s {
		switch {
		case c >= '0' && c <= '9', c >= 'a' && c <= 'f', c >= 'A' && c <= 'F':
		default:
			return fmt.Errorf("owner %q — a public key is 64 hex characters", s)
		}
	}
	return nil
}

// The parameter names the schema, the presets and the code share.
const (
	keyURL       = "url"
	keyUsername  = "username"
	keyPassword  = "password"
	keyAudience  = "audience"
	keyKeepalive = "keepalive"
	keyRetain    = "retain"
)

// Params is one observer connection, resolved.
type Params struct {
	URL      string `yaml:"url"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
	// Audience switches the connection to device authentication: a
	// token signed by the node identity, minted fresh at each dial.
	Audience      string   `yaml:"audience"`
	TokenLifetime Duration `yaml:"token_lifetime"`
	Keepalive     Duration `yaml:"keepalive"`
	// CA is a PEM file pinning the broker's chain; empty trusts the
	// system roots.
	CA     string `yaml:"ca"`
	Retain bool   `yaml:"retain"`

	IATA  string `yaml:"iata"`
	Token string `yaml:"token"`
	Topic string `yaml:"topic"`
	Relay string `yaml:"relay"`

	Packets *bool    `yaml:"packets"`
	Raw     bool     `yaml:"raw"`
	RX      *bool    `yaml:"rx"`
	TX      string   `yaml:"tx"`
	Types   []string `yaml:"types"`

	// StatusInterval is the heartbeat's whole switch: absent takes
	// the default cadence, an explicit zero silences it.
	StatusInterval *Duration `yaml:"status_interval"`
	// NeighboursInterval enables the neighbourhood round by giving it
	// a cadence — setting it IS the consent to the emissions each
	// round makes. Absent means off.
	NeighboursInterval Duration `yaml:"neighbours_interval"`

	// Origin overrides the name published; empty speaks the relay's
	// own. Owner is the operator's public key, an optional claim in
	// the device token — how a feed is claimed on the platforms.
	Origin string `yaml:"origin"`
	Owner  string `yaml:"owner"`
}

// Schema declares the console's view of Params, one attribute per
// field; a reflection test holds the two together.
func Schema() []schema.Attr {
	return []schema.Attr{
		{Name: keyURL, Type: schema.String,
			Doc: "the broker — tcp://host:1883, ssl://, ws:// or wss://"},
		{Name: keyUsername, Type: schema.String,
			Doc: "broker credential; {pubkey} sends the node key, empty connects anonymously"},
		{Name: keyPassword, Type: schema.String, Secret: true,
			Doc: "broker credential"},
		{Name: keyAudience, Type: schema.String,
			Doc: "set by device-auth brokers: the token's audience; empty stays password auth"},
		{Name: "token_lifetime", Type: schema.Duration, Apply: schema.Hot,
			Doc: "how long a minted device token claims to live (default 24h)"},
		{Name: keyKeepalive, Type: schema.Duration, Apply: schema.Hot,
			Doc: "MQTT keepalive; brokers behind balancers often want under 60s (default 2m)"},
		{Name: "ca", Type: schema.String,
			Doc: "PEM file pinning the broker's chain; empty trusts the system roots"},
		{Name: keyRetain, Type: schema.Bool, Apply: schema.Hot,
			Doc: "retain the heartbeat and neighbour snapshots where the broker allows"},
		{Name: "iata", Type: schema.String,
			Doc: "the site's region code, exactly three letters or digits (e.g. DEN)"},
		{Name: "token", Type: schema.String,
			Doc: "per-connection token some topic layouts carry"},
		{Name: "topic", Type: schema.String,
			Doc: "topic template; empty takes meshcore/{iata}/{device}/{type}"},
		{Name: "relay", Type: schema.String,
			Doc: "whose frames to watch; empty takes the only relay"},
		{Name: "packets", Type: schema.Bool, Apply: schema.Hot,
			Doc: "publish each frame, analysed (default true)"},
		{Name: "raw", Type: schema.Bool, Apply: schema.Hot,
			Doc: "publish each frame as plain hex too (default false)"},
		{Name: "rx", Type: schema.Bool, Apply: schema.Hot,
			Doc: "share received frames (default true)"},
		{Name: "tx", Type: schema.String, Enum: []string{"off", "self-adverts", "all"},
			Apply: schema.Hot,
			Doc:   "share sent frames: nothing, our own adverts, everything (default self-adverts)"},
		{Name: "types", Type: schema.Words, Apply: schema.Hot,
			Doc: "payload types to share, by name; empty shares all"},
		{Name: "status_interval", Type: schema.Duration, Apply: schema.Hot,
			Doc: "how often the heartbeat goes out (default 5m; 0 silences it)"},
		{Name: "neighbours_interval", Type: schema.Duration, Apply: schema.Hot,
			Doc: "publish the neighbourhood this often, asking each neighbour its scopes " +
				"over the air — setting it is consent to those emissions (floor 30m)"},
		{Name: "origin", Type: schema.String,
			Doc: "the name published; empty speaks the relay's own"},
		{Name: "owner", Type: schema.String,
			Doc: "the operator's public key (64 hex), claimed in the device token"},
	}
}
