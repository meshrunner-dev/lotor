// Package config loads the daemon's configuration file and implements
// its layering paradigm: named profiles with per-profile override
// scopes, instantiated for radio hardware and for relay bands alike.
// Validation is strict everywhere — unknown keys, unknown profiles and
// dangling references fail the load with a message, because a config
// that half-applies is worse than one that refuses.
package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Radio declares a physical transceiver attachment: which driver
// speaks to it and the layered hardware configuration (bus, pins, and
// the board's envelope — its physical limits).
type Radio struct {
	Driver  string  `yaml:"driver"`
	Layered Layered `yaml:",inline"`
}

// Sensor declares one peripheral attached to this machine:
// which driver speaks to it, how often it is sampled, and the layered
// hardware configuration. Unlike a radio it is not owned — the bus
// belongs to the machine, so every relay on it reports the same
// reading, and nothing claims it exclusively.
type Sensor struct {
	Driver string `yaml:"driver"`
	// SampleInterval is the cadence of its own goroutine. Zero takes
	// the daemon's default; the value belongs to the sensor because
	// the cost of a sample is the bus's, not any one relay's.
	SampleInterval time.Duration `yaml:"sample_interval"`
	Layered        Layered       `yaml:",inline"`
}

// Relay declares one protocol instance: which protocol it speaks,
// which radio it owns, and the layered waveform configuration
// (band presets and their overrides).
type Relay struct {
	Protocol string  `yaml:"protocol"`
	Radio    string  `yaml:"radio"`
	Layered  Layered `yaml:",inline"`
	// NoiseHistory gates archiving this relay's noise floor: it is a
	// disk-write subject, so it is opt-out-able like the sentinel is.
	// Unset takes the build's default (on normally, off in lean); the
	// measurement itself always runs — off just keeps the latest value
	// in RAM only.
	NoiseHistory *bool `yaml:"noise_history"`
	// TX gates and tunes the transmit path; absent means dry — the
	// receive-only posture.
	TX *TX `yaml:"tx"`
}

// The transmit gate ladder: dry runs the judgement alone, shadow runs
// the whole pipeline and journals the emissions it would have made,
// on-air-zero-hop keys only what stays with the direct neighbourhood
// (local adverts, discovery answers) while everything routable is
// still journalled on paper, and on-air keys the radio for all of it.
const (
	TXDry          = "dry"
	TXShadow       = "shadow"
	TXOnAirZeroHop = "on-air-zero-hop"
	TXOnAir        = "on-air"
)

// What a channel busy past the bounded wait earns.
const (
	LBTTransmit = "transmit" // key anyway — the mesh's convention
	LBTDrop     = "drop"     // refuse, counted and visible
)

// TX configures a relay's transmit gate and channel politeness.
type TX struct {
	// Mode is the gate: dry (default), shadow, or on-air.
	Mode string `yaml:"mode"`
	// LBTThresholdDB, above the measured noise floor, marks the
	// channel busy before keying. Zero — the default — disables the
	// RSSI check; field experience rates it unreliable.
	LBTThresholdDB float64 `yaml:"lbt_threshold_db"`
	// LBTExhausted decides what happens when the channel stays busy
	// past the bounded wait: "transmit" (default — the mesh's
	// convention) or "drop", counted and visible.
	LBTExhausted string `yaml:"lbt_exhausted"`
	// CAD gates the hardware's own channel activity detection before
	// keying. Unset leaves it on, which is a deliberate step away
	// from the reference: its firmware ships the scan disabled, while
	// a Linux host with a healthy SPI bus can afford to look before
	// it speaks, and a repeater that talks over its neighbours costs
	// the mesh more than it costs itself. A site measuring the
	// difference — latency, false busy, preambles missed — turns it
	// off here.
	CAD *bool `yaml:"cad"`
	// QueueDepth bounds the outbound queue. The default holds about
	// ten seconds of backlog at the narrow waveforms this daemon
	// ships for; a field knob because sites will want to experiment.
	QueueDepth int `yaml:"queue_depth"`
}

// Normalize fills the TX defaults and rejects unknown enum values.
func (t *TX) Normalize() error {
	if t.Mode == "" {
		t.Mode = TXDry
	}
	if t.Mode != TXDry && t.Mode != TXShadow && t.Mode != TXOnAirZeroHop && t.Mode != TXOnAir {
		return fmt.Errorf("tx: mode %q — want dry, shadow, on-air-zero-hop or on-air", t.Mode)
	}
	if t.LBTExhausted == "" {
		t.LBTExhausted = LBTTransmit
	}
	if t.LBTExhausted != LBTTransmit && t.LBTExhausted != LBTDrop {
		return fmt.Errorf("tx: lbt_exhausted %q — want transmit or drop", t.LBTExhausted)
	}
	if t.LBTThresholdDB < 0 {
		return fmt.Errorf(
			"tx: lbt_threshold_db %g — want 0 to disable the RSSI stage, or a positive margin above the noise floor",
			t.LBTThresholdDB)
	}
	if t.QueueDepth == 0 {
		// The reference's pool size. Smaller looks tempting on a narrow
		// waveform, but the anonymous answers a stranger can demand
		// without any credential would then fill the whole queue and
		// crowd out the relaying this node exists to do.
		t.QueueDepth = 32
	}
	if t.QueueDepth < 1 || t.QueueDepth > 63 {
		return fmt.Errorf("tx: queue_depth %d — want 1..63", t.QueueDepth)
	}
	return nil
}

// TXMode resolves the relay's gate, absent block included.
func (r *Relay) TXMode() string {
	if r.TX == nil {
		return TXDry
	}
	return r.TX.Mode
}

// Sentinel configures the observation and archival instantiation.
// Its absence is meaningful: no sentinel block, no journal, no
// storage — the mode for hosts with tight RAM and CPU.
type Sentinel struct {
	// Journal is the SQLite path, or ":memory:" to keep the archive
	// in RAM on storage that dislikes continuous writes.
	Journal string `yaml:"journal"`
	// Retention bounds how far back the journal reaches.
	Retention time.Duration `yaml:"retention"`
	// MaxFrames, when set, also bounds the journal in rows — for
	// hosts where time alone would let a busy mesh outgrow the disk.
	// It bounds the frames table alone; states, emissions and drops
	// follow retention.
	MaxFrames int `yaml:"max_frames"`
	// MetricsRetention bounds the CONSOLIDATED metric history — the
	// hourly and daily tiers that outlive retention by design, so a
	// year of noise-floor trend survives a month of frame detail.
	// Zero takes two years; retention itself is only the raw→hourly
	// frontier for metrics.
	MetricsRetention time.Duration `yaml:"metrics_retention"`
}

// DefaultRetention keeps the journal for a long default, as an
// observation archive should.
const DefaultRetention = 30 * 24 * time.Hour

// DefaultMetricsRetention keeps the consolidated metric tiers for two
// years — trends are the archive's long game.
const DefaultMetricsRetention = 2 * 365 * 24 * time.Hour

// CLI configures the line-based operator interface. The block's
// absence disables the network listener — the optionality rule — but
// not the local console socket, which is a base function with its own
// opt-out below.
type CLI struct {
	// Listen is the TCP address; loopback by default — the transport
	// is plaintext, authenticates nothing, and grants read-only.
	Listen string `yaml:"listen"`
	// Socket is the local admin console's unix socket path. The OS's
	// file permissions are its whole authentication: whoever may open
	// it is admin. Unset means the default path; explicitly empty
	// disables it.
	Socket *string `yaml:"socket"`
}

// DefaultCLIListen is where the CLI listens when the block is present
// but silent on the address.
const DefaultCLIListen = "127.0.0.1:2323"

// DefaultConsoleSocket is where the local admin console listens.
const DefaultConsoleSocket = "/run/lotor/console.sock"

// ConsoleSocket resolves the local console's socket path; empty means
// disabled. The default holds even without a cli block — the local
// console is a base function, the network listener is the opt-in.
// explicit reports whether the configuration chose the path itself: a
// host that cannot serve the default degrades with a warning, but an
// explicit path is a promise the daemon must keep or fail.
func (f *File) ConsoleSocket() (path string, explicit bool) {
	if f.CLI == nil || f.CLI.Socket == nil {
		return DefaultConsoleSocket, false
	}
	return *f.CLI.Socket, true
}

// System is what this installation calls itself. The name identifies
// the host an operator is standing on — in the console prompt, and in
// whatever a browser eventually puts in its title bar — so it is one
// setting, not one per surface. Absent, the machine's hostname
// answers: a name nobody chose is still better than no name.
type System struct {
	Name string `yaml:"name"`
	// LogLevel, when set, overrides the boot flag while the daemon
	// runs — the knob an investigation turns without a restart.
	LogLevel string `yaml:"log_level"`
}

// MQTT is one broker connection the daemon observes the mesh into,
// layered like a radio: a community-broker preset as the base, the
// override scope patching it. The parameter set itself lives with the
// observer code, beside the preset catalog it resolves against.
type MQTT struct {
	Layered Layered `yaml:",inline"`
	// Disabled parks the connection: configuration kept, nothing runs.
	Disabled bool `yaml:"disabled"`
}

// Update is where this relay looks for newer versions of itself.
type Update struct {
	// Channel names what to follow: release, rc, beta, dev, or a
	// try-<slug> a workflow published.
	Channel string `yaml:"channel"`
	// URL is the manifest tree; empty takes the project's own host.
	URL string `yaml:"url"`
	// Token rides as a bearer on artifact downloads — a private
	// fork's assets, typically.
	Token string `yaml:"token"`
}

// File is the top-level configuration.
type File struct {
	Radios   map[string]Radio  `yaml:"radios"`
	Relays   map[string]Relay  `yaml:"relays"`
	Sensors  map[string]Sensor `yaml:"sensors"`
	Sentinel *Sentinel         `yaml:"sentinel"`
	CLI      *CLI              `yaml:"cli"`
	System   *System           `yaml:"system"`
	Update   *Update           `yaml:"update"`
	MQTT     map[string]MQTT   `yaml:"mqtt"`
}

// The cadence a sensor may be read at. The floor keeps a bus shared
// with a radio usable; the ceiling is where a reading is too old to
// answer a telemetry question with.
const (
	MinSampleInterval = time.Second
	MaxSampleInterval = time.Hour
)

// validateSensors checks what every sensor must say. Nothing claims
// one, so there is no exclusivity to enforce — the bus belongs to the
// machine, and every relay on it reads the same part.
func validateSensors(sensors map[string]Sensor) error {
	for name, s := range sensors {
		if s.Driver == "" {
			return fmt.Errorf("sensor %q: driver is required", name)
		}
		// Bounded like every other cadence here: a bus shared with a
		// radio is not a thing to read a thousand times a second, and
		// a part read less than hourly is not being watched.
		if v := s.SampleInterval; v != 0 && (v < MinSampleInterval || v > MaxSampleInterval) {
			return fmt.Errorf("sensor %q: sample_interval %s — want %s..%s, or 0 for the default",
				name, v, MinSampleInterval, MaxSampleInterval)
		}
	}
	return nil
}

// Load reads, decodes and cross-validates a configuration file.
func Load(path string) (*File, error) {
	raw, err := os.ReadFile(path) //nolint:gosec // the operator's own -config flag
	if err != nil {
		return nil, err
	}
	var f File
	dec := yaml.NewDecoder(bytes.NewReader(raw))
	dec.KnownFields(true)
	if err := dec.Decode(&f); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	// Only one document is read; a second one would be silently
	// dropped, so its presence is an error, not a surprise.
	if err := dec.Decode(new(any)); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("%s: multiple YAML documents — everything after '---' would be ignored", path)
	}
	// A bare `cli:` or `sentinel:` key decodes to a nil pointer, the
	// same as absence — but the operator wrote it expecting something.
	var presence map[string]any
	_ = yaml.Unmarshal(raw, &presence)
	if _, ok := presence["cli"]; ok && f.CLI == nil {
		f.CLI = &CLI{} // bare cli: means "with defaults"
	}
	if _, ok := presence["sentinel"]; ok && f.Sentinel == nil {
		return nil, fmt.Errorf(`%s: sentinel: block is empty — set journal: (":memory:" for RAM-only) or remove it`, path)
	}
	if err := f.validate(); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return &f, nil
}

func (f *File) validate() error { return f.Validate(true) }

// Validate cross-checks an assembled configuration, wherever it was
// assembled. A file with no relays is a mistake — nobody writes one
// to run nothing — but a database may honestly hold none yet: a
// daemon comes up with its console and waits to be configured.
func (f *File) Validate(requireRelays bool) error {
	if requireRelays && len(f.Relays) == 0 {
		return errors.New("no relays declared")
	}
	owner := make(map[string]string, len(f.Radios))
	for name, r := range f.Relays {
		if r.Protocol == "" {
			return fmt.Errorf("relay %q: protocol is required", name)
		}
		if r.TX != nil {
			if err := r.TX.Normalize(); err != nil {
				return fmt.Errorf("relay %q: %w", name, err)
			}
		}
		if r.Radio == "" {
			return fmt.Errorf("relay %q: radio is required", name)
		}
		if _, ok := f.Radios[r.Radio]; !ok {
			return fmt.Errorf("relay %q: radio %q is not declared", name, r.Radio)
		}
		// One owner per radio: the single-owner discipline of the
		// driver layer, enforced at the config door.
		if other, taken := owner[r.Radio]; taken {
			return fmt.Errorf("radio %q is claimed by relays %q and %q — one owner per radio",
				r.Radio, other, name)
		}
		owner[r.Radio] = name
	}
	for name, r := range f.Radios {
		if r.Driver == "" {
			return fmt.Errorf("radio %q: driver is required", name)
		}
	}
	if err := validateSensors(f.Sensors); err != nil {
		return err
	}
	if f.CLI != nil && f.CLI.Listen == "" {
		f.CLI.Listen = DefaultCLIListen
	}
	if f.Sentinel != nil {
		return f.Sentinel.validate()
	}
	return nil
}

func (s *Sentinel) validate() error {
	if s.Journal == "" {
		return errors.New(`sentinel: journal path is required (":memory:" for RAM-only)`)
	}
	if s.Retention < 0 {
		return fmt.Errorf("sentinel: negative retention %s", s.Retention)
	}
	if s.Retention > 0 && s.Retention < time.Minute {
		return fmt.Errorf("sentinel: retention %s would wipe the journal at every prune", s.Retention)
	}
	if s.Retention == 0 {
		s.Retention = DefaultRetention
	}
	if s.MaxFrames < 0 {
		return fmt.Errorf("sentinel: negative max_frames %d", s.MaxFrames)
	}
	return nil
}
