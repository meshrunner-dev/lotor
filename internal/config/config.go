// Package config loads the daemon's configuration file and implements
// its layering paradigm: named profiles with per-profile override
// scopes, instantiated for radio hardware and for relay regions alike.
// Validation is strict everywhere — unknown keys, unknown profiles and
// dangling references fail the load with a message, because a config
// that half-applies is worse than one that refuses.
package config

import (
	"bytes"
	"errors"
	"fmt"
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

// Relay declares one protocol instance: which protocol it speaks,
// which radio it owns, and the layered waveform configuration
// (region/channel presets and their overrides).
type Relay struct {
	Protocol string  `yaml:"protocol"`
	Radio    string  `yaml:"radio"`
	Layered  Layered `yaml:",inline"`
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
	MaxFrames int `yaml:"max_frames"`
}

// DefaultRetention keeps the journal for a long default, as an
// observation archive should.
const DefaultRetention = 30 * 24 * time.Hour

// CLI configures the line-based operator interface. Absent means no
// listener at all, the same optionality rule as the sentinel.
type CLI struct {
	// Listen is the TCP address; loopback by default — the transport
	// is plaintext and v1 is read-only.
	Listen string `yaml:"listen"`
}

// DefaultCLIListen is where the CLI listens when the block is present
// but silent on the address.
const DefaultCLIListen = "127.0.0.1:2323"

// File is the top-level configuration.
type File struct {
	Radios   map[string]Radio `yaml:"radios"`
	Relays   map[string]Relay `yaml:"relays"`
	Sentinel *Sentinel        `yaml:"sentinel"`
	CLI      *CLI             `yaml:"cli"`
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
	if err := f.validate(); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return &f, nil
}

func (f *File) validate() error {
	if len(f.Relays) == 0 {
		return errors.New("no relays declared")
	}
	owner := make(map[string]string, len(f.Radios))
	for name, r := range f.Relays {
		if r.Protocol == "" {
			return fmt.Errorf("relay %q: protocol is required", name)
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
	if f.CLI != nil && f.CLI.Listen == "" {
		f.CLI.Listen = DefaultCLIListen
	}
	if f.Sentinel != nil {
		if f.Sentinel.Journal == "" {
			return errors.New(`sentinel: journal path is required (":memory:" for RAM-only)`)
		}
		if f.Sentinel.Retention == 0 {
			f.Sentinel.Retention = DefaultRetention
		}
	}
	return nil
}
