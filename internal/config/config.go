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

// File is the top-level configuration.
type File struct {
	Radios map[string]Radio `yaml:"radios"`
	Relays map[string]Relay `yaml:"relays"`
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
	return nil
}
