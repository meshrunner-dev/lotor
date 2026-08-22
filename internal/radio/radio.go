// Package radio is the seam between relays and transceiver hardware.
// A Device is an opened attachment; a driver turns a resolved hardware
// configuration into one. Radios carry no waveform choice — they
// declare an Envelope of what choices are possible, and the relay's
// choices are validated against it. There is deliberately no transmit
// on the Device interface yet: the daemon is receive-only by
// construction until the relay engines earn their transmit path.
package radio

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"go.uber.org/zap"
)

// Waveform is a relay's channel choice, in protocol-neutral units.
type Waveform struct {
	FrequencyHz     uint32 `yaml:"frequency_hz"`
	SpreadingFactor int    `yaml:"spreading_factor"`
	BandwidthHz     int    `yaml:"bandwidth_hz"`
	CodingRate      int    `yaml:"coding_rate"` // denominator: 5..8
	Preamble        int    `yaml:"preamble"`
	SyncWord        byte   `yaml:"sync_word"`
	CRC             bool   `yaml:"crc"`
}

// Envelope is what a board physically allows. Zero values mean
// "undeclared": nothing is checked for that axis.
type Envelope struct {
	MaxTxPowerDBm  int8
	FreqRangeLowHz uint32
	FreqRangeHiHz  uint32
}

// Allows verifies a waveform fits the envelope.
func (e Envelope) Allows(w Waveform) error {
	if e.FreqRangeLowHz != 0 && w.FrequencyHz < e.FreqRangeLowHz ||
		e.FreqRangeHiHz != 0 && w.FrequencyHz > e.FreqRangeHiHz {
		return fmt.Errorf("frequency %d Hz is outside the radio's range [%d, %d]",
			w.FrequencyHz, e.FreqRangeLowHz, e.FreqRangeHiHz)
	}
	return nil
}

// Frame is one received transmission.
type Frame struct {
	Payload []byte
	RSSI    float64
	SNR     float64
	Airtime time.Duration
	At      time.Time
}

// ErrCorrupt marks a reception that failed integrity checks: traffic,
// not a fault — callers count it and continue.
var ErrCorrupt = errors.New("radio: corrupt frame")

// Device is an opened radio owned by exactly one relay.
type Device interface {
	Envelope() Envelope
	Configure(w Waveform) error
	StartReceive() error
	// Receive blocks for the next frame; corrupt receptions return
	// an error wrapping ErrCorrupt.
	Receive(ctx context.Context) (Frame, error)
	Close() error
}

// Driver opens devices from a resolved configuration and publishes
// the hardware presets its boards are known by.
type Driver struct {
	Open    func(cfg map[string]any, log *zap.Logger) (Device, error)
	Presets map[string]map[string]any
}

var (
	mu      sync.RWMutex
	drivers = map[string]Driver{}
)

// Register adds a driver under its config name. Drivers register from
// init; a duplicate name is a programming error and panics.
func Register(name string, d Driver) {
	mu.Lock()
	defer mu.Unlock()
	if _, dup := drivers[name]; dup {
		panic("radio: duplicate driver " + name)
	}
	drivers[name] = d
}

// Lookup returns a registered driver.
func Lookup(name string) (Driver, error) {
	mu.RLock()
	defer mu.RUnlock()
	d, ok := drivers[name]
	if !ok {
		names := make([]string, 0, len(drivers))
		for n := range drivers {
			names = append(names, n)
		}
		sort.Strings(names)
		return Driver{}, fmt.Errorf("unknown radio driver %q (known: %v)", name, names)
	}
	return d, nil
}
