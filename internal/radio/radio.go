// Package radio is the seam between relays and transceiver hardware.
// A Device is an opened attachment; a driver turns a resolved hardware
// configuration into one. Radios carry no waveform choice — they
// declare an Envelope of what choices are possible, and the relay's
// choices are validated against it. There is deliberately no transmit
// on the Device interface yet: the daemon is receive-only by
// construction until the relay engines earn their transmit path.
package radio

import (
	"meshrunner.dev/lotor/internal/schema"

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

// Permits judges a whole demand — the waveform and the power choice
// — against the board. Allows covers the waveform alone, for callers
// that have nothing to say about power.
func (e Envelope) Permits(w Waveform, dbm int8, explicit bool) error {
	if err := e.Allows(w); err != nil {
		return err
	}
	if explicit && e.MaxTxPowerDBm != 0 && dbm > e.MaxTxPowerDBm {
		return fmt.Errorf("tx_power_dbm %d exceeds the radio's %d dBm cap — refusing, not clamping",
			dbm, e.MaxTxPowerDBm)
	}
	return nil
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
	// SignalRSSI is the despread signal's own power — meaningful below
	// the noise floor, where RSSI mostly measures the noise.
	SignalRSSI float64
	// FreqErrHz is the sender's carrier offset as the demodulator saw
	// it: a node drifting here frame after frame has a failing crystal.
	FreqErrHz float64
	Airtime   time.Duration
	At        time.Time
}

// ChipStats are the transceiver's own reception counters — an
// independent second opinion on the daemon's tallies.
type ChipStats struct {
	Received     uint16
	CRCErrors    uint16
	HeaderErrors uint16
}

// TxReport is one completed transmission, as the duty ledger records
// it: when, how long, and the power the chip was actually programmed
// for — independent of caller bookkeeping.
type TxReport struct {
	At       time.Time
	Airtime  time.Duration
	Duration time.Duration
	PowerDBm int8
}

// ErrCorrupt marks a reception that failed integrity checks: traffic,
// not a fault — callers count it and continue.
var ErrCorrupt = errors.New("radio: corrupt frame")

// ErrBusyReceiving marks an operation refused because the radio has
// reception work in hand — a frame arriving, or one latched and not
// yet collected. It is the channel being busy, never a fault: the
// caller collects and comes back, it does not tear a session down.
var ErrBusyReceiving = errors.New("radio: reception pending")

// NoiseFloor is the channel's ambient level: what the radio hears
// between frames, when nothing is arriving and nothing transmits.
// DBm is the batch's median — the robust estimator radio-noise
// practice characterizes ambient noise by, immune to the impulsive
// bursts that drag a mean up. SpreadDB is the batch's 90th percentile
// above that median: the site's impulsiveness, near zero on a clean
// channel, growing when something pulses.
type NoiseFloor struct {
	DBm      float64
	SpreadDB float64
	At       time.Time
}

// Telemetry is a device's measurement read side: cached state written
// by the owning goroutine, safe for any goroutine to consult, never a
// hardware touch.
type Telemetry interface {
	// NoiseFloor reports the last measured floor; ok is false until
	// the first measurement converges.
	NoiseFloor() (NoiseFloor, bool)
	// NoiseStarved counts noise-floor batches abandoned because the
	// channel left too few idle gaps to observe within a batch's age
	// bound. Cumulative.
	NoiseStarved() uint64
	// ChipStats reports the transceiver's own reception counters,
	// refreshed periodically by the receive loop; ok is false until
	// the first read.
	ChipStats() (ChipStats, bool)
}

// Device is an opened radio owned by exactly one relay.
type Device interface {
	Envelope() Envelope
	Configure(w Waveform) error
	StartReceive() error
	// Receive blocks for the next frame; corrupt receptions return
	// an error wrapping ErrCorrupt. While it waits, the device keeps
	// the noise floor current — measurement is part of listening.
	Receive(ctx context.Context) (Frame, error)
	// Telemetry is the read side any goroutine may consult.
	Telemetry
	// Transmit keys the radio: it belongs to the owning goroutine,
	// exactly like Receive, and returns once the frame is on the air.
	// The gates deciding whether keying is allowed at all — dry,
	// shadow, on-air — live above this seam; a Device transmits when
	// told to.
	Transmit(ctx context.Context, payload []byte, powerDBm int8) (TxReport, error)
	// AssessChannel is the listen-before-talk verdict: an optional
	// RSSI stage (thresholdDB above the measured floor; zero skips
	// it) then the hardware's own activity detection. Owning
	// goroutine only.
	AssessChannel(ctx context.Context, thresholdDB float64) (busy bool, err error)
	// Airtime computes a frame's channel occupancy at the configured
	// waveform — pure arithmetic, any goroutine, and what a shadow
	// emission journals for a frame it never keyed.
	Airtime(bytes int) time.Duration
	Close() error
}

// Driver opens devices from a resolved configuration and publishes
// the hardware presets its boards are known by. Inspect validates a
// configuration and reports its envelope without touching hardware —
// the config loader's dry run, usable on any platform.
type Driver struct {
	Open    func(cfg map[string]any, log *zap.Logger) (Device, error)
	Inspect func(cfg map[string]any) (Envelope, error)
	// CheckTransmit reports why this radio could not transmit as
	// configured — a part the driver cannot identify, a ceiling left
	// undeclared. Nil means the driver has no transmit prerequisites.
	// A relay whose gate leaves dry asks this at assembly, so a
	// transmitter that could never key is a stillborn relay instead of
	// one that reopens its radio every few seconds forever.
	CheckTransmit func(cfg map[string]any) error
	// CheckWaveform judges one channel choice without hardware, using
	// the very conversion Configure performs. The envelope answers
	// "may this board key there"; this answers the prior question,
	// "can the chip be programmed with this at all" — a spreading
	// factor, bandwidth, coding rate or preamble outside what the part
	// accepts. Nil means the driver takes any waveform the envelope
	// allows.
	CheckWaveform func(w Waveform) error
	Presets       map[string]map[string]any
	// Schema declares every attribute the driver accepts — the
	// administration channels' single source for help, completion and
	// validation vocabulary.
	Schema []schema.Attr
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
