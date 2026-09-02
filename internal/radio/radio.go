// Package radio is the seam between logical consumers and transceiver
// hardware. A Device is an opened attachment; a driver turns a resolved
// hardware configuration into one. Radios declare an Envelope of possible
// waveform choices, while a controller serializes receive and transmit work
// for the relay and stations sharing one physical attachment.
package radio

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"go.uber.org/zap"

	"meshrunner.dev/lotor/internal/correlation"
	"meshrunner.dev/lotor/internal/schema"
)

// Waveform is a logical consumer's channel choice, in protocol-neutral units.
type Waveform struct {
	FrequencyHz     uint32 `yaml:"frequency_hz"`
	SpreadingFactor int    `yaml:"spreading_factor"`
	BandwidthHz     int    `yaml:"bandwidth_hz"`
	CodingRate      int    `yaml:"coding_rate"` // denominator: 5..8
	Preamble        int    `yaml:"preamble"`
	SyncWord        byte   `yaml:"sync_word"`
	CRC             bool   `yaml:"crc"`
}

// Envelope is what a board physically allows. A zero frequency bound
// means "undeclared": nothing is checked for that axis.
type Envelope struct {
	// MaxTxPowerDBm is the integrator's declared ceiling, meaningful
	// only when MaxTxPowerSet. The two are separate because a ceiling
	// of exactly 0 dBm is a real board, and a lone zero cannot say
	// whether it means that or means nobody declared one.
	MaxTxPowerDBm int8
	MaxTxPowerSet bool
	// ChipMinDBm and ChipMaxDBm are what the part itself can be
	// programmed for, whatever ceiling sits on top. Equal values mean
	// the driver did not say, and nothing is checked against them —
	// but where it does say, a ceiling outside that range is a relay
	// that refuses its first frame instead of its configuration.
	ChipMinDBm, ChipMaxDBm int8

	FreqRangeLowHz uint32
	FreqRangeHiHz  uint32
}

// Permits judges a whole demand — the waveform and the power choice
// — against the board. Allows covers the waveform alone, for callers
// that have nothing to say about power.
//
// The power judged is the one that will actually be programmed: an
// explicit figure as written, and "auto" as the ceiling it resolves
// to. Judging only the explicit case let a board declaring 127 dBm —
// or the -128 the driver library reads as a sentinel — resolve auto
// to a power no part can key, and discover it at the first frame.
func (e Envelope) Permits(w Waveform, dbm int8, explicit bool) error {
	if err := e.Allows(w); err != nil {
		return err
	}
	want := dbm
	if !explicit {
		if !e.MaxTxPowerSet {
			// Nothing to resolve auto against; the transmit gate
			// refuses this on its own terms, with its own words.
			return nil
		}
		want = e.MaxTxPowerDBm
	}
	if e.MaxTxPowerSet && want > e.MaxTxPowerDBm {
		return fmt.Errorf("tx_power_dbm %d exceeds the radio's %d dBm cap — refusing, not clamping",
			want, e.MaxTxPowerDBm)
	}
	if e.ChipMinDBm != e.ChipMaxDBm && (want < e.ChipMinDBm || want > e.ChipMaxDBm) {
		how := "tx_power_dbm"
		if !explicit {
			how = "tx_power_dbm auto resolves to the radio's cap, which"
		}
		return fmt.Errorf("%s %d is outside what this part can key (%d..%d dBm)",
			how, want, e.ChipMinDBm, e.ChipMaxDBm)
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
	// Correlation is assigned where a decoded or corrupt reception first
	// crosses the device seam, before any frame-specific hardware log.
	// Devices that cannot assign it may leave it zero; the protocol
	// engine then assigns the fallback at its own receive boundary.
	Correlation correlation.ID
	Payload     []byte
	RSSI        float64
	SNR         float64
	// SignalRSSI is the despread signal's own power — meaningful below
	// the noise floor, where RSSI mostly measures the noise.
	SignalRSSI float64
	// FreqErrHz is the sender's carrier offset as the demodulator saw
	// it: a node drifting here frame after frame has a failing crystal.
	FreqErrHz float64
	// Binding names the consumer on this controller that emitted this
	// frame — "relay:meshcore-868", "station:alice" — and is empty for
	// everything that came off the air. A half-duplex chip cannot hear
	// itself, so the controller carries an emission to the peers that
	// were transmitting with it and heard nothing. Such a frame was
	// never demodulated: its measurements are the zero value and say
	// nothing about any link, so a reader that wants one asks here
	// first.
	Binding string
	// CausedBy is the correlation of the composed emission that the
	// controller handed to its peers. Correlation still identifies this
	// reception: one emitted fact and the peers' reception of it are two
	// distinct steps whose causal link must remain queryable. It is zero
	// for frames received from the air.
	CausedBy correlation.ID
	Airtime  time.Duration
	At       time.Time
}

// HasRFMeasurements reports whether the frame came from a demodulator.
// A locally handed-over frame is real traffic, but its zero-valued radio
// fields are absence markers rather than measurements.
func (f Frame) HasRFMeasurements() bool { return f.Binding == "" }

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

// Device is an opened radio owned by exactly one goroutine. A physical Device
// belongs to its controller; relay and station engines receive logical Device
// ports backed by that controller.
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

// Forwarder is the optional half of a Device that separates an
// emission this node composed from one it is only passing on. The
// distinction exists for the bindings sharing an antenna, and only
// for them: a peer heard the original off this very chip before we
// repeated it, and a repetition hashes the same as its original, so
// handing it over delivers nothing and is deduplicated on arrival.
// What we compose ourselves never crossed the air at all, and the
// hand-over is the only way a peer can learn it.
//
// A Device that does not implement this is assumed to have no peers
// to spare — true of every driver, and of the controller port only
// because nothing wraps it. A decorator that forwarded Device without
// forwarding this would quietly restore the hand-over it is meant to
// withhold, and say so nowhere.
type Forwarder interface {
	// TransmitForwarded keys the radio for a packet this node
	// relays rather than originates. On the air it is Transmit; it
	// differs only in withholding the hand-over.
	TransmitForwarded(ctx context.Context, payload []byte, powerDBm int8) (TxReport, error)
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

// Registered lists every driver name, sorted — for cross-cutting
// derivations like the secrets mask, which must see every schema.
func Registered() []string {
	mu.RLock()
	defer mu.RUnlock()
	names := make([]string, 0, len(drivers))
	for n := range drivers {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

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
