// Package protocol is the registry of mesh protocols a relay can
// speak. A protocol builds an Engine from a resolved relay
// configuration; the engine owns the waveform choice and the frame
// judgement, the relay owns the lifecycle around it.
package protocol

import (
	"meshrunner.dev/lotor/internal/schema"

	"time"

	"context"
	"fmt"
	"sort"
	"sync"

	"go.uber.org/zap"

	"meshrunner.dev/lotor/internal/bus"
	"meshrunner.dev/lotor/internal/radio"
)

// Engine judges frames for one relay.
type Engine interface {
	// Waveform is the channel choice this engine resolved from its
	// configuration; the relay validates it against the radio's
	// envelope before the engine runs.
	Waveform() radio.Waveform
	// TxPower is the configured transmit power choice; explicit is
	// false for "auto". Validated against the radio's cap at load,
	// applied when a transmit path exists.
	TxPower() (dbm int8, explicit bool)
	// Identity is this relay's node public key in hex, empty when the
	// relay has none configured.
	Identity() string
	// Run consumes the device until the context ends or the device
	// fails. The device arrives configured and receiving.
	Run(ctx context.Context, dev radio.Device) error
}

// TXPolicy is the transmit contract a relay resolved from its
// configuration and hands to an engine that can honour it: the gate,
// the channel-politeness knobs, the queue bound and the power the
// pipeline accounts for.
type TXPolicy struct {
	Mode           string // dry, shadow, on-air
	LBTThresholdDB float64
	LBTExhausted   string // transmit or drop
	QueueDepth     int
	PowerDBm       int8
	// Spent seeds the duty ledger with the journal's memory of the
	// last hour; empty when no journal runs — best effort, by design.
	Spent []Spent
}

// Spent is one past emission the duty ledger must still count: the
// sliding hour does not restart with the process.
type Spent struct {
	At      time.Time
	Airtime time.Duration
}

// Armer is the optional capability of engines that own a transmit
// pipeline. Arm is called once at assembly, before Run; an engine that
// cannot honour the policy (no node identity to relay under, say)
// refuses, and the relay is stillborn rather than silently dry.
type Armer interface {
	Arm(policy TXPolicy) error
}

// Builder turns a resolved relay configuration into an engine. Check
// validates a configuration without building — the config loader runs
// it over every override scope, so a typo under a profile that is not
// selected today still fails today.
type Builder struct {
	Build func(relayName string, cfg map[string]any, b *bus.Bus, log *zap.Logger) (Engine, error)
	Check func(cfg map[string]any) error
	// Asks is what a configuration would demand of the radio — the
	// waveform and the power choice — without building an engine.
	// An engine answers the same, but only once built: too late to
	// refuse a mutation before it reaches the store.
	Asks    func(cfg map[string]any) (w radio.Waveform, dbm int8, explicit bool, err error)
	Presets map[string]map[string]any
	// Schema declares every attribute the protocol accepts — the
	// administration channels' single source for help, completion and
	// validation vocabulary.
	Schema []schema.Attr
}

var (
	mu       sync.RWMutex
	builders = map[string]Builder{}
)

// Register adds a protocol under its config name. Protocols register
// from init; a duplicate name is a programming error and panics.
func Register(name string, b Builder) {
	mu.Lock()
	defer mu.Unlock()
	if _, dup := builders[name]; dup {
		panic("protocol: duplicate protocol " + name)
	}
	builders[name] = b
}

// Lookup returns a registered protocol.
func Lookup(name string) (Builder, error) {
	mu.RLock()
	defer mu.RUnlock()
	b, ok := builders[name]
	if !ok {
		names := make([]string, 0, len(builders))
		for n := range builders {
			names = append(names, n)
		}
		sort.Strings(names)
		return Builder{}, fmt.Errorf("unknown protocol %q (known: %v)", name, names)
	}
	return b, nil
}
