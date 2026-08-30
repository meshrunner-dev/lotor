// Package station is the protocol-neutral registry and lifecycle seam for
// locally hosted end-user mesh identities. A station owns its application
// listener and durable protocol state; an optional radio attachment is a
// separate capability supplied by the daemon.
package station

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"go.uber.org/zap"

	"meshrunner.dev/lotor/internal/bus"
	"meshrunner.dev/lotor/internal/radio"
	"meshrunner.dev/lotor/internal/schema"
	"meshrunner.dev/lotor/internal/version"
)

// State is the station lifecycle visible to operators.
type State string

const (
	// StateStarting has not opened the application listener yet.
	StateStarting State = "starting"
	// StateRunning is accepting companion application connections.
	StateRunning State = "running"
	// StateError exposes the listener or service failure cause.
	StateError State = "error"
	// StateStopped is terminal after the station context ends.
	StateStopped State = "stopped"
)

// RFState deliberately differs from the station lifecycle: a detached or
// failed radio never makes the application listener cease to exist.
type RFState string

const (
	// RFDetached means no radio is configured for this station.
	RFDetached RFState = "detached"
	// RFDown means an attachment is configured but unavailable.
	RFDown RFState = "down"
	// RFActive means the station may receive and submit emissions.
	RFActive RFState = "active"
	// RFBlocked means another consumer owns an incompatible waveform.
	RFBlocked RFState = "blocked"
)

// Info is a coherent runtime snapshot.
type Info struct {
	Name       string
	Protocol   string
	Listen     string
	Radio      string
	State      State
	Cause      string
	RF         RFState
	RFCause    string
	Connected  bool
	Remote     string
	Mailbox    int
	MailboxCap int
	Waveform   radio.Waveform
	PublicKey  string
}

// Service owns one station's listener and protocol state.
type Service interface {
	Run(ctx context.Context) error
	Info() Info
}

// RadioAttacher is the optional live RF door. A manager may move a station
// between radios without stopping its application listener or TCP client.
type RadioAttacher interface {
	AttachRadio(name string, binding *radio.Binding, duty *radio.AirtimeLedger, cause string)
}

// RadioRequester exposes the live protocol-owned waveform. It may differ from
// the configuration default after a companion application changed its radio
// parameters and that preference survived a daemon restart.
type RadioRequester interface {
	RadioDemand() RadioDemand
}

// RadioDemand is everything a station protocol asks from an attachment. Duty
// remains a percentage here because the manager owns conversion to the one
// shared sliding-hour ledger.
type RadioDemand struct {
	Waveform     radio.Waveform
	PowerDBm     int8
	DutyCyclePct float64
}

// TXPolicy is the station's protocol-neutral origination gate. Stations never
// receive a forwarding rung: dry, shadow and on-air are the whole ladder.
type TXPolicy struct {
	Mode           string
	LBTThresholdDB float64
	LBTExhausted   string
	CAD            bool
	QueueDepth     int
}

// StateStore is the protocol-neutral durable home for one station's mutable
// companion state. The payload is owned and versioned by the protocol
// implementation; the configuration store only guarantees atomic bytes.
type StateStore interface {
	LoadStationState(ctx context.Context, station string) ([]byte, bool, error)
	SaveStationState(ctx context.Context, station string, state []byte) error
}

// Spec is the protocol-neutral structure resolved before a protocol builder
// sees its contributed configuration.
type Spec struct {
	Name     string
	Protocol string
	Listen   string
	Radio    string
	Config   map[string]any
	Log      *zap.Logger
	Build    version.Info
	State    StateStore
	TX       TXPolicy
	Bus      *bus.Bus
}

// Builder constructs and validates one station protocol implementation.
type Builder struct {
	Build   func(Spec) (Service, error)
	Check   func(map[string]any) error
	Asks    func(map[string]any) (RadioDemand, error)
	Presets map[string]map[string]any
	Schema  []schema.Attr
}

var (
	registryMu sync.RWMutex
	builders   = map[string]Builder{}
)

// Register adds a station protocol. Duplicate registration is a programming
// error and panics during assembly.
func Register(name string, builder Builder) {
	registryMu.Lock()
	defer registryMu.Unlock()
	if _, exists := builders[name]; exists {
		panic("station: duplicate protocol " + name)
	}
	builders[name] = builder
}

// Lookup finds a station protocol builder.
func Lookup(name string) (Builder, error) {
	registryMu.RLock()
	defer registryMu.RUnlock()
	builder, ok := builders[name]
	if ok {
		return builder, nil
	}
	return Builder{}, fmt.Errorf("unknown station protocol %q (known: %v)", name, registeredLocked())
}

// Registered lists station protocols in stable order.
func Registered() []string {
	registryMu.RLock()
	defer registryMu.RUnlock()
	return registeredLocked()
}

func registeredLocked() []string {
	names := make([]string, 0, len(builders))
	for name := range builders {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
