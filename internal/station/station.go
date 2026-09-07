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
	"meshrunner.dev/lotor/internal/hosted"
	"meshrunner.dev/lotor/internal/radio"
	"meshrunner.dev/lotor/internal/schema"
	"meshrunner.dev/lotor/internal/version"
)

// The lifecycle, the radio attachment, the RF door and the origination
// gate are what every hosted identity shares; internal/hosted says them
// once, and this seam reads in its own vocabulary.

type (
	// State is the station lifecycle: StateStarting has not opened its
	// listener yet.
	State = hosted.State
	// RFState is the radio attachment, apart from the lifecycle: a
	// detached or failed radio never makes the listener cease to exist.
	RFState = hosted.RFState
	// RadioAttacher is the live RF door.
	RadioAttacher = hosted.RadioAttacher
	// RadioRequester exposes the live protocol-owned waveform.
	RadioRequester = hosted.RadioRequester
	// RadioDemand is what a station asks of an attachment.
	RadioDemand = hosted.RadioDemand
	// TXPolicy is the station's origination gate.
	TXPolicy = hosted.TXPolicy
)

// StateStarting and its siblings are the shared words, re-exported so a
// call site reads in this seam's own vocabulary.
const (
	StateStarting = hosted.StateStarting
	StateRunning  = hosted.StateRunning
	StateError    = hosted.StateError
	StateStopped  = hosted.StateStopped

	RFDetached = hosted.RFDetached
	RFDown     = hosted.RFDown
	RFActive   = hosted.RFActive
	RFBlocked  = hosted.RFBlocked
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
