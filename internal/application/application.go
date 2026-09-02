// Package application is the protocol-neutral registry and lifecycle seam
// for hosted mesh identities that serve peers over the air — a room
// server first. An application is neither a relay nor a station: it
// never forwards, and its users are on the mesh rather than on a local
// socket. Like a station it owns its durable protocol state and exists
// while detached from RF; the radio attachment is a separate capability
// the daemon supplies and withdraws.
package application

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"go.uber.org/zap"

	"meshrunner.dev/lotor/internal/bus"
	"meshrunner.dev/lotor/internal/confdb"
	"meshrunner.dev/lotor/internal/meshcorehost"
	"meshrunner.dev/lotor/internal/radio"
	"meshrunner.dev/lotor/internal/schema"
	"meshrunner.dev/lotor/internal/version"
)

// State is the application lifecycle visible to operators.
type State string

const (
	// StateStarting has not begun serving yet.
	StateStarting State = "starting"
	// StateRunning is serving — on the air when RF is active, otherwise
	// holding its state and waiting for a radio.
	StateRunning State = "running"
	// StateError exposes the failure cause.
	StateError State = "error"
	// StateStopped is terminal after the application context ends.
	StateStopped State = "stopped"
)

// RFState is the radio attachment, deliberately apart from the
// lifecycle: an application without a radio is idle, not broken.
type RFState string

const (
	// RFDetached means no radio is configured for this application.
	RFDetached RFState = "detached"
	// RFDown means an attachment is configured but unavailable.
	RFDown RFState = "down"
	// RFActive means the application may receive and submit emissions.
	RFActive RFState = "active"
	// RFBlocked means another consumer owns an incompatible waveform.
	RFBlocked RFState = "blocked"
)

// Info is a coherent runtime snapshot. Summary is the type's own
// status line — members, posts, whatever it counts — keyed by the
// words the console prints, so no surface needs per-type knowledge.
type Info struct {
	Name      string
	Protocol  string
	Type      string
	Radio     string
	State     State
	Cause     string
	RF        RFState
	RFCause   string
	Waveform  radio.Waveform
	PublicKey string
	Summary   map[string]string
}

// Service owns one application's protocol state and serves it.
type Service interface {
	Run(ctx context.Context) error
	Info() Info
}

// RadioAttacher is the optional live RF door. A manager may move an
// application between radios without stopping its service.
type RadioAttacher interface {
	AttachRadio(name string, binding *radio.Binding, duty *radio.AirtimeLedger, cause string)
}

// RadioRequester exposes the live protocol-owned waveform, which may
// differ from the configured default once an admin changed it over the
// air and the change survived a restart.
type RadioRequester interface {
	RadioDemand() RadioDemand
}

// RadioDemand is everything an application asks from an attachment.
// Duty stays a percentage here because the manager owns conversion to
// the one shared sliding-hour ledger.
type RadioDemand struct {
	Waveform     radio.Waveform
	PowerDBm     int8
	DutyCyclePct float64
}

// TXPolicy is the application's protocol-neutral origination gate. An
// application never earns a forwarding rung: dry, shadow and on-air
// are the whole ladder.
type TXPolicy struct {
	Mode           string
	LBTThresholdDB float64
	LBTExhausted   string
	CAD            bool
	QueueDepth     int
}

// Spec is the protocol-neutral structure resolved before a type's
// builder sees its contributed configuration.
type Spec struct {
	Name     string
	Protocol string
	Type     string
	Radio    string
	Config   map[string]any
	Log      *zap.Logger
	Build    version.Info
	TX       TXPolicy
	Bus      *bus.Bus
	// Sessions is the application's durable access list — its members
	// and their roles — keyed to it in the configuration store; nil
	// keeps the table in memory.
	Sessions meshcorehost.SessionStore
	// Store is the configuration store itself, for the tables a type
	// keeps beside the revision trail — a room's posts and cursors.
	// Nil is the memory-only posture.
	Store *confdb.Store
}

// Builder constructs and validates one application type.
type Builder struct {
	// Protocol is the mesh this type speaks; the registry keys types
	// by name and holds each to its protocol.
	Protocol string
	Build    func(Spec) (Service, error)
	Check    func(map[string]any) error
	Asks     func(map[string]any) (RadioDemand, error)
	Presets  map[string]map[string]any
	Schema   []schema.Attr
}

var (
	registryMu sync.RWMutex
	builders   = map[string]Builder{}
)

// Register adds an application type under its name. Types are unique
// across protocols for now — the console's choice attribute resolves a
// type's attributes by that one word — and a duplicate is a
// programming error that panics during assembly.
func Register(typeName string, builder Builder) {
	registryMu.Lock()
	defer registryMu.Unlock()
	if builder.Protocol == "" {
		panic("application: type " + typeName + " names no protocol")
	}
	if _, exists := builders[typeName]; exists {
		panic("application: duplicate type " + typeName)
	}
	builders[typeName] = builder
}

// Lookup finds a type's builder and holds it to the protocol asked for:
// a room is a MeshCore room, and asking for it under another mesh is a
// configuration error, not a near miss.
func Lookup(protocol, typeName string) (Builder, error) {
	builder, err := LookupType(typeName)
	if err != nil {
		return Builder{}, err
	}
	if builder.Protocol != protocol {
		return Builder{}, fmt.Errorf("application type %q speaks %s, not %q", typeName, builder.Protocol, protocol)
	}
	return builder, nil
}

// LookupType finds a type's builder by name alone — what the
// configuration vocabulary needs to list a type's attributes.
func LookupType(typeName string) (Builder, error) {
	registryMu.RLock()
	defer registryMu.RUnlock()
	builder, ok := builders[typeName]
	if ok {
		return builder, nil
	}
	return Builder{}, fmt.Errorf("unknown application type %q (known: %v)", typeName, registeredLocked())
}

// Registered lists application types in stable order.
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
