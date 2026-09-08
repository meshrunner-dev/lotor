// Package application is the registry and lifecycle seam for hosted
// mesh identities that serve peers over the air — a room server first.
// An application is neither a relay nor a station: it never forwards,
// and its users are on the mesh rather than on a local socket. Like a
// station it owns its durable protocol state and exists while detached
// from RF; the radio attachment is a separate capability the daemon
// supplies and withdraws.
//
// The registry and the lifecycle know no protocol. The Spec a builder
// receives does: it hands over MeshCore's session store, because every
// type registered today speaks MeshCore, and pretending otherwise would
// be a neutrality of names only. When a second protocol brings its own
// notion of a session, that field is where the seam will have to widen.
package application

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"go.uber.org/zap"

	"meshrunner.dev/lotor/internal/bus"
	"meshrunner.dev/lotor/internal/confdb"
	"meshrunner.dev/lotor/internal/hosted"
	"meshrunner.dev/lotor/internal/meshcorehost"
	"meshrunner.dev/lotor/internal/radio"
	"meshrunner.dev/lotor/internal/schema"
	"meshrunner.dev/lotor/internal/version"
)

// The lifecycle, the radio attachment, the RF door and the origination
// gate are what every hosted identity shares; internal/hosted says them
// once, and this seam reads in its own vocabulary.

type (
	// State is the application lifecycle.
	State = hosted.State
	// RFState is the radio attachment, apart from the lifecycle: an
	// application without a radio is idle, not broken.
	RFState = hosted.RFState
	// RadioAttacher is the live RF door.
	RadioAttacher = hosted.RadioAttacher
	// RadioRequester exposes the live protocol-owned waveform.
	RadioRequester = hosted.RadioRequester
	// RadioDemand is what an application asks of an attachment.
	RadioDemand = hosted.RadioDemand
	// TXPolicy is the application's origination gate.
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

// Spec is what the daemon resolves before a type's builder sees its
// contributed configuration.
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
	// keeps the table in memory. Removing or replacing a member also
	// removes the associated cursor and receipt in the same transaction.
	Sessions meshcorehost.SessionStore
	// Store is the configuration store itself, for the tables a type
	// keeps beside the revision trail — a room's posts and cursors.
	// Nil is the memory-only posture. A type declares the few methods
	// it actually uses as an interface of its own and assigns this to
	// it, so what it asks of the store is written down where the
	// asking happens.
	Store *confdb.Store
}

// Builder constructs and validates one application type.
type Builder struct {
	// Protocol is the mesh this type speaks; the registry keys types
	// by name and holds each to its protocol.
	Protocol string
	Build    func(Spec) (Service, error)
	Check    func(map[string]any) error
	// CheckStored optionally rejects a configuration that cannot
	// preserve existing durable state, before a live mutation is saved.
	// It reads only, first during preflight and again after live writers
	// stop. Build must enforce the same invariant at startup.
	CheckStored func(Spec) error
	Asks        func(map[string]any) (RadioDemand, error)
	Presets     map[string]map[string]any
	Schema      []schema.Attr
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
