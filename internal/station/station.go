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

	"meshrunner.dev/lotor/internal/radio"
	"meshrunner.dev/lotor/internal/schema"
	"meshrunner.dev/lotor/internal/version"
)

// State is the station lifecycle visible to operators.
type State string

const (
	StateStarting State = "starting"
	StateRunning  State = "running"
	StateError    State = "error"
	StateStopped  State = "stopped"
)

// RFState deliberately differs from the station lifecycle: a detached or
// failed radio never makes the application listener cease to exist.
type RFState string

const (
	RFDetached RFState = "detached"
	RFDown     RFState = "down"
	RFActive   RFState = "active"
	RFBlocked  RFState = "blocked"
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
	Connected  bool
	Remote     string
	Mailbox    int
	MailboxCap int
	Waveform   radio.Waveform
	PublicKey  string
}

// Service owns one station's listener and protocol state.
type Service interface {
	Run(context.Context) error
	Info() Info
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
}

// Builder constructs and validates one station protocol implementation.
type Builder struct {
	Build   func(Spec) (Service, error)
	Check   func(map[string]any) error
	Asks    func(map[string]any) (radio.Waveform, error)
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
