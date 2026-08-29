// Package sensor is the daemon's vocabulary for the peripherals a
// machine reads about itself and its surroundings — a battery monitor, a
// thermometer, a barometer.
//
// The types here name physical quantities, never a protocol's encoding
// of them. MeshCore's Cayenne LPP is one consumer among the ones to
// come, and a volt outlives the byte that carried it; the mapping
// belongs to the protocol engine, which is the layer that knows what
// it is speaking.
//
// The hardware seam of internal/radio applies here unchanged: a bus, a
// register map and an ioctl stop at the driver, and everything above
// speaks Device and Reading.
package sensor

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"go.uber.org/zap"

	"meshrunner.dev/lotor/internal/schema"
)

// Quantity is what a reading measures. Named for the physical
// quantity rather than the sensor that produced it: two boards
// measuring a battery in different ways still report Voltage, and a
// consumer that wants a voltage should not have to know which chip
// answered.
type Quantity string

// The quantities a driver reports. Only what some driver measures is
// named here — a constant nothing produces is a guess about hardware
// nobody has wired yet, and it costs one line to add the day it is.
// A consumer maps these to whatever its protocol encodes; one it does
// not know, it does not report.
const (
	Voltage Quantity = "voltage"
	Current Quantity = "current"
	Power   Quantity = "power"
)

// Units, stated once so nothing downstream has to guess: volts, amps,
// watts. A driver converts at its own edge — the raw register scaling
// is the datasheet's business, not this package's — and a quantity
// added here brings its unit with it.

// Reading is one measurement and the moment it was taken. The moment
// travels because a cached reading is only as good as its age, and
// the consumer — not the sampler — is what knows how stale is too
// stale for the question being asked.
type Reading struct {
	Quantity Quantity
	Value    float64
	At       time.Time
}

// Device is one attached sensor. Read belongs to a single owning
// goroutine, exactly as radio.Device's Receive does: a bus
// transaction is not reentrant, and nothing above this interface may
// call Read from two places at once.
//
// Read may block for as long as the bus makes it block — that is the
// whole reason a Sampler exists to own it — but it must honour its
// context and return, as radio.Device's waits do. A driver on a bus
// that can hang is what bounds the hang: no context reaches a
// syscall already blocked in the kernel, so the adapter's own timeout
// is the only thing between a stuck slave and a daemon that cannot
// stop.
type Device interface {
	// Read takes one sample of everything this device measures. It
	// returns what the readings say, or the reason it could not ask.
	// A device that measures nothing right now returns no readings
	// and no error; that is a device warming up, not a fault. The
	// slice is copied before anyone else sees it, so a driver may
	// keep its own and refill it.
	Read(ctx context.Context) ([]Reading, error)
	Close() error
}

// Driver opens devices from a resolved configuration. Inspect
// validates one without touching hardware — the config loader's dry
// run, usable on a machine where the part is not attached, mirroring
// radio.Driver.
//
// There is no preset catalogue, unlike radio.Driver: a sensor kind
// carries no profile knob, so a catalogue could never be selected and
// one declared here would be silently ignored.
type Driver struct {
	Open    func(cfg map[string]any, log *zap.Logger) (Device, error)
	Inspect func(cfg map[string]any) error
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
		panic("sensor: duplicate driver " + name)
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
		return Driver{}, fmt.Errorf("sensor: no driver %q — known: %v", name, names)
	}
	return d, nil
}
