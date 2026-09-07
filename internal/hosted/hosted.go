// Package hosted is what a station and an application have in common
// from the daemon's point of view: a mesh identity the daemon hosts,
// with a lifecycle of its own and a radio attachment it can live
// without. The two seams — internal/station, internal/application —
// each owned a copy of these shapes; the manager treated the copies
// as one thing by hand, path for path. This package is the one place
// they are said, and the seams alias them. What differs between the
// two — what a builder is given, what a snapshot reports — stays with
// each seam, because it genuinely differs.
package hosted

import "meshrunner.dev/lotor/internal/radio"

// State is the lifecycle visible to operators.
type State string

const (
	// StateStarting has not begun serving yet.
	StateStarting State = "starting"
	// StateRunning is serving — on the air when RF is active, otherwise
	// holding its state and waiting for a radio.
	StateRunning State = "running"
	// StateError exposes the failure cause.
	StateError State = "error"
	// StateStopped is terminal after the context ends.
	StateStopped State = "stopped"
)

// RFState is the radio attachment, deliberately apart from the
// lifecycle: a hosted identity without a radio is idle, not broken. A
// station's listener and a room's members exist whether or not the
// radio does.
type RFState string

const (
	// RFDetached means no radio is configured.
	RFDetached RFState = "detached"
	// RFDown means an attachment is configured but unavailable.
	RFDown RFState = "down"
	// RFActive means the identity may receive and submit emissions.
	RFActive RFState = "active"
	// RFBlocked means another consumer owns an incompatible waveform.
	RFBlocked RFState = "blocked"
)

// RadioAttacher is the optional live RF door. A manager may move a
// hosted identity between radios without stopping its service.
type RadioAttacher interface {
	AttachRadio(name string, binding *radio.Binding, duty *radio.AirtimeLedger, cause string)
}

// RadioRequester exposes the live protocol-owned waveform, which may
// differ from the configured default once a companion or an admin
// changed it and the change survived a restart.
type RadioRequester interface {
	RadioDemand() RadioDemand
}

// RadioDemand is everything a hosted identity asks from an attachment.
// Duty stays a percentage here because the manager owns conversion to
// the one shared sliding-hour ledger.
type RadioDemand struct {
	Waveform     radio.Waveform
	PowerDBm     int8
	DutyCyclePct float64
}

// TXPolicy is the protocol-neutral origination gate. A hosted identity
// never earns a forwarding rung: dry, shadow and on-air are the whole
// ladder.
type TXPolicy struct {
	Mode           string
	LBTThresholdDB float64
	LBTExhausted   string
	CAD            bool
	QueueDepth     int
}
