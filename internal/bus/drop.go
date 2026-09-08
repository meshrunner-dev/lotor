package bus

import "fmt"

// DropReason identifies why an emission was refused. Its numeric value
// stays inside the process; logs, JSON and the journal use the stable
// text returned by String. The zero value means there was no refusal.
type DropReason uint8

const (
	// DropNone means no emission was refused.
	DropNone DropReason = iota
	// DropDry means the transmission gate does not permit an emission.
	DropDry
	// DropRadioDown means no usable radio is attached.
	DropRadioDown
	// DropDuty means the airtime budget could not admit the emission.
	DropDuty
	// DropDutyUnavailable means the producer has no airtime ledger.
	DropDutyUnavailable
	// DropExpired means the emission outlived its useful lifetime.
	DropExpired
	// DropCancelled means the owner stopped before the emission radiated.
	DropCancelled
	// DropTXFailed means the radio failed without radiating the frame.
	DropTXFailed
	// DropLBTFailed means the channel assessment failed.
	DropLBTFailed
	// DropLBT means the channel stayed busy past the site's patience.
	DropLBT
	// DropQueueFull means the outbound queue could not hold the emission.
	DropQueueFull
	// DropMalformed means the packet could not be composed or forwarded.
	DropMalformed
	// DropDuplicate means a prior copy already accounts for this emission.
	DropDuplicate
	// DropRateLimited means a response exceeded its request allowance.
	DropRateLimited
	// DropSessionStore means the access replay guard could not be persisted.
	DropSessionStore
	// DropSessionRestart means a relay's radio session ended with queued work.
	DropSessionRestart
	// DropStationRestart means a station restarted with queued work.
	DropStationRestart
	// DropComposeFailed means a hosted application's packet did not marshal.
	DropComposeFailed

	dropReasonCount
)

// String returns the historical journal label. An invalid value is
// visible as unknown rather than mistaken for DropNone.
func (r DropReason) String() string {
	switch r {
	case DropNone:
		return ""
	case DropDry:
		return "dry"
	case DropRadioDown:
		return "radio-down"
	case DropDuty:
		return "duty"
	case DropDutyUnavailable:
		return "duty-unavailable"
	case DropExpired:
		return "expired"
	case DropCancelled:
		return "cancelled"
	case DropTXFailed:
		return "tx-failed"
	case DropLBTFailed:
		return "lbt-failed"
	case DropLBT:
		return "lbt"
	case DropQueueFull:
		return "queue-full"
	case DropMalformed:
		return "malformed"
	case DropDuplicate:
		return "duplicate"
	case DropRateLimited:
		return "rate-limited"
	case DropSessionStore:
		return "session-store"
	case DropSessionRestart:
		return "session-restart"
	case DropStationRestart:
		return "station-restart"
	case DropComposeFailed:
		return "compose-failed"
	default:
		return "unknown"
	}
}

// MarshalText keeps the event's external representation textual; an
// invalid internal value cannot be exported as a recognised refusal.
func (r DropReason) MarshalText() ([]byte, error) {
	if r >= dropReasonCount {
		return nil, fmt.Errorf("unknown drop reason value %d", r)
	}
	return []byte(r.String()), nil
}

// UnmarshalText accepts only the defined labels, including empty for
// DropNone. Invalid input leaves the receiver unchanged. Historical
// journal rows remain strings and do not pass through this parser.
func (r *DropReason) UnmarshalText(text []byte) error {
	for candidate := range dropReasonCount {
		if candidate.String() == string(text) {
			*r = candidate
			return nil
		}
	}
	return fmt.Errorf("unknown drop reason %q", text)
}
