package bus

import (
	"time"

	"meshrunner.dev/lotor/internal/txn"
)

// FrameHeard is published for every frame a relay's radio delivers,
// before any protocol judgement.
type FrameHeard struct {
	Relay   string
	Txn     txn.ID
	At      time.Time
	Bytes   int
	RSSI    float64
	SNR     float64
	Airtime time.Duration
}

// FrameJudged is the protocol engine's verdict on a heard frame.
type FrameJudged struct {
	Relay   string
	Txn     txn.ID
	Verdict string
	// DuplicateOf links a suppressed frame to the transaction that
	// carried the first copy, so log chains can be followed.
	DuplicateOf string
	// Wire identity, protocol vocabulary; empty on malformed frames.
	Type    string
	Route   string
	PathLen int
	// What the frame says, when its payload speaks an open format:
	// the advertised node name, a public-key prefix, a human detail.
	Node   string
	PubKey string
	Detail string
}

// RelayState is published on every relay lifecycle transition.
type RelayState struct {
	Relay string
	State string
	Err   string
}
