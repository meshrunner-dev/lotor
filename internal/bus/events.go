package bus

import (
	"time"

	"meshrunner.dev/lotor/internal/correlation"
)

// FrameHeard is published for every frame a relay's radio delivers,
// before any protocol judgement.
type FrameHeard struct {
	Relay       string
	Correlation correlation.ID
	At          time.Time
	Bytes       int
	RSSI        float64
	SNR         float64
	// SignalRSSI is the despread signal's own power; below the noise
	// floor the plain RSSI mostly measures the noise.
	SignalRSSI float64
	// FreqErrHz is the sender's carrier offset — a per-node crystal
	// health signal once averaged over its frames.
	FreqErrHz float64
	// Raw is the frame as it came off the air, whole. Observers
	// republish it; the journal ignores it.
	Raw     []byte
	Airtime time.Duration
}

// FrameJudged is the protocol engine's verdict on a heard frame —
// and the journal's whole record of it: it carries the reception's
// own measurements, so the archive lands in ONE event. FrameHeard
// remains the live feed; a backpressure drop of either event can no
// longer leave a row forever without its verdict, or a verdict
// without its reception.
type FrameJudged struct {
	Relay       string
	Correlation correlation.ID
	Verdict     string
	// The reception the verdict is about, as FrameHeard carried it.
	At         time.Time
	Bytes      int
	RSSI       float64
	SNR        float64
	SignalRSSI float64
	FreqErrHz  float64
	Airtime    time.Duration
	// DuplicateOf links a suppressed frame to the correlation that
	// carried the first copy, so log chains can be followed.
	DuplicateOf string
	// Wire identity, protocol vocabulary; empty on malformed frames.
	Type  string
	Route string
	// Scope names the transport scope a frame arrived in: a scope this
	// relay carries, the raw code of one it does not, or the wildcard
	// for a plain flood. Empty for direct traffic, which carries no
	// scope a relay acts on.
	Scope   string
	PathLen int
	// What the frame says, when its payload speaks an open format:
	// the advertised node name, a public-key prefix, a human detail.
	Node   string
	PubKey string
	Detail string
}

// FrameCorrupt is published for receptions that failed integrity
// checks — RF noise is traffic too, and silence about it would hide a
// site's health. It carries a correlation even though no packet could
// be parsed: the receive and journal logs still describe one causal
// event.
type FrameCorrupt struct {
	Relay       string
	Correlation correlation.ID
	At          time.Time
	Err         string
}

// FrameSent is one emission — real, or shadow-journalled by a relay
// whose gate stops short of keying. Correlation links a relayed frame
// to the reception it answers; originated traffic (adverts) carries
// its own.
type FrameSent struct {
	Relay       string
	Correlation correlation.ID
	At          time.Time
	Airtime     time.Duration
	PowerDBm    int8
	// Kind names what was sent: relay-flood, relay-direct,
	// relay-trace, advert-flood, advert-local.
	Kind string
	// Shadow marks a journalled-never-keyed emission: the audit trail
	// that earns on-air.
	Shadow bool
	// Raw is the frame as it went on the air (or would have, for a
	// shadow), whole.
	Raw []byte
}

// TxDropped is an emission the pipeline gave up on, with its reason:
// queue-full, duty, lbt (when the site chose drop), tx-failed.
type TxDropped struct {
	Relay       string
	Correlation correlation.ID
	At          time.Time
	Reason      string
	// Kind names what was refused — the queue entry's kind, or the
	// would-be answer's for refusals that never composed a packet.
	// Empty when the pipeline gave up before it knew.
	Kind string
}

// NoiseFloor is a relay channel's measured ambient level — what the
// radio hears between frames: the batch median in DBm, the 90th
// percentile's excess over it in SpreadDB (the site's impulsiveness).
// Published on meaningful change and on a slow heartbeat, not on
// every measurement: the live value is always readable from the
// relay, the bus carries the story.
type NoiseFloor struct {
	Relay    string
	At       time.Time
	DBm      float64
	SpreadDB float64
}

// NoiseStarved reports abandoned noise-floor measurement batches: the
// channel was too busy to collect one within its age bound. Aborted
// counts the abandonments since the previous report — a channel this
// starved is a fact about the site worth archiving.
type NoiseStarved struct {
	Relay   string
	At      time.Time
	Aborted uint64
}

// RelayState is published on every relay lifecycle transition. At is
// the transition time as the producer saw it: a consumer may dequeue
// long after — the shutdown drain by design does — and stamping at
// consumption would mis-date the archive.
type RelayState struct {
	Relay string
	At    time.Time
	State string
	Err   string
}

// ObserverState is published on every observer lifecycle transition —
// up, down with its cause, stopped — so a refusal to start is history
// the journal keeps, not a log line that scrolled away.
type ObserverState struct {
	Observer string
	At       time.Time
	State    string
	Cause    string
}
