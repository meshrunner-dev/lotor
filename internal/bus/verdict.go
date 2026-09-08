package bus

import "fmt"

// Verdict is the protocol judgement carried by FrameJudged. Its numeric
// representation stays inside the daemon; String and text encoding preserve
// the journal, console and JSON vocabulary. The protocol owns the decisions
// behind these outcomes, including what a dry relay would do on the air.
type Verdict uint8

// Frame judgement vocabulary. None is the absence of a judgement, not a
// decision to ignore a frame. The textual spellings are a public contract;
// numeric values are not a persisted format.
const (
	VerdictNone Verdict = iota
	VerdictRelayFlood
	VerdictRelayDirect    // our hash heads the path: the reference relays and consumes it
	VerdictRelayTrace     // our hash is the trace's next target hop
	VerdictDropFloodType  // the reference never re-floods this payload type
	VerdictDropFloodShort // the payload does not hold the envelope its type declares
	VerdictDropBadAdvert  // flood advert whose signature fails
	VerdictDropPathFull   // appending our hash would exceed the path
	VerdictDropFloodHops  // the flood travelled past its hop limit
	VerdictDropLoop       // our hash already rides the path: we relayed this already
	VerdictDropScoped     // transport-scoped flood into a scope this relay denies
	VerdictSelfAdvert     // our own advert echoing back
	VerdictSameRadio      // a binding sharing this antenna: relaying reaches nobody new
	VerdictCommand        // a logged-in admin's command line
	VerdictZeroHop        // direct, empty path: addressed to whoever hears it
	VerdictNotAddressed   // the path's next hop is not us (or no identity exists)
	VerdictDiscover       // a zero-hop neighbourhood scan asking who hears it
	VerdictAnon           // a question sealed to our key, asker named in the clear
	VerdictDiscoverAnswer // a neighbour answering a scan this node sent
	VerdictScopeAnswer    // a neighbour telling us what it carries, for our own question
	VerdictRequest        // a question from a client whose session we hold
	VerdictClientPath     // a client teaching us how to reach it directly
	VerdictTraceTransit   // trace walking its target path, next hop unjudgeable
	VerdictTraceNotUs     // trace walking its target path, next hop is not us
	VerdictTraceArrived   // trace consumed its whole target path
	VerdictBadVersion     // the reference dispatcher rejects it at parse
	VerdictIgnored
	VerdictMalformed
	VerdictDuplicate
	verdictCount
)

// verdictNames is the fixed text contract, indexed by the typed constants.
// It is never populated or changed by a producer or a consumer.
var verdictNames = [verdictCount]string{
	VerdictNone:           "",
	VerdictRelayFlood:     "would-relay-flood",
	VerdictRelayDirect:    "would-relay-direct",
	VerdictRelayTrace:     "would-relay-trace",
	VerdictDropFloodType:  "would-drop-flood-type",
	VerdictDropFloodShort: "would-drop-flood-truncated",
	VerdictDropBadAdvert:  "would-drop-invalid-advert",
	VerdictDropPathFull:   "would-drop-flood-path-full",
	VerdictDropFloodHops:  "would-drop-flood-hops",
	VerdictDropLoop:       "would-drop-flood-loop",
	VerdictDropScoped:     "would-drop-flood-scoped",
	VerdictSelfAdvert:     "self-advert",
	VerdictSameRadio:      "heard-on-our-own-radio",
	VerdictCommand:        "administration",
	VerdictZeroHop:        "heard-zero-hop",
	VerdictNotAddressed:   "direct-not-addressed",
	VerdictDiscover:       "discover-request",
	VerdictAnon:           "anon-request",
	VerdictDiscoverAnswer: "discover-answer",
	VerdictScopeAnswer:    "scopes-answer",
	VerdictRequest:        "authenticated-request",
	VerdictClientPath:     "client-route-home",
	VerdictTraceTransit:   "trace-transit",
	VerdictTraceNotUs:     "trace-not-addressed",
	VerdictTraceArrived:   "trace-arrived",
	VerdictBadVersion:     "unsupported-version",
	VerdictIgnored:        "ignored",
	VerdictMalformed:      "malformed",
	VerdictDuplicate:      "duplicate",
}

// String names a judgement, or "unknown" for an invalid value.
func (v Verdict) String() string {
	if v >= verdictCount {
		return "unknown"
	}
	return verdictNames[v]
}

// MarshalText keeps JSON and other text encoders from exposing enum numbers.
// Invalid internal values are errors rather than new public vocabulary.
func (v Verdict) MarshalText() ([]byte, error) {
	if v >= verdictCount {
		return nil, fmt.Errorf("invalid frame verdict %d", v)
	}
	return []byte(v.String()), nil
}

// UnmarshalText accepts the published vocabulary and leaves the receiver
// unchanged if the text does not name a judgement.
func (v *Verdict) UnmarshalText(text []byte) error {
	for candidate := range verdictCount {
		if candidate.String() == string(text) {
			*v = candidate
			return nil
		}
	}
	return fmt.Errorf("unknown frame verdict %q", text)
}
