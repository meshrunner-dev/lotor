package meshcore

// The regions this relay works under: which one it speaks in, whose
// scoped floods it carries, and the administrative model around them —
// the reference's RegionMap, owned by the pipeline's goroutine and
// mutable over the air without a relay bounce. A region is a mesh
// agreement, not a band — see the Vocabulary in DESIGN.md.

import (
	"errors"
	"fmt"
	"strings"

	"meshrunner.dev/pkg/meshcore"
)

// wildcardRegion is what the reference calls the unscoped flood: not a
// region at all, but the root whose deny-flood flag governs plain
// floods, and the name the anonymous answer uses for it.
const wildcardRegion = "*"

// regionTable is the resolved agreement: the map itself, plus the key
// this relay stamps on what it originates — re-derived whenever the
// default designation moves, so origination never derives per packet.
// The map belongs to the pipeline's goroutine; every other reader
// goes through the snapshot order.
type regionTable struct {
	m *meshcore.RegionMap
	// speak is the default region's transport key; zero speaks
	// unscoped, which is what a relay does on a mesh without regions.
	speak meshcore.TransportKey
}

// newRegionTable wraps a map and derives the speaking key.
func newRegionTable(m *meshcore.RegionMap) *regionTable {
	t := &regionTable{m: m}
	t.rederive()
	return t
}

// rederive refreshes the speaking key from the default designation —
// the reference's onDefaultRegionChanged.
func (t *regionTable) rederive() {
	t.speak = meshcore.TransportKey{}
	if d := t.m.Default(); d != nil {
		if ks := t.m.KeysFor(d); len(ks) == 1 {
			t.speak = ks[0]
		}
	}
}

// match names the region a flood arrived in, and whether this relay
// carries it. A plain flood is the wildcard, carried unless its
// deny-flood flag says otherwise; a scoped flood is carried only when
// a flood-allowed region's key recomputes its code — a region the
// table holds but denies reads exactly like one it never heard of,
// as the reference's findMatch mask does.
func (t *regionTable) match(pkt *meshcore.Packet) (name string, key meshcore.TransportKey, carried bool) {
	floods := t.m.Wildcard().Flags&meshcore.RegionDenyFlood == 0
	if !pkt.HasTransportCodes() {
		return wildcardRegion, meshcore.TransportKey{}, floods
	}
	// A scope this relay names is judged by ITS flag: naming a region
	// is how an operator says something about that scope, allow or
	// deny. Everything else — a scope nobody here named — rides the
	// wildcard, like a plain flood: a relay carries the mesh's traffic
	// by default, and a region table is where carriage is RESTRICTED,
	// never where it is granted. The whole table is scanned, not just
	// its flood-allowed half, or a denied scope would fall through to
	// the wildcard and be carried anyway.
	if r := t.m.FindMatch(pkt, 0); r != nil {
		if r.Flags&meshcore.RegionDenyFlood != 0 {
			return r.BareName(), meshcore.TransportKey{}, false
		}
		if ks := t.m.KeysFor(r); len(ks) == 1 {
			return r.BareName(), ks[0], true
		}
	}
	return "", meshcore.TransportKey{}, floods
}

// served lists the regions this relay carries, the reference's own
// shape for the anonymous answer: the wildcard first when plain
// floods are carried, then each flood-allowed region, bare names, in
// insertion order.
func (t *regionTable) served() []string {
	var out []string
	if t.m.Wildcard().Flags&meshcore.RegionDenyFlood == 0 {
		out = append(out, wildcardRegion)
	}
	for _, r := range t.m.Entries() {
		if r.Flags&meshcore.RegionDenyFlood == 0 {
			out = append(out, r.BareName())
		}
	}
	return out
}

// regionOf resolves a frame's region once and remembers the answer.
func (e *engine) regionOf(rx *reception) (name string, carried bool) {
	if !rx.regionKnown {
		rx.region, rx.regionKey, rx.regionCarried = e.regions.match(rx.pkt)
		rx.regionKnown = true
	}
	return rx.region, rx.regionCarried
}

// replyScope picks the transport key an answer travels under — the
// reference's chooseReplyScope, in its order. A question we matched a
// region for is answered inside it. A plain flood is answered
// plainly, never pulled into a region its asker may not hold.
// Anything else — every direct question, since a direct packet
// carries no code we could have matched, and a scoped one whose
// region we do not carry — is answered in the region this relay
// speaks, which is nothing when it speaks unscoped.
func (e *engine) replyScope(rx *reception) meshcore.TransportKey {
	if _, carried := e.regionOf(rx); carried && !rx.regionKey.IsZero() {
		return rx.regionKey
	}
	if rx.pkt.IsRouteFlood() && !rx.pkt.HasTransportCodes() {
		return meshcore.TransportKey{}
	}
	return e.regions.speak
}

// regionName is what the journal records about a frame's region: the
// name when we carry it, the raw code when we do not, and the
// wildcard for a plain flood. Direct traffic gets nothing — a relay
// acts on no region there, and matching one would cost an HMAC on a
// path the reference never pays it.
func (e *engine) regionName(rx *reception) string {
	switch {
	case rx.pkt.IsRouteFlood():
		name, _ := e.regionOf(rx)
		if name != "" {
			return name
		}
		return fmt.Sprintf("%#04x", rx.pkt.TransportCodes[0])
	case rx.pkt.HasTransportCodes():
		return fmt.Sprintf("%#04x", rx.pkt.TransportCodes[0])
	default:
		return ""
	}
}

// checkRegionName refuses the names that cannot mean what they look
// like they mean — the same refusals the old configuration door made,
// now guarding the command door.
func checkRegionName(name string) error {
	switch {
	case name == "":
		return errors.New("a region name cannot be empty")
	case name == wildcardRegion:
		return errors.New("the wildcard is not a region to create")
	case strings.HasPrefix(name, "$"):
		// The reference reserves '$' for regions whose key comes from
		// a hardware keystore instead of the name, and ships that
		// keystore unimplemented. Accepting the syntax would promise
		// a privacy this cannot deliver.
		return errors.New("private '$' regions are not supported")
	}
	return nil
}
