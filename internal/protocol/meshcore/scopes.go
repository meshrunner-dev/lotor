package meshcore

// The transport agreement this relay works under: which scope it
// speaks in, and whose scoped floods it carries. A scope is a mesh
// agreement, not a band — see the Vocabulary in DESIGN.md, and beware
// that the reference's documentation calls this a region.

import (
	"errors"
	"fmt"
	"strings"

	"meshrunner.dev/pkg/meshcore"
)

// maxScopeNameLen bounds a scope name at what the reference stores it
// in — a 31-byte field, so 30 usable characters. The name never
// crosses the air except in the anonymous scopes answer, but a name
// this node cannot hold is a name it cannot agree on.
const maxScopeNameLen = 30

// wildcardScope is what the reference calls the unscoped flood: not a
// scope at all, but the thing acceptUnscoped governs, and the name the
// anonymous answer uses for it.
const wildcardScope = "*"

// scopeTable is the resolved agreement: keys derived once at assembly
// rather than per packet, because deriving one is a SHA-256 and
// matching against one is an HMAC over the whole payload.
type scopeTable struct {
	// speakName and speak are the scope this relay stamps on what it
	// originates. A zero key means it speaks unscoped, which is what a
	// relay does on a mesh that has no scopes.
	speakName string
	speak     meshcore.TransportKey

	// acceptNames and accept run in step: the scopes whose floods this
	// relay carries. Matching walks them in order.
	acceptNames []string
	accept      []meshcore.TransportKey

	// unscoped says whether a plain flood is relayed at all — the
	// reference's wildcard region and its deny-flood bit.
	unscoped bool
}

// newScopeTable resolves the configured names into keys.
func newScopeTable(p params) *scopeTable {
	t := &scopeTable{unscoped: p.acceptUnscoped(), speakName: canonicalScope(p.DefaultScope)}
	if t.speakName != "" {
		t.speak = meshcore.TransportKeyForName(t.speakName)
	}
	for _, name := range p.AcceptScopes {
		n := canonicalScope(name)
		t.acceptNames = append(t.acceptNames, n)
		t.accept = append(t.accept, meshcore.TransportKeyForName(n))
	}
	return t
}

// match names the scope a flood arrived in, and whether this relay
// carries it. A plain flood is the wildcard, carried or not as the
// operator said; a scoped flood is carried only when one of our keys
// recomputes its code.
func (t *scopeTable) match(pkt *meshcore.Packet) (name string, carried bool) {
	if !pkt.HasTransportCodes() {
		return wildcardScope, t.unscoped
	}
	if i := meshcore.MatchTransportRegion(pkt, t.accept); i >= 0 {
		return t.acceptNames[i], true
	}
	return "", false
}

// served lists the scopes this relay carries, the reference's own
// shape for the anonymous answer: the wildcard first when plain floods
// are carried, then each accepted scope, all with the '#' stripped.
func (t *scopeTable) served() []string {
	out := make([]string, 0, len(t.acceptNames)+1)
	if t.unscoped {
		out = append(out, wildcardScope)
	}
	for _, n := range t.acceptNames {
		out = append(out, strings.TrimPrefix(n, "#"))
	}
	return out
}

// canonicalScope is the name in the form the key derivation uses.
func canonicalScope(name string) string {
	if name == "" {
		return ""
	}
	return "#" + strings.TrimPrefix(name, "#")
}

// normalizeScopes validates the agreement and fills its defaults. The
// contradictions it refuses are the ones an operator would otherwise
// discover as silence on the air: speaking in a scope nobody here
// carries, or naming the wildcard as though it were one.
func normalizeScopes(p *params) error {
	seen := map[string]bool{}
	for _, name := range p.AcceptScopes {
		if err := checkScopeName(name); err != nil {
			return err
		}
		c := canonicalScope(name)
		if seen[c] {
			return fmt.Errorf("meshcore params: accept_scopes names %q twice", name)
		}
		seen[c] = true
	}
	if p.DefaultScope == "" {
		return nil // speaks unscoped, which needs no agreement
	}
	if err := checkScopeName(p.DefaultScope); err != nil {
		return err
	}
	if !seen[canonicalScope(p.DefaultScope)] {
		return fmt.Errorf(
			"meshcore params: default_scope %q is not in accept_scopes — "+
				"a relay that speaks in a scope carries it too", p.DefaultScope)
	}
	return nil
}

// checkScopeName refuses the names that cannot mean what they look
// like they mean.
func checkScopeName(name string) error {
	switch {
	case name == "":
		return errors.New("meshcore params: a scope name cannot be empty")
	case name == wildcardScope:
		return errors.New(
			"meshcore params: the wildcard is not a scope — accept_unscoped decides " +
				"whether plain floods are relayed")
	case strings.HasPrefix(name, "$"):
		// The reference reserves '$' for scopes whose key comes from a
		// hardware keystore instead of the name, and ships that
		// keystore unimplemented. Accepting the syntax would promise
		// a privacy this cannot deliver.
		return fmt.Errorf("meshcore params: scope %q — private '$' scopes are not supported", name)
	case len(strings.TrimPrefix(name, "#")) > maxScopeNameLen:
		return fmt.Errorf("meshcore params: scope %q is longer than the %d characters the mesh holds",
			name, maxScopeNameLen)
	}
	return nil
}
