package meshcorehost

import (
	"testing"

	mesh "meshrunner.dev/pkg/meshcore"
)

func question(route mesh.RouteType) *mesh.Packet {
	return &mesh.Packet{
		Header:  mesh.MakeHeader(route, mesh.PayloadTypeReq, mesh.PayloadVer1),
		Payload: []byte{1, 2, 3, 4, 5, 6, 7, 8},
	}
}

// The reference's chooseReplyScope, for a node that speaks one scope:
// inside it when the question came inside it, plainly for a plain
// flood, and in its own scope for a direct question or a foreign code
// — which is nothing when it speaks unscoped.
func TestAReplyTravelsInTheScopeTheReferenceChooses(t *testing.T) {
	ours, theirs := mesh.TransportKeyForName("lyon"), mesh.TransportKeyForName("paris")
	inOurs := question(mesh.RouteFlood)
	ours.Scope(inOurs)
	inTheirs := question(mesh.RouteFlood)
	theirs.Scope(inTheirs)
	cases := []struct {
		name  string
		in    *mesh.Packet
		speak mesh.TransportKey
		want  mesh.TransportKey
	}{
		{"a question inside our scope", inOurs, ours, ours},
		{"a plain flood", question(mesh.RouteFlood), ours, mesh.TransportKey{}},
		{"a direct question", question(mesh.RouteDirect), ours, ours},
		{"a code we do not carry", inTheirs, ours, ours},
		{"a direct question to an unscoped node", question(mesh.RouteDirect), mesh.TransportKey{}, mesh.TransportKey{}},
		{"a scoped question to an unscoped node", inTheirs, mesh.TransportKey{}, mesh.TransportKey{}},
	}
	for _, c := range cases {
		if got := ReplyScope(c.in, c.speak); got != c.want {
			t.Errorf("%s: scope %x, want %x", c.name, got[:2], c.want[:2])
		}
	}
}

// A fresh flood declares the width its node speaks at and travels
// under its scope, where a reply inherits the asker's width.
func TestAFreshFloodDeclaresItsOwnWidthAndScope(t *testing.T) {
	scope := mesh.TransportKeyForName("lyon")
	pkt := question(mesh.RouteDirect)
	if prio := RouteFloodFresh(pkt, 2, scope); prio != PrioFloodReply {
		t.Fatalf("priority %d", prio)
	}
	if !pkt.IsRouteFlood() || pkt.PathHashSize() != 2 || pkt.PathHashCount() != 0 || !scope.Matches(pkt) {
		t.Fatalf("fresh flood = route %v, hash %d×%d, scoped %t", pkt.Route(), pkt.PathHashSize(), pkt.PathHashCount(),
			scope.Matches(pkt))
	}
	plain := question(mesh.RouteDirect)
	RouteFloodFresh(plain, 1, mesh.TransportKey{})
	if plain.Route() != mesh.RouteFlood || plain.HasTransportCodes() {
		t.Fatalf("unscoped fresh flood = %v", plain.Route())
	}
}
