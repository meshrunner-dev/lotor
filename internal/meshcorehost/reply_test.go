package meshcorehost

import (
	"crypto/rand"
	"testing"

	"meshrunner.dev/pkg/meshcore"
)

func TestASuppliedPathOutranksTheStoredOne(t *testing.T) {
	// A route carried by this very question is fresher than one
	// remembered from an earlier exchange.
	stored := &OutPath{PathLen: 2, Path: []byte{0xAA, 0xBB}}
	home := Answer{Supplied: true, PathLen: 1, Path: []byte{0x11}, Out: stored}.routeHome()
	if home == nil || home.PathLen != 1 || home.Path[0] != 0x11 {
		t.Fatalf("route home = %+v, want the one the question carried", home)
	}
	if home := (Answer{Out: stored}).routeHome(); home == nil || home.PathLen != 2 {
		t.Fatalf("stored route ignored: %+v", home)
	}
	if home := (Answer{}).routeHome(); home != nil {
		t.Fatalf("invented a route out of nothing: %+v", home)
	}
}

// A scope travels with what floods — the PATH return to a flooded
// question, the flood for want of a route — and never with a direct
// send, as the reference's sendDirect never carries one.
func TestAScopeRidesTheFloodsAndNeverTheDirects(t *testing.T) {
	client, _ := meshcore.NewLocalIdentity(rand.Reader)
	server, _ := meshcore.NewLocalIdentity(rand.Reader)
	secret, _ := server.SharedSecret(client.PubKey[:])
	src := server.PubKey[:meshcore.PathHashSize]
	scope := meshcore.TransportKeyForName("lyon")
	scoped := Answer{DestHash: client.PubKey[:meshcore.PathHashSize], Secret: secret, Tag: 7, Body: []byte{1}, Scope: scope}

	flooded := &meshcore.Packet{
		Header: meshcore.MakeHeader(meshcore.RouteFlood, meshcore.PayloadTypeReq, meshcore.PayloadVer1),
		Path:   []byte{0x11}, PathLen: 1,
	}
	if pkt, _, _, err := ComposeReply(flooded, scoped, src); err != nil || !scope.Matches(pkt) {
		t.Fatalf("path return: scoped %t, %v", err == nil && scope.Matches(pkt), err)
	}
	direct := &meshcore.Packet{
		Header: meshcore.MakeHeader(meshcore.RouteDirect, meshcore.PayloadTypeReq, meshcore.PayloadVer1),
	}
	if pkt, _, _, err := ComposeReply(direct, scoped, src); err != nil || !scope.Matches(pkt) {
		t.Fatalf("flood for want of a route: scoped %t, %v", err == nil && scope.Matches(pkt), err)
	}
	taught := scoped
	taught.Out = &OutPath{PathLen: 1, Path: []byte{0xAA}}
	pkt, _, _, err := ComposeReply(direct, taught, src)
	if err != nil || pkt.Route() != meshcore.RouteDirect || pkt.HasTransportCodes() {
		t.Fatalf("direct answer = %v, codes %t, %v", pkt.Route(), pkt.HasTransportCodes(), err)
	}

	// The same rule for an already-composed packet, and a packet that
	// was once scoped is stripped when it goes direct.
	ack, _ := meshcore.BuildAck([]byte{1, 2, 3, 4})
	scope.Scope(ack)
	if prio, source := RouteHome(ack, direct, taught.Out, scope); prio != PrioDirect || source != "learned" ||
		ack.Route() != meshcore.RouteDirect || ack.HasTransportCodes() {
		t.Fatalf("routed home direct = %v, codes %t (%d, %s)", ack.Route(), ack.HasTransportCodes(), prio, source)
	}
	if prio, source := RouteHome(ack, flooded, nil, scope); prio != PrioFloodReply || source != "flood" || !scope.Matches(ack) {
		t.Fatalf("routed home flooded = %v, scoped %t (%d, %s)", ack.Route(), scope.Matches(ack), prio, source)
	}
}

// The four routes, in the reference's order: a flooded question earns
// a path return; a supplied path, then a taught one, a direct answer;
// nothing at all, a flood.
func TestComposeReplyChoosesTheReferenceRoute(t *testing.T) {
	client, _ := meshcore.NewLocalIdentity(rand.Reader)
	server, _ := meshcore.NewLocalIdentity(rand.Reader)
	secret, _ := server.SharedSecret(client.PubKey[:])
	src := server.PubKey[:meshcore.PathHashSize]
	base := Answer{DestHash: client.PubKey[:meshcore.PathHashSize], Secret: secret, Tag: 42, Body: []byte{1}}

	flooded := &meshcore.Packet{
		Header: meshcore.MakeHeader(meshcore.RouteFlood, meshcore.PayloadTypeReq, meshcore.PayloadVer1),
		Path:   []byte{0x11, 0x22}, PathLen: 2,
	}
	pkt, prio, source, err := ComposeReply(flooded, base, src)
	if err != nil || pkt.PayloadType() != meshcore.PayloadTypePath || prio != PrioPathReturn || source != "path-return" {
		t.Fatalf("flooded question: %v %d %s %v", pkt.PayloadType(), prio, source, err)
	}

	direct := &meshcore.Packet{
		Header: meshcore.MakeHeader(meshcore.RouteDirect, meshcore.PayloadTypeReq, meshcore.PayloadVer1),
	}
	supplied := base
	supplied.Supplied, supplied.PathLen, supplied.Path = true, 1, []byte{0x33}
	pkt, prio, source, err = ComposeReply(direct, supplied, src)
	if err != nil || !pkt.IsRouteDirect() || prio != PrioDirect || source != "supplied" || pkt.Path[0] != 0x33 {
		t.Fatalf("supplied path: route %v %d %s path % x %v", pkt.Route(), prio, source, pkt.Path, err)
	}

	taught := base
	taught.Out = &OutPath{PathLen: 2, Path: []byte{0xAA, 0xBB}}
	pkt, prio, source, err = ComposeReply(direct, taught, src)
	if err != nil || !pkt.IsRouteDirect() || prio != PrioDirect || source != "learned" || pkt.PathLen != 2 {
		t.Fatalf("taught route: route %v %d %s %v", pkt.Route(), prio, source, err)
	}

	pkt, prio, source, err = ComposeReply(direct, base, src)
	if err != nil || !pkt.IsRouteFlood() || prio != PrioFloodReply || source != "flood" {
		t.Fatalf("no route: %v %d %s %v", pkt.Route(), prio, source, err)
	}
	// The answer opens under the client's secret, tag first.
	d, _ := meshcore.ParseDatagram(pkt.Payload)
	plain, err := d.Open(secret)
	if err != nil {
		t.Fatal(err)
	}
	if tag, body, _ := meshcore.UnframeAdmin(plain); tag != 42 || body[0] != 1 {
		t.Errorf("answer = tag %d body % x", tag, body)
	}
}
