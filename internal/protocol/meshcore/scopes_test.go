package meshcore

import (
	"testing"
	"time"

	"meshrunner.dev/pkg/meshcore"
)

func TestScopeConfigRefusesWhatCannotWork(t *testing.T) {
	for _, c := range []struct {
		what    string
		p       params
		refused bool
	}{
		{"unscoped is the default posture", params{}, false},
		{"a scope carried and spoken", params{
			DefaultScope: "fr", AcceptScopes: []string{"fr"}}, false},
		{"the hash prefix is optional either side", params{
			DefaultScope: "#fr", AcceptScopes: []string{"fr", "be"}}, false},
		{"carrying without speaking", params{AcceptScopes: []string{"fr"}}, false},
		{"speaking what nobody here carries", params{
			DefaultScope: "fr", AcceptScopes: []string{"be"}}, true},
		{"speaking with nothing carried", params{DefaultScope: "fr"}, true},
		{"the same scope twice", params{AcceptScopes: []string{"fr", "#fr"}}, true},
		{"the wildcard as a scope", params{AcceptScopes: []string{"*"}}, true},
		{"a private scope", params{AcceptScopes: []string{"$secret"}}, true},
		{"an empty name", params{AcceptScopes: []string{""}}, true},
		{"a name longer than the mesh holds", params{
			AcceptScopes: []string{"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}}, true},
	} {
		err := normalizeScopes(&c.p)
		if c.refused && err == nil {
			t.Errorf("%s: accepted", c.what)
		}
		if !c.refused && err != nil {
			t.Errorf("%s: refused with %v", c.what, err)
		}
	}
}

func TestScopeTableMatchesWhatItCarries(t *testing.T) {
	on := true
	off := false
	table := newScopeTable(params{
		DefaultScope: "fr", AcceptScopes: []string{"fr", "be"}, AcceptUnscoped: &on,
	})

	// A plain flood is the wildcard, carried because the operator says so.
	plain := &meshcore.Packet{
		Header:  meshcore.MakeHeader(meshcore.RouteFlood, meshcore.PayloadTypeTxtMsg, meshcore.PayloadVer1),
		Payload: []byte("hello"),
	}
	if name, carried := table.match(plain); name != wildcardScope || !carried {
		t.Fatalf("plain flood = %q/%v, want the wildcard carried", name, carried)
	}
	shut := newScopeTable(params{AcceptScopes: []string{"fr"}, AcceptUnscoped: &off})
	if _, carried := shut.match(plain); carried {
		t.Fatal("plain flood carried with accept_unscoped false")
	}

	// A flood in a scope we carry names itself.
	scoped := &meshcore.Packet{
		Header:  meshcore.MakeHeader(meshcore.RouteFlood, meshcore.PayloadTypeTxtMsg, meshcore.PayloadVer1),
		Payload: []byte("hello"),
	}
	meshcore.TransportKeyForName("be").Scope(scoped)
	if !scoped.HasTransportCodes() {
		t.Fatal("scoping left the packet unscoped")
	}
	if name, carried := table.match(scoped); name != "#be" || !carried {
		t.Fatalf("scoped flood = %q/%v, want #be carried", name, carried)
	}

	// A flood in a scope we do not carry is somebody else's business.
	foreign := &meshcore.Packet{
		Header:  meshcore.MakeHeader(meshcore.RouteFlood, meshcore.PayloadTypeTxtMsg, meshcore.PayloadVer1),
		Payload: []byte("hello"),
	}
	meshcore.TransportKeyForName("de").Scope(foreign)
	if name, carried := table.match(foreign); carried {
		t.Fatalf("a foreign scope was carried as %q", name)
	}
}

func TestServedNamesFollowTheReferenceShape(t *testing.T) {
	on, off := true, false
	full := newScopeTable(params{AcceptScopes: []string{"#fr", "be"}, AcceptUnscoped: &on})
	got := full.served()
	if len(got) != 3 || got[0] != "*" || got[1] != "fr" || got[2] != "be" {
		t.Fatalf("served = %v, want the wildcard first and the hashes stripped", got)
	}
	quiet := newScopeTable(params{AcceptScopes: []string{"fr"}, AcceptUnscoped: &off})
	if got := quiet.served(); len(got) != 1 || got[0] != "fr" {
		t.Fatalf("served = %v, want no wildcard", got)
	}
}

func TestACarriedScopeIsRelayed(t *testing.T) {
	// The gate stopped being a blanket refusal: a flood in a scope
	// this relay carries moves on, and one in a scope it does not
	// still stops here.
	e, dev, sub, peer := txRig(t, "on-air")
	e.scopes = newScopeTable(params{
		DefaultScope: "fr", AcceptScopes: []string{"fr"},
	})
	runEngine(t, e, dev)

	frame := peerAdvert(t, peer, time.Now())
	pkt, err := meshcore.ParsePacket(frame.Payload)
	if err != nil {
		t.Fatal(err)
	}
	meshcore.TransportKeyForName("fr").Scope(pkt)
	raw, err := pkt.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	frame.Payload = raw
	dev.frames <- frame

	sent := awaitSent(t, sub)
	if sent.Kind != "relay-flood" {
		t.Fatalf("sent = %+v, want the carried scope relayed", sent)
	}
	out, err := meshcore.ParsePacket(<-dev.sent)
	if err != nil {
		t.Fatal(err)
	}
	// The scope survives the hop untouched: a relay copies, it never
	// recomputes.
	if out.Route() != meshcore.RouteTransportFlood ||
		out.TransportCodes != pkt.TransportCodes {
		t.Fatalf("relayed as %v with codes %v, want the scope preserved",
			out.Route(), out.TransportCodes)
	}
}

func TestUnscopedHopLimitBitesPlainFloodsAlone(t *testing.T) {
	e, _, _, peer := txRig(t, "shadow")
	e.scopes = newScopeTable(params{AcceptScopes: []string{"fr"}})
	e.p.FloodMaxUnscopedHops = 3

	build := func(scoped bool, hops int) *reception {
		pkt, err := meshcore.BuildAdvert(peer, time.Now(), &meshcore.AdvertData{Name: "p"})
		if err != nil {
			t.Fatal(err)
		}
		pkt.Path = make([]byte, hops)
		for i := range pkt.Path {
			pkt.Path[i] = ^e.id.PubKey[0]
		}
		pkt.SetPathHashSizeAndCount(1, hops)
		if scoped {
			meshcore.TransportKeyForName("fr").Scope(pkt)
		}
		return rxOf(e, pkt)
	}
	if v, why := e.floodVerdict(build(false, 3), true); v != verdictDropFloodHops {
		t.Errorf("plain flood at 3 hops = %q (%s), want the unscoped limit to stop it", v, why)
	}
	if v, _ := e.floodVerdict(build(true, 3), true); v != verdictRelayFlood {
		t.Errorf("scoped flood at 3 hops = %q, want the unscoped limit not to touch it", v)
	}
}
