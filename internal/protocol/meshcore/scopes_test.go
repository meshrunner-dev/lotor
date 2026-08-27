package meshcore

import (
	"fmt"
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
	if name, _, carried := table.match(plain); name != wildcardScope || !carried {
		t.Fatalf("plain flood = %q/%v, want the wildcard carried", name, carried)
	}
	shut := newScopeTable(params{AcceptScopes: []string{"fr"}, AcceptUnscoped: &off})
	if _, _, carried := shut.match(plain); carried {
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
	if name, _, carried := table.match(scoped); name != "#be" || !carried {
		t.Fatalf("scoped flood = %q/%v, want #be carried", name, carried)
	}

	// A flood in a scope we do not carry is somebody else's business.
	foreign := &meshcore.Packet{
		Header:  meshcore.MakeHeader(meshcore.RouteFlood, meshcore.PayloadTypeTxtMsg, meshcore.PayloadVer1),
		Payload: []byte("hello"),
	}
	meshcore.TransportKeyForName("de").Scope(foreign)
	if name, _, carried := table.match(foreign); carried {
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

func TestOriginatedTrafficSpeaksInItsScope(t *testing.T) {
	// A routable announcement travels in the scope this relay speaks;
	// the zero-hop one stays plain, as the reference's does.
	e, dev, _, _ := txRig(t, "shadow")
	e.scopes = newScopeTable(params{DefaultScope: "fr", AcceptScopes: []string{"fr"}})
	e.queue.depth = 8

	e.advert(dev, time.Now(), "advert-flood", false)
	e.advert(dev, time.Now(), "advert-local", true)
	if len(e.queue.entries) != 2 {
		t.Fatalf("%d adverts queued", len(e.queue.entries))
	}
	flood, local := e.queue.entries[0].pkt, e.queue.entries[1].pkt
	if flood.Route() != meshcore.RouteTransportFlood {
		t.Errorf("flood advert route = %v, want the scope stamped", flood.Route())
	}
	if flood.TransportCodes[0] != meshcore.TransportKeyForName("fr").Code(flood) {
		t.Error("the flood advert's code is not this relay's scope")
	}
	if local.Route() != meshcore.RouteDirect || local.HasTransportCodes() {
		t.Errorf("zero-hop advert route = %v, want it plain", local.Route())
	}
}

func TestReplyScopeFollowsTheReference(t *testing.T) {
	e, _, _, peer := txRig(t, "shadow")
	e.scopes = newScopeTable(params{DefaultScope: "fr", AcceptScopes: []string{"fr"}})
	speak := meshcore.TransportKeyForName("fr")

	ask := func(route meshcore.RouteType, scoped bool) *reception {
		pkt, err := meshcore.BuildAdvert(peer, time.Now(), &meshcore.AdvertData{Name: "p"})
		if err != nil {
			t.Fatal(err)
		}
		pkt.Header = meshcore.MakeHeader(route, meshcore.PayloadTypeAdvert, meshcore.PayloadVer1)
		if scoped {
			speak.Scope(pkt)
		}
		return rxOf(e, pkt)
	}
	// A question we matched a scope for is answered inside it.
	if got := e.replyScope(ask(meshcore.RouteFlood, true)); got != speak {
		t.Error("a scoped question was not answered in its own scope")
	}
	// A plain flood is answered plainly — never pulled into a scope
	// its asker may not hold.
	if got := e.replyScope(ask(meshcore.RouteFlood, false)); !got.IsZero() {
		t.Error("a plain flood was answered inside a scope")
	}
	// A direct question carries no scope to match, so it gets ours —
	// the reference's default, not openHop's plain reply.
	if got := e.replyScope(ask(meshcore.RouteDirect, false)); got != speak {
		t.Error("a direct question was not answered in the default scope")
	}
}

func TestScopeNameRecordsWhatItKnows(t *testing.T) {
	// The journal names a scope we carry, admits the raw code of one
	// we do not, calls a plain flood the wildcard, and says nothing
	// about direct traffic, where a relay acts on no scope at all.
	e, _, _, peer := txRig(t, "shadow")
	e.scopes = newScopeTable(params{AcceptScopes: []string{"fr"}})

	build := func(route meshcore.RouteType, key string) *reception {
		pkt, err := meshcore.BuildAdvert(peer, time.Now(), &meshcore.AdvertData{Name: "p"})
		if err != nil {
			t.Fatal(err)
		}
		pkt.Header = meshcore.MakeHeader(route, meshcore.PayloadTypeAdvert, meshcore.PayloadVer1)
		if key != "" {
			meshcore.TransportKeyForName(key).Scope(pkt)
		}
		return rxOf(e, pkt)
	}
	if got := e.scopeName(build(meshcore.RouteFlood, "fr")); got != "#fr" {
		t.Errorf("a carried scope = %q, want its name", got)
	}
	foreign := build(meshcore.RouteFlood, "de")
	want := fmt.Sprintf("%#04x", foreign.pkt.TransportCodes[0])
	if got := e.scopeName(foreign); got != want {
		t.Errorf("an uncarried scope = %q, want the raw code %q", got, want)
	}
	if got := e.scopeName(build(meshcore.RouteFlood, "")); got != wildcardScope {
		t.Errorf("a plain flood = %q, want the wildcard", got)
	}
	if got := e.scopeName(build(meshcore.RouteDirect, "")); got != "" {
		t.Errorf("direct traffic = %q, want nothing", got)
	}
}
