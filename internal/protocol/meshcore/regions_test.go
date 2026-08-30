package meshcore

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"go.uber.org/zap"

	"meshrunner.dev/pkg/meshcore"

	"meshrunner.dev/lotor/internal/bus"
	"meshrunner.dev/lotor/internal/radio"
)

// testRegions builds a table the way the old scope attributes did:
// accepted names flood-allowed and flat, a default designation, the
// wildcard open or shut.
func testRegions(t *testing.T, defaultName string, accepts []string, unscoped bool) *regionTable {
	t.Helper()
	m := meshcore.NewRegionMap()
	for _, n := range accepts {
		r, err := m.Put(n, 0)
		if err != nil {
			t.Fatal(err)
		}
		r.Flags = 0
	}
	if defaultName != "" {
		d := m.FindByName(defaultName)
		if d == nil {
			t.Fatalf("default %q is not among the accepts", defaultName)
		}
		m.SetDefault(d)
	}
	if !unscoped {
		m.Wildcard().Flags = meshcore.RegionDenyFlood
	}
	return newRegionTable(m)
}

func TestMovedScopeAttrsPointAtTheRegionTable(t *testing.T) {
	// The three old attributes are gone from the config door; a store
	// or file still carrying one gets the pointer, not a bare
	// unknown-key refusal.
	for _, gone := range []string{"default_scope", "accept_scopes", "accept_unscoped"} {
		_, err := paramsFrom(map[string]any{"frequency_hz": 869_618_000, gone: "x"})
		if err == nil || !strings.Contains(err.Error(), "region table") {
			t.Errorf("%s: %v — want the region-table pointer", gone, err)
		}
	}
}

func TestRegionTableMatchesWhatItCarries(t *testing.T) {
	table := testRegions(t, "fr", []string{"fr", "be"}, true)

	// A plain flood is the wildcard, carried because the operator says so.
	plain := &meshcore.Packet{
		Header:  meshcore.MakeHeader(meshcore.RouteFlood, meshcore.PayloadTypeTxtMsg, meshcore.PayloadVer1),
		Payload: []byte("hello"),
	}
	if name, _, carried := table.match(plain); name != wildcardRegion || !carried {
		t.Fatalf("plain flood = %q/%v, want the wildcard carried", name, carried)
	}
	shut := testRegions(t, "", []string{"fr"}, false)
	if _, _, carried := shut.match(plain); carried {
		t.Fatal("plain flood carried through a denied wildcard")
	}

	// A flood in a region we carry names itself.
	scoped := &meshcore.Packet{
		Header:  meshcore.MakeHeader(meshcore.RouteFlood, meshcore.PayloadTypeTxtMsg, meshcore.PayloadVer1),
		Payload: []byte("hello"),
	}
	meshcore.TransportKeyForName("be").Scope(scoped)
	if !scoped.HasTransportCodes() {
		t.Fatal("scoping left the packet unscoped")
	}
	if name, _, carried := table.match(scoped); name != "be" || !carried {
		t.Fatalf("scoped flood = %q/%v, want be carried", name, carried)
	}

	// A scope nobody here named rides the wildcard, like a plain
	// flood: a relay carries the mesh's traffic, and the table is
	// where carriage is RESTRICTED, never where it is granted. It
	// carries no name of ours and no key to answer under.
	foreign := &meshcore.Packet{
		Header:  meshcore.MakeHeader(meshcore.RouteFlood, meshcore.PayloadTypeTxtMsg, meshcore.PayloadVer1),
		Payload: []byte("hello"),
	}
	meshcore.TransportKeyForName("de").Scope(foreign)
	name, key, carried := table.match(foreign)
	if !carried {
		t.Fatal("an unnamed scope was refused — the wildcard carries floods")
	}
	if name != "" || !key.IsZero() {
		t.Errorf("an unnamed scope claimed name %q / key %v", name, key)
	}
	// Shutting the wildcard shuts them with it: one switch for every
	// flood this relay was never told anything about.
	table.m.Wildcard().Flags = meshcore.RegionDenyFlood
	if _, _, carried := table.match(foreign); carried {
		t.Error("a shut wildcard still carried an unnamed scope")
	}
	table.m.Wildcard().Flags = 0

	// Naming a region is how an operator speaks about that scope, and
	// denying it is heard even though the wildcard is open.
	denied := table.m.FindByName("be")
	denied.Flags = meshcore.RegionDenyFlood
	if _, _, carried := table.match(scoped); carried {
		t.Fatal("a denied region still carried its flood")
	}
}

func TestServedNamesFollowTheReferenceShape(t *testing.T) {
	full := testRegions(t, "", []string{"#fr", "be"}, true)
	got := full.served()
	if len(got) != 3 || got[0] != "*" || got[1] != "fr" || got[2] != "be" {
		t.Fatalf("served = %v, want the wildcard first and the hashes stripped", got)
	}
	quiet := testRegions(t, "", []string{"fr"}, false)
	if got := quiet.served(); len(got) != 1 || got[0] != "fr" {
		t.Fatalf("served = %v, want no wildcard", got)
	}
}

func TestACarriedRegionIsRelayed(t *testing.T) {
	// The gate stopped being a blanket refusal: a flood in a region
	// this relay carries moves on, and one in a region it does not
	// still stops here.
	e, dev, sub, peer := txRig(t, "on-air")
	e.regions = testRegions(t, "fr", []string{"fr"}, true)
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
		t.Fatalf("sent = %+v, want the carried region relayed", sent)
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
	e.regions = testRegions(t, "", []string{"fr"}, true)
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

func TestOriginatedTrafficSpeaksInItsRegion(t *testing.T) {
	// A routable announcement travels in the region this relay speaks;
	// the zero-hop one stays plain, as the reference's does.
	e, dev, _, _ := txRig(t, "shadow")
	e.regions = testRegions(t, "fr", []string{"fr"}, true)
	e.queue.depth = 8

	e.advert(dev, time.Now(), "advert-flood", false)
	e.advert(dev, time.Now(), "advert-local", true)
	if len(e.queue.entries) != 2 {
		t.Fatalf("%d adverts queued", len(e.queue.entries))
	}
	flood, local := e.queue.entries[0].pkt, e.queue.entries[1].pkt
	if flood.Route() != meshcore.RouteTransportFlood {
		t.Errorf("flood advert route = %v, want the region stamped", flood.Route())
	}
	if flood.TransportCodes[0] != meshcore.TransportKeyForName("fr").Code(flood) {
		t.Error("the flood advert's code is not this relay's region")
	}
	if local.Route() != meshcore.RouteDirect || local.HasTransportCodes() {
		t.Errorf("zero-hop advert route = %v, want it plain", local.Route())
	}
}

func TestReplyScopeFollowsTheReference(t *testing.T) {
	e, _, _, peer := txRig(t, "shadow")
	e.regions = testRegions(t, "fr", []string{"fr"}, true)
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
	// A question we matched a region for is answered inside it.
	if got := e.replyScope(ask(meshcore.RouteFlood, true)); got != speak {
		t.Error("a scoped question was not answered in its own region")
	}
	// A plain flood is answered plainly — never pulled into a region
	// its asker may not hold.
	if got := e.replyScope(ask(meshcore.RouteFlood, false)); !got.IsZero() {
		t.Error("a plain flood was answered inside a region")
	}
	// A direct question carries no code to match, so it gets ours —
	// the reference's default: the answer is scoped, never plain.
	if got := e.replyScope(ask(meshcore.RouteDirect, false)); got != speak {
		t.Error("a direct question was not answered in the default region")
	}
}

func TestRegionNameRecordsWhatItKnows(t *testing.T) {
	// The journal names a region we carry, admits the raw code of one
	// we do not, calls a plain flood the wildcard, and says nothing
	// about direct traffic, where a relay acts on no region at all.
	e, _, _, peer := txRig(t, "shadow")
	e.regions = testRegions(t, "", []string{"fr"}, true)

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
	if got := e.regionName(build(meshcore.RouteFlood, "fr")); got != "fr" {
		t.Errorf("a carried region = %q, want its name", got)
	}
	foreign := build(meshcore.RouteFlood, "de")
	want := fmt.Sprintf("%#04x", foreign.pkt.TransportCodes[0])
	if got := e.regionName(foreign); got != want {
		t.Errorf("an uncarried region = %q, want the raw code %q", got, want)
	}
	if got := e.regionName(build(meshcore.RouteFlood, "")); got != wildcardRegion {
		t.Errorf("a plain flood = %q, want the wildcard", got)
	}
	if got := e.regionName(build(meshcore.RouteDirect, "")); got != "" {
		t.Errorf("direct traffic = %q, want nothing", got)
	}
}

func TestSessionLimitIsConfigurable(t *testing.T) {
	base := map[string]any{"frequency_hz": 869_618_000}
	p, err := paramsFrom(base)
	if err != nil {
		t.Fatal(err)
	}
	if got := p.withDefaults().SessionLimit; got != sessionLimitMax {
		t.Fatalf("default session_limit = %d, want %d", got, sessionLimitMax)
	}
	base["session_limit"] = 20
	if p, err = paramsFrom(base); err != nil || p.withDefaults().SessionLimit != 20 {
		t.Fatalf("session_limit = %d (%v)", p.SessionLimit, err)
	}
	base["session_limit"] = -1
	if _, err := build("test", base, bus.New(), zap.NewNop()); err == nil {
		t.Error("a budget below zero was accepted")
	}
}

// regionRig is a bare engine for grammar tests: the command handlers
// run on the caller's goroutine, which stands in for the pipeline's.
func regionRig(t *testing.T) *engine {
	t.Helper()
	e := newEngine("test", params{}.withDefaults(), nil, bus.New(), zap.NewNop())
	return e
}

func TestRegionCommandSpeaksTheReferenceReplies(t *testing.T) {
	e := regionRig(t)
	steps := []struct{ line, want string }{
		{"region", "*^ F\n"},
		{"region put lab-eu", "OK - (flood allowed)"},
		{"region put lab-eu-91 lab-eu", "OK - (flood allowed)"},
		{"region", "*^ F\n lab-eu F\n  lab-eu-91 F\n"},
		{"region get lab-eu-91", " lab-eu-91 (lab-eu) F"},
		{"region denyf lab-eu-91", "OK"},
		{"region get lab-eu-91", " lab-eu-91 (lab-eu) "},
		{"region allowf lab-eu-91", "OK"},
		{"region get nowhere", "Err - unknown region"},
		{"region put x nowhere", "Err - unknown parent"},
		{"region home lab-eu", " home is now lab-eu"},
		{"region home", " home is lab-eu"},
		{"region default", " default scope is <null>"},
		{"region default lab-eu", " default scope is now lab-eu"},
		{"region default fresh", " default scope is now fresh"},
		{"region default <null>", " default scope is now <null>"},
		{"region list allowed", "*,lab-eu,lab-eu-91,fresh"},
		{"region list denied", "-none-"},
		{"region list everything", "Err - use 'allowed' or 'denied'"},
		{"region remove lab-eu", "Err - not empty"},
		{"region remove nowhere", "Err - not found"},
		{"region remove *", "Err - not empty"},
		{"region remove lab-eu-91", "OK"},
		{"region save", "OK"},
		{"region nonsense", "Err - ??"},
	}
	for _, s := range steps {
		reply, handled := e.serveRegionLine("admin", s.line)
		if !handled || reply != s.want {
			t.Fatalf("%q = %q (handled %v), want %q", s.line, reply, handled, s.want)
		}
	}
	// Something else entirely is not region business.
	if _, handled := e.serveRegionLine("admin", "get name"); handled {
		t.Error("a non-region line was captured")
	}
}

func TestRegionDefComposesBeforeItInstalls(t *testing.T) {
	e := regionRig(t)
	reply, _ := e.serveRegionLine("admin", "region def a b c d|b e f")
	want := "*^ F\n a F\n  b F\n   c F\n    d F\n   e F\n    f F\n"
	if reply != want {
		t.Fatalf("def =\n%q\nwant\n%q", reply, want)
	}
	// A refused batch answers the reference's words and leaves the
	// live table untouched — same replies, sounder outcome than the
	// reference's no-rollback.
	reply, _ = e.serveRegionLine("admin", "region def x y|nowhere")
	if reply != "Err - unknown jump: nowhere" {
		t.Fatalf("bad def = %q", reply)
	}
	if e.regions.m.FindByName("x") != nil {
		t.Error("a refused def left its segments in the live table")
	}
}

func TestRegionLoadIsModalAndCommits(t *testing.T) {
	e := regionRig(t)
	e.serveRegionLine("admin", "region put keeper")
	e.serveRegionLine("admin", "region denyf keeper")
	keeperID := e.regions.m.FindByName("keeper").ID

	if reply, handled := e.serveRegionLine("admin", "region load"); !handled || reply != "" {
		t.Fatalf("load arm = %q/%v", reply, handled)
	}
	if !e.RegionLoadArmed("admin") || e.RegionLoadArmed("other") {
		t.Fatal("the staging is not keyed to its owner")
	}
	// While armed, this owner's lines feed the staging — even ones
	// that would otherwise be commands; another admin's do not.
	for _, line := range []string{"*", " keeper F", " fresh", "  child F"} {
		if reply, handled := e.serveRegionLine("admin", line); !handled || reply != "" {
			t.Fatalf("load line %q = %q/%v", line, reply, handled)
		}
	}
	if _, handled := e.serveRegionLine("other", "whatever"); handled {
		t.Error("another owner's line fed the staging")
	}
	reply, _ := e.serveRegionLine("admin", "")
	if reply != "OK - loaded 3 regions" {
		t.Fatalf("commit = %q", reply)
	}
	if e.RegionLoadArmed("admin") {
		t.Error("the staging survived its commit")
	}
	m := e.regions.m
	if r := m.FindByName("keeper"); r == nil || r.ID != keeperID {
		t.Error("the id did not carry over by name")
	}
	if r := m.FindByName("keeper"); r.Flags != meshcore.RegionDenyFlood {
		t.Error("carried flags did not win over the dump's F")
	}
	if r := m.FindByName("child"); r == nil || r.Flags != 0 ||
		r.Parent != m.FindByName("fresh").ID {
		t.Error("the staged hierarchy did not install")
	}
}

func TestRegionLoadStagingExpires(t *testing.T) {
	e := regionRig(t)
	e.serveRegionLine("admin", "region load")
	e.regionStaging.until = time.Now().Add(-time.Second)
	// The next line meets an expired staging: dropped, and the line is
	// served as what it is.
	if reply, handled := e.serveRegionLine("admin", "region"); !handled || reply != "*^ F\n" {
		t.Fatalf("after expiry = %q/%v, want the tree", reply, handled)
	}
	if e.regionStaging != nil {
		t.Error("the expired staging survived")
	}
}

// failingRegionStore refuses every save.
type failingRegionStore struct{ loaded *PersistedRegions }

func (s *failingRegionStore) LoadRegions() (*PersistedRegions, error) { return s.loaded, nil }
func (s *failingRegionStore) SaveRegions(PersistedRegions) error {
	return errors.New("disk full")
}

// memRegionStore keeps the last save.
type memRegionStore struct {
	loaded *PersistedRegions
	saved  []PersistedRegions
}

func (s *memRegionStore) LoadRegions() (*PersistedRegions, error) { return s.loaded, nil }
func (s *memRegionStore) SaveRegions(p PersistedRegions) error {
	s.saved = append(s.saved, p)
	return nil
}

func TestRegionMutationsPersistBeforeTheyInstall(t *testing.T) {
	e := regionRig(t)
	store := &memRegionStore{}
	if err := e.AttachRegions(store); err != nil {
		t.Fatal(err)
	}
	if reply, _ := e.serveRegionLine("admin", "region put lab"); reply != "OK - (flood allowed)" {
		t.Fatalf("put = %q", reply)
	}
	if len(store.saved) != 1 || len(store.saved[0].Entries) != 1 ||
		store.saved[0].Entries[0].Name != "lab" {
		t.Fatalf("saved = %+v", store.saved)
	}

	// A store that refuses leaves the live table exactly as it was.
	broken := regionRig(t)
	if err := broken.AttachRegions(&failingRegionStore{}); err != nil {
		t.Fatal(err)
	}
	reply, _ := broken.serveRegionLine("admin", "region put lab")
	if !strings.Contains(reply, "would not persist") {
		t.Fatalf("refused put = %q", reply)
	}
	if broken.regions.m.Count() != 0 {
		t.Error("a mutation the store refused reached the live table")
	}
	// Reads never touch the store.
	if reply, _ := broken.serveRegionLine("admin", "region"); reply != "*^ F\n" {
		t.Errorf("read = %q", reply)
	}
}

func TestAttachRegionsRestoresTheStoredMap(t *testing.T) {
	stored := &PersistedRegions{
		Entries: []meshcore.Region{
			{ID: 1, Parent: 0, Flags: 0, Name: "eu"},
			{ID: 2, Parent: 1, Flags: 0, Name: "fr"},
		},
		NextID: 3, DefaultID: 2, WildcardFlags: uint8(meshcore.RegionDenyFlood),
	}
	e := regionRig(t)
	if err := e.AttachRegions(&memRegionStore{loaded: stored}); err != nil {
		t.Fatal(err)
	}
	if e.regions.m.Count() != 2 || e.regions.m.Default().Name != "fr" {
		t.Fatalf("restored %d regions, default %+v", e.regions.m.Count(), e.regions.m.Default())
	}
	// The speaking key derives from the restored designation.
	if e.regions.speak != meshcore.TransportKeyForName("fr") {
		t.Error("the speaking key was not re-derived from the restore")
	}
	// A never-written relay keeps the fresh defaults.
	fresh := regionRig(t)
	if err := fresh.AttachRegions(&memRegionStore{}); err != nil {
		t.Fatal(err)
	}
	if fresh.regions.m.Count() != 0 || fresh.regions.m.Wildcard().Flags != 0 {
		t.Error("a never-written relay did not keep its defaults")
	}
	// A table that does not restore refuses the relay.
	bad := &PersistedRegions{Entries: []meshcore.Region{{ID: 0, Name: "x"}}, NextID: 1}
	if err := regionRig(t).AttachRegions(&memRegionStore{loaded: bad}); err == nil {
		t.Error("an unsound stored table was attached")
	}
}

func TestRegionMutationWithoutBounce(t *testing.T) {
	// The arbitration made flesh: a region mutation lands over the
	// order channel while the engine runs, no relay bounce, and the
	// flood gate follows it on the very next packet.
	e, dev, sub, peer := txRig(t, "on-air")
	runEngine(t, e, dev)

	scoped := func() radio.Frame {
		frame := peerAdvert(t, peer, time.Now())
		pkt, err := meshcore.ParsePacket(frame.Payload)
		if err != nil {
			t.Fatal(err)
		}
		meshcore.TransportKeyForName("lab").Scope(pkt)
		raw, err := pkt.MarshalBinary()
		if err != nil {
			t.Fatal(err)
		}
		frame.Payload = raw
		return frame
	}

	reply, handled, err := e.RegionCommand("admin", "region put lab")
	if err != nil || !handled || reply != "OK - (flood allowed)" {
		t.Fatalf("RegionCommand = %q/%v/%v", reply, handled, err)
	}
	dev.frames <- scoped()
	if sent := awaitSent(t, sub); sent.Kind != "relay-flood" {
		t.Fatalf("after put: %+v, want the region carried", sent)
	}

	if reply, _, err = e.RegionCommand("admin", "region denyf lab"); err != nil || reply != "OK" {
		t.Fatalf("denyf = %q/%v", reply, err)
	}
	snap, err := e.Regions()
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range snap.Entries {
		if r.Name == "lab" && r.Flags&meshcore.RegionDenyFlood == 0 {
			t.Error("the snapshot does not show the denial")
		}
	}
}

func TestARawCommandLineCrossesTheAirWhole(t *testing.T) {
	// The counter-test of the load defect: a TXT_MSG's leading spaces
	// are a load line's whole meaning, and the command path must hand
	// them to whoever runs the line, untouched.
	e, dev, sub, peer := txRig(t, "on-air")
	e.p.AdminPassword = "mask"
	var got []string
	e.AttachCommands(func(line string, admin []byte) string {
		got = append(got, line)
		return "ok"
	})
	runEngine(t, e, dev)

	frame, _ := login(t, e.id, peer, nowTS(400), "mask", false)
	dev.frames <- frame
	awaitSent(t, sub)
	<-dev.sent

	lines := []string{"region load", "*^ F", " eu F", "  fr F"}
	for i, l := range lines {
		at := time.Unix(int64(nowTS(410+uint32(i))), 0)
		dev.frames <- commandFrame(t, e.id, peer, at, l)
		awaitSent(t, sub)
		<-dev.sent
	}
	if len(got) != len(lines) {
		t.Fatalf("commands ran: %q", got)
	}
	for i, l := range got {
		if l != lines[i] {
			t.Errorf("line %d = %q, want %q — the air's bytes were normalised", i, l, lines[i])
		}
	}
}

func TestRegionLoadEndToEndThroughTheOrderDoor(t *testing.T) {
	// The dump sequence over the running engine's own order channel,
	// store attached: staged, committed, persisted — and a second
	// engine attaching the same store reads the identical table.
	e, dev, _, _ := txRig(t, "shadow")
	store := &memRegionStore{}
	if err := e.AttachRegions(store); err != nil {
		t.Fatal(err)
	}
	runEngine(t, e, dev)

	seq := []struct{ line, want string }{
		{"region put keeper", "OK - (flood allowed)"},
		{"region home keeper", " home is now keeper"},
		{"region default keeper", " default scope is now keeper"},
		{"region load", ""},
		{"*^ F", ""},
		{" keeper F", ""},
		{"  child", ""},
		{"", "OK - loaded 2 regions"},
	}
	for _, s := range seq {
		reply, handled, err := e.RegionCommand("air:admin", s.line)
		if err != nil || !handled || reply != s.want {
			t.Fatalf("%q = %q/%v/%v, want %q", s.line, reply, handled, err, s.want)
		}
	}
	if len(store.saved) == 0 {
		t.Fatal("the commit never persisted")
	}

	// A fresh engine restores the very table the commit wrote.
	last := store.saved[len(store.saved)-1]
	fresh := regionRig(t)
	if err := fresh.AttachRegions(&memRegionStore{loaded: &last}); err != nil {
		t.Fatal(err)
	}
	want := "* F\n keeper^ F\n  child\n"
	if got := fresh.regions.m.ExportTree(); got != want {
		t.Errorf("restored tree =\n%q\nwant\n%q", got, want)
	}
	if d := fresh.regions.m.Default(); d == nil || d.Name != "keeper" {
		t.Errorf("restored default = %+v", d)
	}
	if fresh.regions.speak != meshcore.TransportKeyForName("keeper") {
		t.Error("the restored engine does not speak the committed default")
	}
}

func TestAStagingIsAnExclusiveTransaction(t *testing.T) {
	e := regionRig(t)
	e.serveRegionLine("A", "region load")
	// Another owner's region commands are refused whole while the
	// staging stands — a mutation slipped under it would be undone by
	// the commit of a snapshot that never saw it — and a second load
	// must not replace the first.
	busy := "Err - busy - another admin is loading regions"
	if reply, handled := e.serveRegionLine("B", "region denyf *"); !handled || reply != busy {
		t.Errorf("mutation under staging = %q/%v", reply, handled)
	}
	if reply, _ := e.serveRegionLine("B", "region load"); reply != busy {
		t.Errorf("second load = %q", reply)
	}
	if e.regionStaging == nil || e.regionStaging.owner != "A" {
		t.Fatal("the staging changed hands")
	}
	// B's non-region lines still pass through to their own dispatch.
	if _, handled := e.serveRegionLine("B", "get name"); handled {
		t.Error("a non-region line was captured by someone else's staging")
	}
	// A commits; B is welcome again.
	e.serveRegionLine("A", " fresh F")
	if reply, _ := e.serveRegionLine("A", ""); reply != "OK - loaded 1 regions" {
		t.Fatalf("commit = %q", reply)
	}
	if reply, _ := e.serveRegionLine("B", "region denyf *"); reply != "OK" {
		t.Errorf("after commit = %q", reply)
	}
}

func TestPrivateRegionsAreRefusedAtEveryDoor(t *testing.T) {
	// def: composed on the clone, refused at install, live untouched.
	e := regionRig(t)
	reply, _ := e.serveRegionLine("A", "region def $secret")
	if !strings.Contains(reply, "not supported") {
		t.Errorf("def $ = %q", reply)
	}
	if e.regions.m.Count() != 0 {
		t.Error("a refused def left the private region live")
	}
	// load: staged, refused at commit, live untouched.
	e.serveRegionLine("A", "region load")
	e.serveRegionLine("A", " $secret F")
	if reply, _ := e.serveRegionLine("A", ""); !strings.Contains(reply, "not supported") {
		t.Errorf("load commit with $ = %q", reply)
	}
	if e.regions.m.Count() != 0 {
		t.Error("a refused load left the private region live")
	}
	// default: the auto-create path names the policy.
	if reply, _ := e.serveRegionLine("A", "region default $secret"); !strings.Contains(reply, "not supported") {
		t.Errorf("default $ = %q", reply)
	}
	// restore: a store that somehow holds one refuses the relay.
	bad := &PersistedRegions{
		Entries: []meshcore.Region{{ID: 1, Name: "$secret"}}, NextID: 2,
	}
	if err := regionRig(t).AttachRegions(&memRegionStore{loaded: bad}); err == nil ||
		!strings.Contains(err.Error(), "not supported") {
		t.Errorf("restore $ = %v", err)
	}
}

func TestAnonRegionsAnswerSkipsAndContinues(t *testing.T) {
	// The reference's export rule: a name that will not fit is left
	// out and the walk continues — a short name after the long ones
	// still answers, where a break would have silently ended the list.
	e, _, _, peer := txRig(t, "on-air")
	e.queue.depth = 4
	names := make([]string, 0, 9)
	for i := range 8 {
		names = append(names, strings.Repeat(string(rune('a'+i)), 30))
	}
	names = append(names, "z")
	e.regions = testRegions(t, "", names, true)

	secret, err := e.id.SharedSecret(peer.PubKey[:])
	if err != nil {
		t.Fatal(err)
	}
	plain, err := meshcore.FrameAnonRequest(42, meshcore.AnonReqScopes, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	pkt, err := meshcore.BuildAnonDatagram(e.id.PubKey[:meshcore.PathHashSize],
		peer.PubKey[:], secret, plain)
	if err != nil {
		t.Fatal(err)
	}
	pkt.Header = meshcore.MakeHeader(meshcore.RouteDirect,
		meshcore.PayloadTypeAnonReq, meshcore.PayloadVer1)
	rx := rxOf(e, pkt)
	e.respondAnon(rx, rx.id)

	if len(e.queue.entries) != 1 {
		t.Fatalf("the answer was not composed: %d queued", len(e.queue.entries))
	}
	d, err := meshcore.ParseDatagram(e.queue.entries[0].pkt.Payload)
	if err != nil {
		t.Fatal(err)
	}
	body, err := d.Open(secret)
	if err != nil {
		t.Fatal(err)
	}
	_, rest, err := meshcore.UnframeAdmin(body)
	if err != nil {
		t.Fatal(err)
	}
	reply, err := meshcore.ParseAnonReply(rest)
	if err != nil {
		t.Fatal(err)
	}
	text := reply.Text
	if !strings.HasSuffix(text, ",z") {
		t.Errorf("answer = %q — the short name after the long ones was dropped", text)
	}
	if strings.Contains(text, strings.Repeat("h", 30)) {
		t.Errorf("answer = %q — every long name fit, the test bites nothing", text)
	}
}
