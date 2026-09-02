package meshcore

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"meshrunner.dev/pkg/meshcore"

	"meshrunner.dev/lotor/internal/bus"
)

func TestSessionSnapshotCarriesTheRouteButNeverTheSecret(t *testing.T) {
	a := newACL(nil)
	now := time.Now()
	with := &client{Secret: []byte("derived"), LastActive: now, Active: true}
	with.PubKey[0] = 0xBB
	with.Out = &outPath{PathLen: 2, Path: []byte{0x4f, 0xa2}, Learned: now}
	without := &client{Secret: []byte("derived"), LastActive: now, Active: true}
	without.PubKey[0] = 0xCC
	idle := &client{LastActive: now.Add(-2 * sessionIdle), Active: true}
	idle.PubKey[0] = 0xDD
	a.Put(with)
	a.Put(without)
	a.Put(idle)

	rows := a.Sessions()
	if len(rows) != 3 {
		t.Fatalf("snapshot holds %d rows, want 3 — expiry belongs to the engine clock", len(rows))
	}
	for _, r := range rows {
		switch r.PubKey[0] {
		case 0xBB:
			if !r.HasPath || len(r.Path) != 2 || r.Path[0] != 0x4f {
				t.Errorf("the taught route did not travel: %+v", r)
			}
		case 0xCC:
			if r.HasPath || r.Path != nil {
				t.Errorf("a route was invented: %+v", r)
			}
		}
	}
	// A read must not change what it reads: even an overdue entry is
	// left for the engine clock to retire.
	if _, ok := a.By[idle.PubKey]; !ok {
		t.Error("the snapshot retired an entry")
	}
	// And the copy is a copy: bending it must not bend the table.
	rows[0].Path = append(rows[0].Path, 0xFF)
	if with.Out != nil && len(with.Out.Path) != 2 {
		t.Error("the snapshot shares the table's bytes")
	}
}

func TestClientViewsNeverWakeReceive(t *testing.T) {
	e, _ := testEngine(t)
	ctx, stop := context.WithCancel(t.Context())
	window, cancel := e.receiveWindow(ctx)
	defer func() {
		cancel()
		stop()
	}()

	if _, err := e.AccessList(); err != nil {
		t.Fatal(err)
	}
	if _, err := e.ClientSessions(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-window.Done():
		t.Fatal("reading a client view yielded the receive window")
	default:
	}
}

func TestClientExpiryPublishesOneCoherentGeneration(t *testing.T) {
	e, sub := testEngine(t)
	now := time.Now()
	guest := &client{
		PubKey: aclKey(0x11), Perms: permGuest,
		Active: true, LastActive: now.Add(-sessionIdle),
	}
	durable := &client{
		PubKey: aclKey(0x22), Perms: permReadOnly, Granted: true,
		Active: true, LastActive: now.Add(-sessionIdle),
	}
	durable.Out = &outPath{PathLen: 1, Path: []byte{0xaa}, Learned: now}
	if err := e.acl.Put(guest); err != nil {
		t.Fatal(err)
	}
	if err := e.acl.Put(durable); err != nil {
		t.Fatal(err)
	}
	e.publishClientView(time.Time{}, false)
	before := e.Clients()
	if len(before.Access) != 1 || len(before.Sessions) != 2 {
		t.Fatalf("initial view = %+v", before)
	}

	if !e.expireClientSessions(now) {
		t.Fatal("the deadline changed nothing")
	}
	after := e.Clients()
	if after.Generation != before.Generation+1 {
		t.Fatalf("generation %d after %d", after.Generation, before.Generation)
	}
	if len(after.Sessions) != 0 || len(after.Access) != 1 || after.Access[0].PubKey != durable.PubKey {
		t.Fatalf("expired view = %+v", after)
	}
	if e.acl.Get(guest.PubKey[:]) != nil {
		t.Fatal("the expired guest kept its live credential")
	}
	kept := e.acl.Get(durable.PubKey[:])
	if kept == nil || kept.Active || kept.Out == nil {
		t.Fatalf("durable principal was removed or damaged: %+v", kept)
	}

	select {
	case raw := <-sub.C:
		ev, ok := raw.(bus.SessionsChanged)
		if !ok {
			t.Fatalf("event = %T, want SessionsChanged", raw)
		}
		if ev.Relay != e.relay || ev.Generation != after.Generation || !ev.At.Equal(now) {
			t.Fatalf("event = %+v", ev)
		}
	case <-time.After(time.Second):
		t.Fatal("no client-view event")
	}
	if e.expireClientSessions(now.Add(time.Second)) {
		t.Fatal("an unchanged table published another expiry")
	}
	select {
	case ev := <-sub.C:
		t.Fatalf("unchanged expiry published %T", ev)
	default:
	}
}

func TestDryGateSchedulesClientExpiry(t *testing.T) {
	e, _ := testEngine(t)
	now := time.Now()
	e.acl.By[aclKey(0x44)] = &client{
		PubKey: aclKey(0x44), Active: true,
		LastActive: now.Add(-sessionIdle).Add(50 * time.Millisecond),
	}

	wait, reason, scheduled := e.receiveStateWake(now)
	if !scheduled || reason != "session-deadline" || wait < 45*time.Millisecond || wait > 55*time.Millisecond {
		t.Fatalf("wake = %v, %q, %v", wait, reason, scheduled)
	}
}

func TestAnswersFitTheRouteTheyWillTravel(t *testing.T) {
	// The residual the remediation review found: the body was sized on
	// the direct budget and the route discovered afterwards. A question
	// that arrived flooded is answered inside a path return that pays
	// for the path it came by — a far smaller envelope — so the answer
	// could not be sealed, while the replay guard was already spent and
	// the client's identical retry was refused as a replay.
	e, _ := identifiedEngine(t)
	if err := e.AttachSessions(newFakeStore()); err != nil {
		t.Fatal(err)
	}
	for i := range maxClients {
		if err := e.acl.Put(&client{
			PubKey: aclKey(byte(i)), Perms: permReadWrite, Granted: true,
			LastActive: time.Now(),
		}); err != nil {
			t.Fatal(err)
		}
	}
	secret := bytes.Repeat([]byte{0x11}, meshcore.SharedSecretSize)
	hash := e.id.PubKey[:meshcore.PathHashSize]

	// Every route the answer may take, from a direct question to a
	// flood that walked a long way home.
	for _, pathBytes := range []int{0, 1, 12, 32, 48} {
		inbound := &meshcore.Packet{
			Header:  meshcore.MakeHeader(meshcore.RouteFlood, meshcore.PayloadTypeReq, meshcore.PayloadVer1),
			Path:    make([]byte, pathBytes),
			PathLen: byte(pathBytes),
		}
		body := e.accessListBody(e.answerBudget(inbound))
		if len(body)%7 != 0 {
			t.Errorf("path %d: the list was cut mid-record: %d bytes", pathBytes, len(body))
		}
		// The composition the reply will actually attempt.
		framed := meshcore.FrameAdmin(1, body)
		if _, err := meshcore.BuildPathReturn(hash, hash, secret,
			byte(pathBytes), make([]byte, pathBytes),
			byte(meshcore.PayloadTypeResponse), framed); err != nil {
			t.Errorf("path %d: the answer this node would send cannot be sealed: %v",
				pathBytes, err)
		}
	}

	// And the direct route still uses its own, larger budget: the fix
	// must not have shrunk every answer to the worst case.
	direct := &meshcore.Packet{
		Header: meshcore.MakeHeader(meshcore.RouteDirect, meshcore.PayloadTypeReq, meshcore.PayloadVer1),
	}
	wide := e.accessListBody(e.answerBudget(direct))
	narrow := e.accessListBody(e.answerBudget(&meshcore.Packet{
		Header:  meshcore.MakeHeader(meshcore.RouteFlood, meshcore.PayloadTypeReq, meshcore.PayloadVer1),
		Path:    make([]byte, 48),
		PathLen: 48,
	}))
	if len(wide) <= len(narrow) {
		t.Errorf("direct answer %d bytes, flooded %d — the direct route carries more",
			len(wide), len(narrow))
	}
	if _, err := meshcore.BuildResponse(hash, hash, secret, 1, wide); err != nil {
		t.Errorf("the direct answer cannot be sealed: %v", err)
	}
}

func TestAScopesAnswerIsCutAtAWholeName(t *testing.T) {
	// Six thirty-character scopes overran the packet. Half a name at
	// the far end is a scope nobody can derive a key for, so the list
	// stops at the last whole one — and it must still compose.
	e, _, _, peer := txRig(t, "on-air")
	e.queue.depth = 4
	names := make([]string, 0, 8)
	for i := range 8 {
		names = append(names, strings.Repeat(string(rune('a'+i)), 30))
	}
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
		t.Fatalf("the scopes answer was not composed: %d queued", len(e.queue.entries))
	}
	// Every name that made it is whole.
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
	served := meshcore.ScopeNames(reply.Text)
	if len(served) == 0 {
		t.Fatal("no scope survived the cut")
	}
	for _, n := range served {
		if len(n) != 30 && n != "*" {
			t.Errorf("scope %q was cut mid-name", n)
		}
	}
	if len(served) >= len(names) {
		t.Errorf("nothing was cut: %d names served of %d", len(served), len(names))
	}
}
