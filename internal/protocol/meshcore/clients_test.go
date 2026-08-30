package meshcore

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"meshrunner.dev/pkg/meshcore"
)

func TestSessionSnapshotCarriesTheRouteButNeverTheSecret(t *testing.T) {
	a := newACL(nil)
	now := time.Now()
	with := &client{secret: []byte("derived"), lastActive: now, active: true}
	with.pubKey[0] = 0xBB
	with.out = &outPath{pathLen: 2, path: []byte{0x4f, 0xa2}, learned: now}
	without := &client{secret: []byte("derived"), lastActive: now, active: true}
	without.pubKey[0] = 0xCC
	idle := &client{lastActive: now.Add(-2 * sessionIdle), active: true}
	idle.pubKey[0] = 0xDD
	a.put(with)
	a.put(without)
	a.put(idle)

	rows := a.sessions(now)
	if len(rows) != 2 {
		t.Fatalf("snapshot holds %d rows, want 2 — the idle one must be skipped", len(rows))
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
	// A read must not change what it reads: the idle entry is skipped,
	// not retired.
	if _, ok := a.by[idle.pubKey]; !ok {
		t.Error("the snapshot retired an entry")
	}
	// And the copy is a copy: bending it must not bend the table.
	rows[0].Path = append(rows[0].Path, 0xFF)
	if with.out != nil && len(with.out.path) != 2 {
		t.Error("the snapshot shares the table's bytes")
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
		if err := e.acl.put(&client{
			pubKey: aclKey(byte(i)), perms: permReadWrite, granted: true,
			lastActive: time.Now(),
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
