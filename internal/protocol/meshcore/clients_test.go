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
	with := &client{secret: []byte("derived"), lastActive: now}
	with.pubKey[0] = 0xBB
	with.out = &outPath{pathLen: 2, path: []byte{0x4f, 0xa2}, learned: now}
	without := &client{secret: []byte("derived"), lastActive: now}
	without.pubKey[0] = 0xCC
	idle := &client{lastActive: now.Add(-2 * sessionIdle)}
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

func TestTheAccessListIsCutWhereTheAnswerFits(t *testing.T) {
	// A list sized on the raw payload composed whole and was then
	// refused — twenty-five entries were enough — leaving the asker
	// with no answer at all and its replay guard already spent.
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
	body := e.accessListBody()
	if n := len(body); n%7 != 0 {
		t.Errorf("the list was cut mid-record: %d bytes", n)
	}
	if len(body) > meshcore.ResponseBodyBudget() {
		t.Fatalf("body of %d bytes past the %d the codec can seal",
			len(body), meshcore.ResponseBodyBudget())
	}
	// And what it produced actually composes, which is the only test
	// that matters: the whole table's worth of grants, sealed.
	secret := bytes.Repeat([]byte{0x11}, meshcore.SharedSecretSize)
	if _, err := meshcore.BuildResponse(e.id.PubKey[:meshcore.PathHashSize],
		e.id.PubKey[:meshcore.PathHashSize], secret, 1, body); err != nil {
		t.Fatalf("the access list this node would send cannot be sealed: %v", err)
	}
	// One record more would not, which is what makes the cut the
	// boundary rather than a guess.
	tooBig := append(append([]byte(nil), body...), make([]byte, 7)...)
	if _, err := meshcore.BuildResponse(e.id.PubKey[:meshcore.PathHashSize],
		e.id.PubKey[:meshcore.PathHashSize], secret, 1, tooBig); err == nil {
		t.Error("the cut left a whole record of room unused")
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
	e.scopes = newScopeTable(params{AcceptScopes: names})

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
