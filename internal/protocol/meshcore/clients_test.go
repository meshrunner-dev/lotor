package meshcore

import (
	"testing"
	"time"
)

func TestSessionSnapshotCarriesTheRouteButNeverTheSecret(t *testing.T) {
	a := newACL()
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
