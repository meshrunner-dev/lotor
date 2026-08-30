package confdb

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestACLRoundTripsAndForgets(t *testing.T) {
	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "acl.db"), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	if err := s.SaveACL(ctx, "mc", ACLRow{
		PubKey: bytes.Repeat([]byte{0xcd}, 32), Perms: 0, LastActive: time.Now(),
	}); err == nil {
		t.Fatal("the ACL accepted the guest/deleted role")
	}

	key := bytes.Repeat([]byte{0xab}, 32)
	now := time.Now().Truncate(time.Second)
	row := ACLRow{
		PubKey: key, Perms: 3, LastTimestamp: 1756000000,
		HasOut: true, OutPath: []byte{0x11, 0x22}, OutPathLen: 2,
		Learned: now.Add(-time.Minute), LastActive: now,
	}
	if err := s.SaveACL(ctx, "mc", row); err != nil {
		t.Fatal(err)
	}
	// A second save is an update, not a duplicate.
	row.LastTimestamp = 1756000009
	if err := s.SaveACL(ctx, "mc", row); err != nil {
		t.Fatal(err)
	}
	rows, err := s.LoadACL(ctx, "mc")
	if err != nil || len(rows) != 1 {
		t.Fatalf("load: %d rows, %v", len(rows), err)
	}
	got := rows[0]
	if !bytes.Equal(got.PubKey, key) || got.Perms != 3 || got.LastTimestamp != 1756000009 ||
		got.OutPathLen != 2 || !bytes.Equal(got.OutPath, []byte{0x11, 0x22}) {
		t.Errorf("round trip lost data: %+v", got)
	}
	if !got.LastActive.Equal(now) {
		t.Errorf("last_active drifted: %v vs %v", got.LastActive, now)
	}
	// Another relay's table is its own.
	if rows, _ := s.LoadACL(ctx, "other"); len(rows) != 0 {
		t.Errorf("relay isolation broken: %d", len(rows))
	}
	// A route-less access entry stores as such: nil path, zero learned.
	if err := s.SaveACL(ctx, "mc", ACLRow{PubKey: bytes.Repeat([]byte{1}, 32),
		Perms: 1, LastActive: now}); err != nil {
		t.Fatal(err)
	}
	rows, err = s.LoadACL(ctx, "mc")
	if err != nil {
		t.Fatalf("load after route-less save: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected two access entries, have %d", len(rows))
	}
	for _, r := range rows {
		if bytes.Equal(r.PubKey, bytes.Repeat([]byte{1}, 32)) &&
			(r.OutPath != nil || !r.Learned.IsZero()) {
			t.Errorf("route-less access entry grew a route: %+v", r)
		}
	}
	if err := s.ForgetACL(ctx, "mc", key); err != nil {
		t.Fatal(err)
	}
	rows, _ = s.LoadACL(ctx, "mc")
	if len(rows) != 1 {
		t.Errorf("forget left %d rows", len(rows))
	}
}

func TestACLLoadsFreshestFirst(t *testing.T) {
	// Which access entries a restart keeps when the store holds more than
	// the table has places for is a policy, not row order.
	ctx := context.Background()
	s, err := Open(ctx, Memory, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()

	now := time.Now()
	for i, age := range []time.Duration{3 * time.Hour, time.Hour, 2 * time.Hour} {
		if err := s.SaveACL(ctx, "mc", ACLRow{
			PubKey: []byte{byte(i)}, Perms: 1, LastActive: now.Add(-age),
		}); err != nil {
			t.Fatal(err)
		}
	}
	rows, err := s.LoadACL(ctx, "mc")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 3 {
		t.Fatalf("loaded %d", len(rows))
	}
	for i := 1; i < len(rows); i++ {
		if rows[i].LastActive.After(rows[i-1].LastActive) {
			t.Errorf("row %d is fresher than row %d — the order is not the policy", i, i-1)
		}
	}
}
