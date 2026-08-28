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
	s, err := Open(ctx, filepath.Join(t.TempDir(), "acl.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()

	key := bytes.Repeat([]byte{0xab}, 32)
	now := time.Now().Truncate(time.Second)
	row := ACLRow{
		PubKey: key, Perms: 3, LastTimestamp: 1756000000,
		OutPath: []byte{0x11, 0x22}, OutPathLen: 2,
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
	// A route-less session stores as such: nil path, zero learned.
	if err := s.SaveACL(ctx, "mc", ACLRow{PubKey: bytes.Repeat([]byte{1}, 32),
		Perms: 0, LastActive: now}); err != nil {
		t.Fatal(err)
	}
	rows, err = s.LoadACL(ctx, "mc")
	if err != nil {
		t.Fatalf("load after route-less save: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected two sessions, have %d", len(rows))
	}
	for _, r := range rows {
		if r.Perms == 0 && (r.OutPath != nil || !r.Learned.IsZero()) {
			t.Errorf("route-less session grew a route: %+v", r)
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
