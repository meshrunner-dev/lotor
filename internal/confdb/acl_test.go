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

func TestSwapACLIsOneStep(t *testing.T) {
	// Admitting a session into a full table is a swap, and the store
	// must never be caught holding both — a crash between two writes
	// would decide by accident which one survives.
	ctx := context.Background()
	s, err := Open(ctx, Memory, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()

	victim := []byte{1, 2, 3}
	newcomer := []byte{4, 5, 6}
	now := time.Now()
	if err := s.SaveACL(ctx, "mc", ACLRow{
		PubKey: victim, Perms: 1, LastActive: now.Add(-time.Hour),
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.SwapACL(ctx, "mc", ACLRow{
		PubKey: newcomer, Perms: 3, Granted: true, LastTimestamp: 99, LastActive: now,
	}, victim); err != nil {
		t.Fatal(err)
	}
	rows, err := s.LoadACL(ctx, "mc")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("the store holds %d sessions after a swap, want one", len(rows))
	}
	if !bytes.Equal(rows[0].PubKey, newcomer) {
		t.Errorf("the store kept %x, want the newcomer", rows[0].PubKey)
	}
	if rows[0].Perms != 3 || !rows[0].Granted || rows[0].LastTimestamp != 99 {
		t.Errorf("the newcomer arrived as %+v", rows[0])
	}
}

func TestACLLoadsFreshestFirst(t *testing.T) {
	// Which sessions a restart keeps when the store holds more than
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
			PubKey: []byte{byte(i)}, LastActive: now.Add(-age),
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
