package confdb

import (
	"context"
	"testing"
	"time"
)

func TestRegionsReplaceAndLoadRoundTrip(t *testing.T) {
	ctx := context.Background()
	s, err := Open(ctx, Memory)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()

	// A relay never written is absent, not empty.
	if _, _, ok, err := s.LoadRegions(ctx, "mc"); err != nil || ok {
		t.Fatalf("unwritten relay: ok=%v err=%v", ok, err)
	}

	entries := []RegionRow{
		{ID: 3, Parent: 0, Flags: 0, Name: "eu"},
		{ID: 1, Parent: 3, Flags: 1, Name: "fr"},
		{ID: 2, Parent: 1, Flags: 0, Name: "fr-idf"},
	}
	meta := RegionsMeta{NextID: 4, HomeID: 2, DefaultID: 1, WildcardFlags: 1}
	if err := s.ReplaceRegions(ctx, "mc", entries, meta); err != nil {
		t.Fatal(err)
	}
	got, gotMeta, ok, err := s.LoadRegions(ctx, "mc")
	if err != nil || !ok {
		t.Fatalf("load: ok=%v err=%v", ok, err)
	}
	if gotMeta != meta {
		t.Errorf("meta = %+v, want %+v", gotMeta, meta)
	}
	// Stored order is the wire order, whatever the ids say.
	for i, r := range got {
		if r.ID != entries[i].ID || r.Parent != entries[i].Parent ||
			r.Flags != entries[i].Flags || r.Name != entries[i].Name || r.Seq != i {
			t.Errorf("row %d = %+v, want %+v", i, r, entries[i])
		}
	}

	// Replace is the whole map: what the new state omits is gone, and
	// an emptied table with its meta row is present, not absent.
	if err := s.ReplaceRegions(ctx, "mc", nil, RegionsMeta{NextID: 4}); err != nil {
		t.Fatal(err)
	}
	got, gotMeta, ok, err = s.LoadRegions(ctx, "mc")
	if err != nil || !ok || len(got) != 0 || gotMeta.NextID != 4 {
		t.Errorf("emptied map: rows=%d meta=%+v ok=%v err=%v", len(got), gotMeta, ok, err)
	}

	// Two relays keep their maps apart.
	if err := s.ReplaceRegions(ctx, "other", entries[:1], meta); err != nil {
		t.Fatal(err)
	}
	if got, _, _, _ := s.LoadRegions(ctx, "mc"); len(got) != 0 {
		t.Error("one relay's replace leaked into another's map")
	}
}

func TestLoadRegionsJudgesRangesBeforeNarrowing(t *testing.T) {
	// A corrupt store must fail closed, not narrow into a different —
	// and more permissive — policy: wildcard_flags 256 read through a
	// uint8 is 0, "carry every plain flood". The CHECK constraints
	// refuse such rows at insertion; the Go validation is the second
	// wall, for stores written before the constraints existed or
	// edited around them.
	ctx := context.Background()
	s, err := Open(ctx, Memory)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	if _, err := s.db.ExecContext(ctx, "PRAGMA ignore_check_constraints = ON"); err != nil {
		t.Fatal(err)
	}
	plant := func(metaVals [4]int64, row *[5]int64) {
		_, _ = s.db.ExecContext(ctx, "DELETE FROM regions_meta; DELETE FROM regions")
		_, err := s.db.ExecContext(ctx,
			"INSERT INTO regions_meta VALUES('mc', ?, ?, ?, ?)",
			metaVals[0], metaVals[1], metaVals[2], metaVals[3])
		if err != nil {
			t.Fatal(err)
		}
		if row != nil {
			if _, err := s.db.ExecContext(ctx,
				"INSERT INTO regions VALUES('mc', ?, ?, 'x', ?, ?)",
				row[0], row[1], row[2], row[3]); err != nil {
				t.Fatal(err)
			}
		}
	}
	cases := []struct {
		name string
		meta [4]int64
		row  *[5]int64
	}{
		{"wildcard_flags past the byte", [4]int64{1, 0, 0, 256}, nil},
		{"negative next_id", [4]int64{-1, 0, 0, 0}, nil},
		{"default_id past the wire", [4]int64{1, 0, 65536, 0}, nil},
		{"row id past the wire", [4]int64{1, 0, 0, 0}, &[5]int64{65536, 0, 0, 0}},
		{"row flags past the byte", [4]int64{1, 0, 0, 0}, &[5]int64{1, 0, 256, 0}},
		{"negative seq", [4]int64{1, 0, 0, 0}, &[5]int64{1, 0, 0, -1}},
	}
	for _, c := range cases {
		plant(c.meta, c.row)
		if _, _, _, err := s.LoadRegions(ctx, "mc"); err == nil {
			t.Errorf("%s: loaded — corruption became a policy", c.name)
		}
	}
	// The constraints themselves hold on a store born with them.
	if _, err := s.db.ExecContext(ctx, "PRAGMA ignore_check_constraints = OFF"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.ExecContext(ctx,
		"INSERT INTO regions VALUES('mc', 70000, 0, 'x', 0, 0)"); err == nil {
		t.Error("the CHECK constraint let an out-of-range id in")
	}
}

func TestRemovingARelayTakesItsRuntimeStateAlong(t *testing.T) {
	// A relay recreated under a removed name must start anew: leaving
	// the old sessions and regions behind silently resurrected the
	// grants and the transport policy of a thing the operator deleted.
	ctx := context.Background()
	s, err := Open(ctx, Memory)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	if err := s.Replace(ctx, KindRelay, "mc", map[string]any{"radio": "r"}, "t", "add", nil); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveACL(ctx, "mc", ACLRow{PubKey: []byte{1, 2}, LastActive: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if err := s.ReplaceRegions(ctx, "mc",
		[]RegionRow{{ID: 1, Name: "eu"}}, RegionsMeta{NextID: 2}); err != nil {
		t.Fatal(err)
	}

	if err := s.Remove(ctx, KindRelay, "mc", "t"); err != nil {
		t.Fatal(err)
	}
	if rows, _ := s.LoadACL(ctx, "mc"); len(rows) != 0 {
		t.Error("the sessions survived the relay")
	}
	if _, _, ok, _ := s.LoadRegions(ctx, "mc"); ok {
		t.Error("the region table survived the relay")
	}

	// Removing anything else leaves runtime state untouched.
	if err := s.Replace(ctx, KindRadio, "mc", map[string]any{"driver": "d"}, "t", "add", nil); err != nil {
		t.Fatal(err)
	}
	if err := s.ReplaceRegions(ctx, "mc",
		[]RegionRow{{ID: 1, Name: "eu"}}, RegionsMeta{NextID: 2}); err != nil {
		t.Fatal(err)
	}
	if err := s.Remove(ctx, KindRadio, "mc", "t"); err != nil {
		t.Fatal(err)
	}
	if _, _, ok, _ := s.LoadRegions(ctx, "mc"); !ok {
		t.Error("a radio's removal purged a relay's regions")
	}
}
