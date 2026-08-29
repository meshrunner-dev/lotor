package confdb

import (
	"context"
	"testing"
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
