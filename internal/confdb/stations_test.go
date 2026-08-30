package confdb

import (
	"bytes"
	"context"
	"testing"
)

func TestStationStateRoundTripAndReplace(t *testing.T) {
	s := openTest(t)
	ctx := context.Background()
	if state, ok, err := s.LoadStationState(ctx, "alice"); err != nil || ok || state != nil {
		t.Fatalf("missing state = %q, %v, %v", state, ok, err)
	}
	if err := s.SaveStationState(ctx, "alice", []byte(`{"version":1}`)); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveStationState(ctx, "alice", []byte(`{"version":2}`)); err != nil {
		t.Fatal(err)
	}
	state, ok, err := s.LoadStationState(ctx, "alice")
	if err != nil || !ok || !bytes.Equal(state, []byte(`{"version":2}`)) {
		t.Fatalf("state = %q, %v, %v", state, ok, err)
	}
	state[0] = '!'
	again, _, _ := s.LoadStationState(ctx, "alice")
	if again[0] == '!' {
		t.Fatal("loaded station state aliases the store buffer")
	}
}

func TestStationStateFollowsStationLifetime(t *testing.T) {
	s := openTest(t)
	ctx := context.Background()
	station := map[string]any{"protocol": "meshcore", "listen": "127.0.0.1:5000"}
	if err := s.Replace(ctx, KindStation, "alice", station, "test", "add", nil); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveStationState(ctx, "alice", []byte(`{"version":1}`)); err != nil {
		t.Fatal(err)
	}
	if err := s.Remove(ctx, KindStation, "alice", "test"); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := s.LoadStationState(ctx, "alice"); err != nil || ok {
		t.Fatalf("state survived station removal: %v, %v", ok, err)
	}

	if err := s.Replace(ctx, KindStation, "alice", station, "test", "add", nil); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveStationState(ctx, "alice", []byte(`{"version":1}`)); err != nil {
		t.Fatal(err)
	}
	if err := s.ImportFile(ctx, sample(), "test"); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := s.LoadStationState(ctx, "alice"); err != nil || ok {
		t.Fatalf("state survived configuration import: %v, %v", ok, err)
	}
}
