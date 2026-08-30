package bus

import "testing"

func TestTrafficSourceKeysKeepStationsDisjointFromRelays(t *testing.T) {
	relay := FrameSent{Relay: "alice"}
	station := FrameSent{SourceKind: SourceStation, Source: "alice"}
	if relay.SourceKey() != "alice" || station.SourceKey() != "station/alice" ||
		relay.SourceKey() == station.SourceKey() {
		t.Fatalf("source keys relay=%q station=%q", relay.SourceKey(), station.SourceKey())
	}
	if got := (TxDropped{SourceKind: SourceStation, Source: "alice"}).SourceKey(); got != "station/alice" {
		t.Fatalf("drop source = %q", got)
	}
}

func TestFanOutAndDropAccounting(t *testing.T) {
	b := New()
	fast := b.Subscribe(4)
	slow := b.Subscribe(1)
	defer fast.Close()
	defer slow.Close()

	for range 3 {
		b.Publish(FrameHeard{Relay: "r"})
	}

	if got := len(fast.C); got != 3 {
		t.Errorf("fast subscriber has %d events, want 3", got)
	}
	if got := len(slow.C); got != 1 {
		t.Errorf("slow subscriber has %d events, want 1", got)
	}
	if got := slow.Dropped(); got != 2 {
		t.Errorf("slow dropped = %d, want 2", got)
	}
	if got := fast.Dropped(); got != 0 {
		t.Errorf("fast dropped = %d, want 0", got)
	}
}

func TestSubscribeClampsDegenerateBuffers(t *testing.T) {
	b := New()
	for _, buffer := range []int{0, -1} {
		s := b.Subscribe(buffer) // must not panic, must hold one event
		b.Publish(FrameHeard{Relay: "r"})
		if got := len(s.C); got != 1 {
			t.Errorf("Subscribe(%d) buffered %d events, want 1", buffer, got)
		}
		s.Close()
	}
}

func TestCloseUnregisters(t *testing.T) {
	b := New()
	s := b.Subscribe(1)
	s.Close()
	s.Close()                         // idempotent
	b.Publish(RelayState{Relay: "r"}) // must not panic on a closed channel
	if _, ok := <-s.C; ok {
		t.Error("channel should be closed and drained")
	}
}
