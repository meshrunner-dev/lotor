package origin

import (
	"testing"
	"time"
)

func TestEmissionQueueOrdersDuePacketsByReferencePriority(t *testing.T) {
	queue := NewQueue(4)
	if !queue.Offer(Emission{Kind: "low", Priority: 5}) ||
		!queue.Offer(Emission{Kind: "first-high", Priority: 1}) ||
		!queue.Offer(Emission{Kind: "second-high", Priority: 1}) {
		t.Fatal("queue refused an emission below capacity")
	}
	for _, want := range []string{"first-high", "second-high", "low"} {
		got, ok := queue.TakeUntil(t.Context(), time.Now().Add(time.Second))
		if !ok || got.Kind != want {
			t.Fatalf("queue yielded %q, %v; want %q", got.Kind, ok, want)
		}
	}
}

func TestFutureHighPriorityDoesNotBlockDuePacket(t *testing.T) {
	queue := NewQueue(2)
	_ = queue.Offer(Emission{Kind: "future", Priority: 0, NotBefore: time.Now().Add(time.Hour)})
	_ = queue.Offer(Emission{Kind: "due", Priority: 5})
	got, ok := queue.TakeUntil(t.Context(), time.Now().Add(time.Second))
	if !ok || got.Kind != "due" {
		t.Fatalf("queue yielded %q, %v; want due", got.Kind, ok)
	}
}

func TestEmissionQueueIsBoundedAndDrains(t *testing.T) {
	queue := NewQueue(1)
	if !queue.Offer(Emission{Kind: "kept"}) || queue.Offer(Emission{Kind: "overflow"}) {
		t.Fatal("queue capacity was not enforced")
	}
	dropped := queue.Drain()
	if len(dropped) != 1 || dropped[0].Kind != "kept" || queue.Len() != 0 {
		t.Fatalf("drain = %#v, remaining %d", dropped, queue.Len())
	}
}
