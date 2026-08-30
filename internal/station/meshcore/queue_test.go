package meshcore

import (
	"testing"
	"time"
)

func TestEmissionQueueOrdersDuePacketsByReferencePriority(t *testing.T) {
	queue := newEmissionQueue(4)
	if !queue.offer(emission{kind: "low", priority: 5}) ||
		!queue.offer(emission{kind: "first-high", priority: 1}) ||
		!queue.offer(emission{kind: "second-high", priority: 1}) {
		t.Fatal("queue refused an emission below capacity")
	}
	for _, want := range []string{"first-high", "second-high", "low"} {
		got, ok := queue.takeUntil(t.Context(), time.Now().Add(time.Second))
		if !ok || got.kind != want {
			t.Fatalf("queue yielded %q, %v; want %q", got.kind, ok, want)
		}
	}
}

func TestFutureHighPriorityDoesNotBlockDuePacket(t *testing.T) {
	queue := newEmissionQueue(2)
	_ = queue.offer(emission{kind: "future", priority: 0, notBefore: time.Now().Add(time.Hour)})
	_ = queue.offer(emission{kind: "due", priority: 5})
	got, ok := queue.takeUntil(t.Context(), time.Now().Add(time.Second))
	if !ok || got.kind != "due" {
		t.Fatalf("queue yielded %q, %v; want due", got.kind, ok)
	}
}

func TestEmissionQueueIsBoundedAndDrains(t *testing.T) {
	queue := newEmissionQueue(1)
	if !queue.offer(emission{kind: "kept"}) || queue.offer(emission{kind: "overflow"}) {
		t.Fatal("queue capacity was not enforced")
	}
	dropped := queue.drain()
	if len(dropped) != 1 || dropped[0].kind != "kept" || queue.len() != 0 {
		t.Fatalf("drain = %#v, remaining %d", dropped, queue.len())
	}
}
