package origin

import (
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"meshrunner.dev/lotor/internal/bus"
)

func TestDropReasonKeepsItsStructuredLogLabel(t *testing.T) {
	core, logs := observer.New(zap.DebugLevel)
	p := New(Config{Log: zap.New(core)}, 1)
	item := emission("answer")
	out := p.Drop(item, bus.DropQueueFull)
	if out.Dropped != bus.DropQueueFull {
		t.Fatalf("drop outcome=%+v", out)
	}
	entries := logs.FilterMessage("frame dropped").All()
	if len(entries) != 1 {
		t.Fatalf("got %d drop logs, want one", len(entries))
	}
	fields := entries[0].ContextMap()
	if fields["reason"] != "queue-full" || fields["kind"] != "answer" || fields["corr"] != item.Correlation.Short() {
		t.Fatalf("drop log changed its textual fields: %+v", fields)
	}
}
