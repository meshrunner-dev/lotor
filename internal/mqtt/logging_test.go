package mqtt

import (
	"testing"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	zapobserver "go.uber.org/zap/zaptest/observer"

	"meshrunner.dev/lotor/internal/bus"
	"meshrunner.dev/lotor/internal/logging"
	"meshrunner.dev/lotor/internal/txn"
)

func mqttObservedOne(t *testing.T, logs *zapobserver.ObservedLogs, message string) zapobserver.LoggedEntry {
	t.Helper()
	entries := logs.FilterMessage(message).All()
	if len(entries) != 1 {
		t.Fatalf("%q logs = %+v, want exactly one", message, logs.All())
	}
	return entries[0]
}

func TestObserverLogsTrafficAtDebugAndBrokerCompletionAtTrace(t *testing.T) {
	raw, _ := advertFrame(t)
	core, observed := zapobserver.New(logging.TraceLevel)
	rec := &recorder{}
	id := txn.New()
	o := New(Config{
		Instance: "paris", Relay: "mc", IATA: "PAR", Topic: DefaultTopic,
		RX: true, Packets: true, OriginID: "feed",
	}, rec, zap.New(core).With(zap.String("observer", "paris")))

	o.event(bus.FrameHeard{Relay: "mc", Txn: id, At: time.Now(), Raw: raw})

	traffic := mqttObservedOne(t, observed, "observer frame selected")
	if traffic.Level != zap.DebugLevel {
		t.Errorf("traffic level = %s, want debug", traffic.Level)
	}
	trafficFields := traffic.ContextMap()
	if trafficFields["direction"] != "rx" || trafficFields["packet_type"] != "ADVERT" {
		t.Errorf("traffic fields = %+v", trafficFields)
	}
	if trafficFields["payload"] != nil || trafficFields["raw"] != nil {
		t.Errorf("traffic log exposes message content: %+v", trafficFields)
	}

	completed := mqttObservedOne(t, observed, "observer broker publish completed")
	if completed.Level != logging.TraceLevel {
		t.Errorf("broker completion level = %s, want trace", completed.Level)
	}
	fields := completed.ContextMap()
	if fields["observer"] != "paris" || fields["class"] != TopicPackets ||
		fields["topic"] != "meshcore/PAR/feed/packets" || fields["qos"] != uint8(0) ||
		fields["retain"] != false || fields["success"] != true {
		t.Errorf("broker completion fields = %+v", fields)
	}
	if fields["payload_bytes"] == nil || fields["elapsed"] == nil {
		t.Errorf("broker completion lacks size or timing: %+v", fields)
	}
	if fields["payload"] != nil || fields["raw"] != nil {
		t.Errorf("broker completion exposes message content: %+v", fields)
	}
	for _, entry := range observed.All() {
		if got := entry.ContextMap()["txn"]; got != id.Short() {
			t.Errorf("%q txn = %v, want %s", entry.Message, got, id.Short())
		}
	}
}

func TestFailedPublicationDoesNotClaimBrokerCompletion(t *testing.T) {
	raw, _ := advertFrame(t)
	core, observed := zapobserver.New(logging.TraceLevel)
	rec := &recorder{fail: true}
	id := txn.New()
	o := New(Config{
		Relay: "mc", IATA: "PAR", Topic: DefaultTopic,
		RX: true, Packets: true, OriginID: "feed",
	}, rec, zap.New(core))

	o.event(bus.FrameHeard{Relay: "mc", Txn: id, At: time.Now(), Raw: raw})

	if got := observed.FilterMessage("observer broker publish completed").Len(); got != 0 {
		t.Fatalf("broker completion logs = %d, want none after failure", got)
	}
	failed := mqttObservedOne(t, observed, "observer publish failed")
	if failed.Level != zap.DebugLevel {
		t.Errorf("publish failure level = %s, want debug", failed.Level)
	}
	if got := failed.ContextMap()["txn"]; got != id.Short() {
		t.Errorf("publish failure txn = %v, want %s", got, id.Short())
	}
}

func TestDebugLevelHidesBrokerCompletionDetail(t *testing.T) {
	raw, _ := advertFrame(t)
	core, observed := zapobserver.New(zapcore.DebugLevel)
	id := txn.New()
	o := New(Config{
		Relay: "mc", IATA: "PAR", Topic: DefaultTopic,
		RX: true, Packets: true, OriginID: "feed",
	}, &recorder{}, zap.New(core))

	o.event(bus.FrameHeard{Relay: "mc", Txn: id, At: time.Now(), Raw: raw})

	if observed.FilterMessage("observer frame selected").Len() != 1 {
		t.Fatal("debug traffic log is missing")
	}
	if got := observed.FilterMessage("observer broker publish completed").Len(); got != 0 {
		t.Fatalf("trace broker completion logs at debug = %d, want none", got)
	}
	selected := mqttObservedOne(t, observed, "observer frame selected")
	if got := selected.ContextMap()["txn"]; got != id.Short() {
		t.Errorf("selected frame txn = %v, want %s", got, id.Short())
	}
}

func TestObserverFrameDecisionsNeverLoseTheirTransaction(t *testing.T) {
	raw, _ := advertFrame(t)
	tests := []struct {
		name  string
		cfg   Config
		event func(txn.ID) bus.Event
	}{
		{
			name: "rx disabled",
			cfg:  Config{Relay: "mc"},
			event: func(id txn.ID) bus.Event {
				return bus.FrameHeard{Relay: "mc", Txn: id, Raw: raw}
			},
		},
		{
			name: "empty rx",
			cfg:  Config{Relay: "mc", RX: true},
			event: func(id txn.ID) bus.Event {
				return bus.FrameHeard{Relay: "mc", Txn: id}
			},
		},
		{
			name: "invalid rx",
			cfg:  Config{Relay: "mc", RX: true, Packets: true},
			event: func(id txn.ID) bus.Event {
				return bus.FrameHeard{Relay: "mc", Txn: id, Raw: []byte{1}}
			},
		},
		{
			name: "type filtered",
			cfg:  Config{Relay: "mc", RX: true, Packets: true, Types: []string{"REQ"}},
			event: func(id txn.ID) bus.Event {
				return bus.FrameHeard{Relay: "mc", Txn: id, Raw: raw}
			},
		},
		{
			name: "shadow tx",
			cfg:  Config{Relay: "mc", TX: TXAll, Packets: true},
			event: func(id txn.ID) bus.Event {
				return bus.FrameSent{Relay: "mc", Txn: id, Raw: raw, Shadow: true}
			},
		},
		{
			name: "tx disabled",
			cfg:  Config{Relay: "mc", Packets: true},
			event: func(id txn.ID) bus.Event {
				return bus.FrameSent{Relay: "mc", Txn: id, Raw: raw}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			core, observed := zapobserver.New(logging.TraceLevel)
			o := New(test.cfg, &recorder{}, zap.New(core))
			id := txn.New()

			o.event(test.event(id))

			entries := observed.All()
			if len(entries) == 0 {
				t.Fatal("frame decision was not logged")
			}
			for _, entry := range entries {
				if got := entry.ContextMap()["txn"]; got != id.Short() {
					t.Errorf("%q txn = %v, want %s", entry.Message, got, id.Short())
				}
			}
		})
	}
}
