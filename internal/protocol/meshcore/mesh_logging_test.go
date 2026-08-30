package meshcore

import (
	"context"
	"testing"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	meshwire "meshrunner.dev/pkg/meshcore"

	"meshrunner.dev/lotor/internal/bus"
	"meshrunner.dev/lotor/internal/correlation"
	"meshrunner.dev/lotor/internal/logging"
	"meshrunner.dev/lotor/internal/radio"
)

func observedOne(t *testing.T, logs *observer.ObservedLogs, message string) observer.LoggedEntry {
	t.Helper()
	entries := logs.FilterMessage(message).All()
	if len(entries) != 1 {
		t.Fatalf("%q logs = %+v, want exactly one", message, logs.All())
	}
	return entries[0]
}

func TestNominalRelayLogKeepsOneCorrelationChain(t *testing.T) {
	e, dev, sub, peer := txRig(t, "on-air")
	core, observed := observer.New(logging.TraceLevel)
	e.log = zap.New(core)
	runEngine(t, e, dev)
	dev.frames <- peerAdvert(t, peer, time.Now())
	sent := awaitSent(t, sub)

	want := []string{
		"frame heard", "frame judged", "tx queued", "rx window yielded",
		"tx selected", "lbt resolved", "tx handed to radio",
		"tx returned from radio", "frame sent", "tx emission accounted",
	}
	positions := make(map[string]int, len(want))
	for i, entry := range observed.All() {
		if _, tracked := positions[entry.Message]; !tracked {
			positions[entry.Message] = i
		}
	}
	previous := -1
	for _, message := range want {
		position, ok := positions[message]
		if !ok {
			t.Fatalf("missing %q in relay logs: %+v", message, observed.All())
		}
		if position <= previous {
			t.Fatalf("%q logged out of order in %+v", message, observed.All())
		}
		previous = position
	}
	for _, message := range []string{"frame heard", "frame judged", "tx queued", "frame sent"} {
		entry := observedOne(t, observed, message)
		if got := entry.ContextMap()["corr"]; got != sent.Correlation.Short() {
			t.Errorf("%s corr = %v, want %s", message, got, sent.Correlation.Short())
		}
	}
	if reason := observedOne(t, observed, "rx window yielded").ContextMap()["reason"]; reason != "tx-due" {
		t.Errorf("receive window yielded for %v, want tx-due", reason)
	}
}

func TestReceptionLogsKeepTrafficAtDebugAndRFAtTrace(t *testing.T) {
	e, _, _, _ := txRig(t, "shadow") //nolint:dogsled // the rig supplies an armed engine
	core, observed := observer.New(logging.TraceLevel)
	e.log = zap.New(core)
	id := correlation.New()
	frame := radio.Frame{
		Correlation: id,
		Payload:     []byte{1, 2, 3}, At: time.Now(), Airtime: 41 * time.Millisecond,
		RSSI: -91.5, SNR: 7.25, SignalRSSI: -89, FreqErrHz: 123,
	}

	e.heard(frame)

	heard := observedOne(t, observed, "frame heard")
	if heard.Level != zap.DebugLevel {
		t.Errorf("frame heard level = %s, want debug", heard.Level)
	}
	if fields := heard.ContextMap(); fields["corr"] != id.Short() ||
		fields["rssi_dbm"] != nil || fields["snr_db"] != nil {
		t.Errorf("debug traffic log carries RF measurements: %+v", fields)
	}
	measurements := observedOne(t, observed, "rx frame measurements")
	if measurements.Level != logging.TraceLevel {
		t.Errorf("RF measurement level = %s, want trace", measurements.Level)
	}
	fields := measurements.ContextMap()
	if fields["corr"] != id.Short() || fields["rssi_dbm"] != frame.RSSI ||
		fields["snr_db"] != frame.SNR ||
		fields["airtime"] != frame.Airtime {
		t.Errorf("RF measurement fields = %+v", fields)
	}
}

func TestTransmitHandsCorrelationAcrossTheRadioSeam(t *testing.T) {
	e, dev, _, _ := txRig(t, "on-air")
	id := correlation.New()

	if _, err := e.key(context.Background(), dev, []byte{1, 2, 3}, id, zap.NewNop()); err != nil {
		t.Fatal(err)
	}
	if dev.lastCorrelation != id {
		t.Errorf("radio context corr = %s, want %s", dev.lastCorrelation, id)
	}
}

func TestEmissionLogsKeepTrafficAtDebugAndRadioAccountingAtTrace(t *testing.T) {
	e, _, _, _ := txRig(t, "shadow") //nolint:dogsled // the rig supplies duty and stats ledgers
	core, observed := observer.New(logging.TraceLevel)
	e.log = zap.New(core)
	pkt := &meshwire.Packet{
		Header:  meshwire.MakeHeader(meshwire.RouteFlood, meshwire.PayloadTypeAck, meshwire.PayloadVer1),
		Payload: []byte{1, 2, 3, 4},
	}
	id := correlation.New()
	entry := txEntry{pkt: pkt, kind: "test", origin: id}
	sent := bus.FrameSent{
		Correlation: id, Kind: entry.kind, At: time.Now(), Airtime: 37 * time.Millisecond,
		PowerDBm: -5, Shadow: true,
	}
	log := e.log.With(zap.String("corr", id.Short()), zap.String("kind", entry.kind))

	e.recordEmission(entry, sent, log, nil)

	traffic := observedOne(t, observed, "frame sent")
	if traffic.Level != zap.DebugLevel {
		t.Errorf("frame sent level = %s, want debug", traffic.Level)
	}
	if fields := traffic.ContextMap(); fields["airtime"] != nil || fields["power_dbm"] != nil {
		t.Errorf("debug traffic log carries radio accounting: %+v", fields)
	}
	accounted := observedOne(t, observed, "tx emission accounted")
	if accounted.Level != logging.TraceLevel {
		t.Errorf("radio accounting level = %s, want trace", accounted.Level)
	}
	fields := accounted.ContextMap()
	if fields["airtime"] != sent.Airtime || fields["power_dbm"] != sent.PowerDBm {
		t.Errorf("radio accounting fields = %+v", fields)
	}
}

func TestTerminalPolicyDropsStayAtDebug(t *testing.T) {
	e, dev, _, _ := txRig(t, "on-air")
	core, observed := observer.New(logging.TraceLevel)
	e.log = zap.New(core)
	e.duty.SetBudget(time.Nanosecond)
	pkt := &meshwire.Packet{
		Header:  meshwire.MakeHeader(meshwire.RouteFlood, meshwire.PayloadTypeAck, meshwire.PayloadVer1),
		Payload: []byte{1, 2, 3, 4},
	}
	entry := txEntry{pkt: pkt, kind: "test", origin: correlation.New()}

	if e.admitDuty(dev, entry) {
		t.Fatal("an emission larger than the whole duty budget was admitted")
	}
	duty := observedOne(t, observed, "duty budget refuses the emission, dropping")
	if duty.Level != zap.DebugLevel {
		t.Errorf("duty drop level = %s, want debug", duty.Level)
	}

	e.policy.LBTExhausted = "drop"
	if got := e.resolveLBTExhaustion(e.log, entry.origin, entry.kind, 4, time.Second); got != lbtDrop {
		t.Fatalf("LBT drop outcome = %v", got)
	}
	lbt := observedOne(t, observed, "channel busy past the LBT bound, dropping")
	if lbt.Level != zap.DebugLevel {
		t.Errorf("LBT policy drop level = %s, want debug", lbt.Level)
	}
}

func TestDutyDeferralIsAVisibleDebugDecision(t *testing.T) {
	e, dev, _, _ := txRig(t, "on-air")
	core, observed := observer.New(logging.TraceLevel)
	e.log = zap.New(core)
	e.duty.SetBudget(time.Millisecond)
	e.duty.Record(time.Now().Add(-55*time.Minute), time.Millisecond)
	pkt := &meshwire.Packet{
		Header:  meshwire.MakeHeader(meshwire.RouteFlood, meshwire.PayloadTypeAck, meshwire.PayloadVer1),
		Payload: []byte{1, 2, 3, 4},
	}
	entry := txEntry{pkt: pkt, kind: "test", origin: correlation.New()}

	if e.admitDuty(dev, entry) {
		t.Fatal("the full duty budget admitted another emission")
	}
	if len(e.queue.entries) != 1 {
		t.Fatalf("deferred queue length = %d, want 1", len(e.queue.entries))
	}
	deferred := observedOne(t, observed, "tx deferred by duty budget")
	if deferred.Level != zap.DebugLevel {
		t.Errorf("duty deferral level = %s, want debug", deferred.Level)
	}
	fields := deferred.ContextMap()
	if fields["corr"] != entry.origin.Short() || fields["retry_at"] == nil || fields["retry_in"] == nil {
		t.Errorf("duty deferral fields = %+v", fields)
	}
}

func TestForcedTransmissionAfterLBTExhaustionRemainsWarn(t *testing.T) {
	e, _, _, _ := txRig(t, "on-air") //nolint:dogsled // the rig supplies a transmitting policy
	core, observed := observer.New(logging.TraceLevel)
	e.log = zap.New(core)
	e.policy.LBTExhausted = "transmit"

	if got := e.resolveLBTExhaustion(e.log, correlation.New(), "test", 9, 4*time.Second); got != lbtGo {
		t.Fatalf("forced LBT outcome = %v", got)
	}
	forced := observedOne(t, observed, "channel busy past the LBT bound, transmitting anyway")
	if forced.Level != zap.WarnLevel {
		t.Errorf("forced transmission level = %s, want warn", forced.Level)
	}
}

func TestResponseSuppressionAndReplyRoutingAreDebugDecisions(t *testing.T) {
	e, dev, _, _ := txRig(t, "shadow")
	e.queue.depth = 8
	core, observed := observer.New(logging.TraceLevel)
	e.log = zap.New(core)
	id := correlation.New()
	chatOnly := meshwire.AdvTypeFilter(1 << meshwire.AdvTypeChat)
	discover, err := meshwire.BuildDiscoverReq(meshwire.DiscoverReq{Filter: chatOnly, Tag: 7})
	if err != nil {
		t.Fatal(err)
	}
	e.respondDiscover(dev, discover, id, 8)

	suppressed := observedOne(t, observed, "response suppressed")
	if suppressed.Level != zap.DebugLevel {
		t.Errorf("response suppression level = %s, want debug", suppressed.Level)
	}
	if fields := suppressed.ContextMap(); fields["corr"] != id.Short() ||
		fields["request"] != "discover" || fields["reason"] != "filter-miss" {
		t.Errorf("response suppression fields = %+v", fields)
	}

	inbound := &meshwire.Packet{
		Header: meshwire.MakeHeader(meshwire.RouteDirect, meshwire.PayloadTypeReq, meshwire.PayloadVer1),
	}
	e.reply(inbound, answer{
		destHash: make([]byte, meshwire.PathHashSize),
		secret:   make([]byte, meshwire.SharedSecretSize),
		tag:      42,
		body:     []byte{1},
		kind:     "test-response",
	}, id)

	route := observedOne(t, observed, "reply route selected")
	if route.Level != zap.DebugLevel {
		t.Errorf("reply route level = %s, want debug", route.Level)
	}
	fields := route.ContextMap()
	if fields["corr"] != id.Short() || fields["route_source"] != "flood" ||
		fields["route"] != meshwire.RouteFlood.String() {
		t.Errorf("reply route fields = %+v", fields)
	}
	for _, name := range []string{"secret", "password", "body", "payload", "raw"} {
		if _, leaked := fields[name]; leaked {
			t.Errorf("reply route log exposes %q: %+v", name, fields)
		}
	}
}

func TestReplyRouteDebugDistinguishesAllSources(t *testing.T) {
	e, _, _, _ := txRig(t, "shadow") //nolint:dogsled // the rig supplies identity and queue
	e.queue.depth = 8
	core, observed := observer.New(logging.TraceLevel)
	e.log = zap.New(core)
	direct := &meshwire.Packet{
		Header: meshwire.MakeHeader(meshwire.RouteDirect, meshwire.PayloadTypeReq, meshwire.PayloadVer1),
	}
	flood := &meshwire.Packet{
		Header: meshwire.MakeHeader(meshwire.RouteFlood, meshwire.PayloadTypeReq, meshwire.PayloadVer1),
	}
	base := answer{
		destHash: make([]byte, meshwire.PathHashSize),
		secret:   make([]byte, meshwire.SharedSecretSize),
		tag:      42,
		body:     []byte{1},
	}

	cases := []struct {
		name    string
		inbound *meshwire.Packet
		answer  answer
	}{
		{name: "path-return", inbound: flood, answer: base},
		{name: "supplied", inbound: direct, answer: func() answer {
			a := base
			a.supplied, a.pathLen, a.path = true, 1, []byte{0x11}
			a.scope = meshwire.TransportKeyForName("lab")
			return a
		}()},
		{name: "learned", inbound: direct, answer: func() answer {
			a := base
			a.out = &outPath{pathLen: 1, path: []byte{0x22}}
			return a
		}()},
		{name: "flood", inbound: direct, answer: base},
	}
	for _, test := range cases {
		a := test.answer
		a.kind = "route-" + test.name
		e.reply(test.inbound, a, correlation.New())
	}

	entries := observed.FilterMessage("reply route selected").All()
	if len(entries) != len(cases) {
		t.Fatalf("reply route logs = %+v, want %d", entries, len(cases))
	}
	seen := make(map[string]map[string]any, len(entries))
	for _, entry := range entries {
		fields := entry.ContextMap()
		source, ok := fields["route_source"].(string)
		if !ok {
			t.Fatalf("reply route has no string source: %+v", fields)
		}
		seen[source] = fields
	}
	for _, source := range []string{"path-return", "supplied", "learned", "flood"} {
		if seen[source] == nil {
			t.Errorf("route source %q missing from %+v", source, seen)
		}
	}
	if scoped, ok := seen["supplied"]["scoped"].(bool); !ok || !scoped {
		t.Errorf("scoped supplied route = %+v", seen["supplied"])
	}
}

func TestDebugLoggerDoesNotReceiveTraceOnlyMeasurements(t *testing.T) {
	e, _, _, _ := txRig(t, "shadow") //nolint:dogsled // the rig supplies an armed engine
	core, observed := observer.New(zapcore.DebugLevel)
	e.log = zap.New(core)
	e.heard(radio.Frame{Payload: []byte{1}, RSSI: -90, SNR: 5})

	observedOne(t, observed, "frame heard")
	if entries := observed.FilterMessage("rx frame measurements").All(); len(entries) != 0 {
		t.Fatalf("debug logger received trace measurements: %+v", entries)
	}
}
