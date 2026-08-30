package meshcore

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"meshrunner.dev/lotor/internal/correlation"
	"meshrunner.dev/lotor/internal/logging"
	"meshrunner.dev/lotor/internal/meshcorecfg"
	"meshrunner.dev/lotor/internal/product"
	"meshrunner.dev/lotor/internal/radio"
	"meshrunner.dev/lotor/internal/station"
	"meshrunner.dev/lotor/internal/version"

	mesh "meshrunner.dev/pkg/meshcore"
	"meshrunner.dev/pkg/meshcore/companion"
)

func testSpec(t *testing.T) station.Spec {
	t.Helper()
	cfg := meshcorecfg.Presets()["eu-868-narrow"]
	cfg["identity"] = hex.EncodeToString(bytes.Repeat([]byte{1}, 32))
	cfg["node_name"] = "Alice"
	cfg["tx_power_dbm"] = 14
	return station.Spec{
		Name: "alice", Protocol: protocolName, Listen: "127.0.0.1:0", Config: cfg,
		Log: zap.NewNop(), Build: version.Info{
			Version: "1.2.3", SourceTime: time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC),
		},
	}
}

func TestDetachedStationAnswersStartupProtocol(t *testing.T) {
	built, err := build(testSpec(t))
	if err != nil {
		t.Fatal(err)
	}
	svc := requireService(t, built)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- svc.Run(ctx) }()
	addr := awaitListener(t, svc)

	conn, err := (&net.Dialer{}).DialContext(t.Context(), "tcp", addr)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()

	query := companion.DeviceQuery{TargetVersion: protocolVersion}
	got := exchange(t, conn, query)
	want, err := companion.MarshalResponse(companion.DeviceInfo{
		ProtocolVersion: protocolVersion, MaxContacts: defaultContacts,
		MaxChannels: defaultChannels, BuildDate: "30 Aug 2026",
		Model: "Lotor Virtual Station", FirmwareVersion: "1.2.3",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("device info = % X, want % X", got, want)
	}

	got = exchange(t, conn, companion.AppStart{Name: "test"})
	want, err = companion.MarshalResponse(svc.selfInfo())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("self info = % X, want % X", got, want)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run shutdown: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("station did not stop")
	}
}

func TestChannelEnumerationExposesEmptySlotsBeforeSet(t *testing.T) {
	built, err := build(testSpec(t))
	if err != nil {
		t.Fatal(err)
	}
	svc := requireService(t, built)

	responses := svc.handle(t.Context(), companion.GetChannel{Index: 0})
	if len(responses) != 1 {
		t.Fatalf("public channel response = %#v", responses)
	}
	public, ok := responses[0].(companion.ChannelInfo)
	if !ok {
		t.Fatalf("public channel response = %#v", responses)
	}
	wantPublic := mesh.NewPublicChannel()
	if public.Index != 0 || public.Name != "Public" || !bytes.Equal(public.Secret[:], wantPublic.Secret[:16]) {
		t.Fatalf("public channel = %+v", public)
	}

	responses = svc.handle(t.Context(), companion.GetChannel{Index: 1})
	if len(responses) != 1 {
		t.Fatalf("empty channel response = %#v", responses)
	}
	empty, ok := responses[0].(companion.ChannelInfo)
	if !ok || empty.Index != 1 || empty.Name != "" || empty.Secret != ([16]byte{}) {
		t.Fatalf("empty channel response = %#v", responses)
	}

	secret := [16]byte{1, 2, 3}
	responses = svc.handle(t.Context(), companion.SetChannel{Index: 1, Name: "#szer", Secret: secret})
	if len(responses) != 1 || responses[0] != companion.StatusResponse(companion.ResponseOK) {
		t.Fatalf("set channel response = %#v", responses)
	}
	responses = svc.handle(t.Context(), companion.GetChannel{Index: 1})
	if len(responses) != 1 {
		t.Fatalf("stored channel response = %#v", responses)
	}
	got, ok := responses[0].(companion.ChannelInfo)
	if !ok || got.Name != "#szer" || got.Secret != secret {
		t.Fatalf("stored channel response = %#v", responses)
	}

	responses = svc.handle(t.Context(), companion.GetChannel{Index: uint8(svc.p.MaxChannels)})
	if len(responses) != 1 || responses[0] != (companion.ErrorResponse{Code: companion.ErrNotFound}) {
		t.Fatalf("out-of-range channel response = %#v", responses)
	}
}

func TestChannelCommandTraceIncludesSlotAndCapacityWithoutSecret(t *testing.T) {
	built, err := build(testSpec(t))
	if err != nil {
		t.Fatal(err)
	}
	svc := requireService(t, built)
	secret := [16]byte{1, 2, 3}
	fields := svc.companionCommandFields(companion.SetChannel{
		Index: 1, Name: "#szer", Secret: secret,
	})
	core, observed := observer.New(logging.TraceLevel)
	logging.Trace(zap.New(core), "command", fields...)
	entries := observed.All()
	if len(entries) != 1 {
		t.Fatalf("command traces = %#v", entries)
	}
	got := entries[0].ContextMap()
	if got["command"] != "SetChannel" || got["channel"] != uint8(1) ||
		got["channel_capacity"] != int64(defaultChannels) {
		t.Fatalf("command fields = %#v", got)
	}
	if rendered := fmt.Sprint(got); strings.Contains(rendered, "#szer") ||
		strings.Contains(rendered, fmt.Sprint(secret)) {
		t.Fatalf("command trace exposed channel material: %s", rendered)
	}
}

func TestAttachedStationUsesPhysicalPowerEnvelopeWhileRadioIsDown(t *testing.T) {
	built, err := build(testSpec(t))
	if err != nil {
		t.Fatal(err)
	}
	svc := requireService(t, built)
	envelope := radio.Envelope{
		MaxTxPowerSet: true, MaxTxPowerDBm: 10,
		ChipMinDBm: -9, ChipMaxDBm: 22,
		FreqRangeLowHz: 863_000_000, FreqRangeHiHz: 870_000_000,
	}
	driver := radio.Driver{
		Inspect: func(map[string]any) (radio.Envelope, error) { return envelope, nil },
		Open: func(map[string]any, *zap.Logger) (radio.Device, error) {
			return &stationRadio{}, nil
		},
	}
	controller, err := radio.NewController("slot1", driver, nil, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	binding, err := controller.Bind("alice", radio.RoleStation, svc.p.Waveform)
	if err != nil {
		t.Fatal(err)
	}
	svc.AttachRadio("slot1", binding, nil, "radio unavailable")

	if got := svc.selfInfo().MaxTXPowerDBm; got != envelope.MaxTxPowerDBm {
		t.Fatalf("self info max power = %d, want %d", got, envelope.MaxTxPowerDBm)
	}
	if got := svc.handle(t.Context(), companion.SetRadioTXPower{PowerDBm: 11}); len(got) != 1 || got[0] != (companion.ErrorResponse{Code: companion.ErrIllegalArgument}) {
		t.Fatalf("power above physical envelope = %#v", got)
	}
	if got := svc.handle(t.Context(), companion.SetRadioTXPower{PowerDBm: 10}); len(got) != 1 || got[0] != companion.StatusResponse(companion.ResponseOK) {
		t.Fatalf("power at physical envelope = %#v", got)
	}
}

func TestNewCompanionClientReplacesThePreviousOne(t *testing.T) {
	built, err := build(testSpec(t))
	if err != nil {
		t.Fatal(err)
	}
	svc := requireService(t, built)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go func() { _ = svc.Run(ctx) }()
	addr := awaitListener(t, svc)

	first, err := (&net.Dialer{}).DialContext(t.Context(), "tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = first.Close() }()
	_ = exchange(t, first, companion.DeviceQuery{TargetVersion: protocolVersion})

	second, err := (&net.Dialer{}).DialContext(t.Context(), "tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = second.Close() }()
	_ = exchange(t, second, companion.DeviceQuery{TargetVersion: protocolVersion})

	if err := first.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	var one [1]byte
	if _, err := first.Read(one[:]); err == nil {
		t.Fatal("the first client remained connected after its replacement")
	}
	info := svc.Info()
	if !info.Connected || info.Remote != second.LocalAddr().String() {
		t.Fatalf("active client = %+v", info)
	}
}

func TestCompanionReplacementStartsANewApplicationSession(t *testing.T) {
	built, err := build(testSpec(t))
	if err != nil {
		t.Fatal(err)
	}
	svc := requireService(t, built)
	firstStation, firstApp := net.Pipe()
	defer func() {
		_ = firstStation.Close()
		_ = firstApp.Close()
	}()
	svc.replaceClient(firstStation)
	svc.mu.Lock()
	svc.appVersion = protocolVersion
	svc.sendUnscoped = true
	svc.sendScope[0] = 1
	svc.signData = []byte("partial signature input")
	svc.pending = pendingRequest{kind: pendingStatus, tag: 42}
	svc.expectedACKs[0] = ackExpectation{crc: 7, used: true}
	svc.connections[[mesh.PubKeySize]byte{1}] = remoteConnection{keepAlive: time.Minute}
	svc.contacts[[mesh.PubKeySize]byte{2}] = contactEntry{ephemeral: true}
	svc.advertPaths[0].pathLen = 1
	svc.advertPaths[0].path[0] = 1
	svc.mu.Unlock()

	secondStation, secondApp := net.Pipe()
	defer func() {
		_ = secondStation.Close()
		_ = secondApp.Close()
	}()
	svc.replaceClient(secondStation)
	svc.mu.Lock()
	defer svc.mu.Unlock()
	if svc.appVersion != 0 || svc.sendUnscoped || svc.sendScope != ([16]byte{}) ||
		svc.signData != nil || svc.pending.kind != pendingNone || svc.expectedACKs[0].used ||
		len(svc.connections) != 0 || svc.advertPaths[0] != (advertPath{}) {
		t.Fatalf("application session state crossed replacement: app=%d scope=%x sign=%q pending=%+v",
			svc.appVersion, svc.sendScope, svc.signData, svc.pending)
	}
	if _, exists := svc.contacts[[mesh.PubKeySize]byte{2}]; exists {
		t.Fatal("ephemeral contact crossed application replacement")
	}
}

func TestSilentCompanionCannotBlockRFPushProducer(t *testing.T) {
	built, err := build(testSpec(t))
	if err != nil {
		t.Fatal(err)
	}
	svc := requireService(t, built)
	stationSide, appSide := net.Pipe()
	defer func() {
		_ = stationSide.Close()
		_ = appSide.Close()
	}()
	svc.replaceClient(stationSide)
	writerCtx, cancelWriter := context.WithCancel(t.Context())
	defer cancelWriter()
	go svc.runPushes(writerCtx)

	done := make(chan struct{})
	go func() {
		for range pushQueueDepth * 2 {
			svc.push(companion.MessagesWaiting{})
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("silent companion blocked the RF push producer")
	}
}

func TestIdentitySnapshotIsSafeDuringCompanionImport(t *testing.T) {
	built, err := build(testSpec(t))
	if err != nil {
		t.Fatal(err)
	}
	svc := requireService(t, built)
	identity, err := mesh.LocalIdentityFromSeed(bytes.Repeat([]byte{19}, mesh.SeedSize))
	if err != nil {
		t.Fatal(err)
	}
	command := companion.ImportPrivateKey{}
	copy(command.PrivateKey[:], identity.PrvKey())
	start := make(chan struct{})
	done := make(chan struct{}, 2)
	go func() {
		<-start
		for range 1_000 {
			_ = svc.identitySnapshot().PubKey
		}
		done <- struct{}{}
	}()
	go func() {
		<-start
		for range 100 {
			_ = svc.handle(t.Context(), command)
		}
		done <- struct{}{}
	}()
	close(start)
	<-done
	<-done
}

func TestDetachedStationForbidsRepeatingAndKeepsRadioPreferences(t *testing.T) {
	built, err := build(testSpec(t))
	if err != nil {
		t.Fatal(err)
	}
	svc := requireService(t, built)
	responses := svc.handle(t.Context(), companion.SetRadioParams{Repeat: true})
	wire, _ := companion.MarshalResponse(responses[0])
	want, _ := companion.MarshalResponse(companion.ErrorResponse{Code: companion.ErrIllegalArgument})
	if !bytes.Equal(wire, want) {
		t.Fatalf("repeat response = % X, want % X", wire, want)
	}
	responses = svc.handle(t.Context(), companion.SetRadioParams{
		FrequencyKHz: 869_525, BandwidthHz: 125_000, Spreading: 9, CodingRate: 5,
	})
	wire, _ = companion.MarshalResponse(responses[0])
	want, _ = companion.MarshalResponse(companion.StatusResponse(companion.ResponseOK))
	if !bytes.Equal(wire, want) {
		t.Fatalf("detached preference response = % X, want % X", wire, want)
	}
	if svc.p.FrequencyHz != 869_525_000 || svc.p.BandwidthHz != 125_000 ||
		svc.p.SpreadingFactor != 9 || svc.p.CodingRate != 5 {
		t.Fatalf("radio preferences were not retained: %+v", svc.p.Waveform)
	}
}

func TestDeviceTimeUpdateAcknowledgesLikeTheReference(t *testing.T) {
	built, err := build(testSpec(t))
	if err != nil {
		t.Fatal(err)
	}
	svc := requireService(t, built)
	responses := svc.handle(t.Context(), companion.SetDeviceTime{
		UnixSeconds: uint32(time.Now().Add(time.Minute).Unix()),
	})
	if len(responses) != 1 || responses[0] != companion.StatusResponse(companion.ResponseOK) {
		t.Fatalf("set device time responses = %#v", responses)
	}
}

func TestAutoAddFlagsAlonePreserveTheHopLimit(t *testing.T) {
	built, err := build(testSpec(t))
	if err != nil {
		t.Fatal(err)
	}
	svc := requireService(t, built)
	_ = svc.handle(t.Context(), companion.SetAutoAddConfig{
		Flags: 2, MaxHops: 4, HasMaxHops: true,
	})
	responses := svc.handle(t.Context(), companion.SetAutoAddConfig{Flags: 4})
	if len(responses) != 1 || responses[0] != companion.StatusResponse(companion.ResponseOK) ||
		svc.autoFlags != 4 || svc.autoHops != 4 {
		t.Fatalf("auto-add update = %#v flags=%d hops=%d", responses, svc.autoFlags, svc.autoHops)
	}
	_ = svc.handle(t.Context(), companion.SetAutoAddConfig{Flags: 8, HasMaxHops: true})
	if svc.autoHops != 0 {
		t.Fatalf("explicit zero max hops remained %d", svc.autoHops)
	}
}

func TestStationConfigurationMutationsLogAtDebugWithoutValues(t *testing.T) {
	core, observed := observer.New(logging.TraceLevel)
	spec := testSpec(t)
	spec.Log = zap.New(core)
	built, err := build(spec)
	if err != nil {
		t.Fatal(err)
	}
	svc := requireService(t, built)
	responses := svc.handle(t.Context(), companion.SetAdvertName{Name: "Private operator label"})
	if len(responses) != 1 || responses[0] != companion.StatusResponse(companion.ResponseOK) {
		t.Fatalf("set name responses = %#v", responses)
	}
	entries := observed.FilterMessage("station configuration changed").All()
	if len(entries) != 1 || entries[0].Level != zapcore.DebugLevel {
		t.Fatalf("configuration logs = %#v", entries)
	}
	fields := entries[0].ContextMap()
	if fields["source"] != "companion" || fields["command"] != "SetAdvertName" {
		t.Fatalf("configuration fields = %#v", fields)
	}
	if fmt.Sprint(fields) == "" || strings.Contains(fmt.Sprint(fields), "Private operator label") {
		t.Fatalf("configuration log exposed the value: %#v", fields)
	}
}

func TestMailboxTraceCoversEvictionRefusalAndDurableCorrelation(t *testing.T) {
	core, observed := observer.New(logging.TraceLevel)
	store := &memoryStationState{}
	spec := testSpec(t)
	spec.State = store
	spec.Log = zap.New(core)
	spec.Config["mailbox_capacity"] = 1
	built, err := build(spec)
	if err != nil {
		t.Fatal(err)
	}
	svc := requireService(t, built)
	corr := correlation.ID{1, 2, 3, 4, 5, 6}
	ctx := correlation.WithContext(t.Context(), corr)
	svc.enqueueMailbox(ctx, companion.ChannelData{Channel: 1, DataType: 1, Data: []byte{1}})
	svc.enqueueMailbox(ctx, companion.ContactMessageV3{Text: "private"})
	svc.enqueueMailbox(ctx, companion.ContactMessageV3{Text: "refused"})

	enqueued := observed.FilterMessage("station mailbox item enqueued").All()
	if len(enqueued) != 2 || enqueued[0].Level != logging.TraceLevel ||
		enqueued[1].ContextMap()["evicted_channel"] != true {
		t.Fatalf("mailbox enqueue logs = %#v", enqueued)
	}
	refused := observed.FilterMessage("station mailbox item refused").All()
	if len(refused) != 1 || refused[0].Level != logging.TraceLevel ||
		refused[0].ContextMap()["reason"] != "full" {
		t.Fatalf("mailbox refusal logs = %#v", refused)
	}
	if got := enqueued[1].ContextMap()["corr"]; got != corr.Short() {
		t.Fatalf("enqueue correlation = %v, want %s", got, corr.Short())
	}

	built, err = build(spec)
	if err != nil {
		t.Fatal(err)
	}
	restored := requireService(t, built)
	responses := restored.handle(t.Context(), companion.SimpleCommand{Kind: companion.CommandSyncNextMessage})
	if len(responses) != 1 {
		t.Fatalf("sync responses = %#v", responses)
	}
	delivered := observed.FilterMessage("station mailbox delivered").All()
	if len(delivered) != 1 || delivered[0].Level != logging.TraceLevel ||
		delivered[0].ContextMap()["corr"] != corr.Short() {
		t.Fatalf("mailbox delivery logs = %#v", delivered)
	}
}

func TestStationStateWithoutMailboxCorrelationsStillRestores(t *testing.T) {
	store := &memoryStationState{}
	spec := testSpec(t)
	spec.State = store
	built, err := build(spec)
	if err != nil {
		t.Fatal(err)
	}
	requireService(t, built).enqueueMailbox(t.Context(), companion.ChannelData{
		Channel: 1, DataType: 1, Data: []byte{1},
	})
	var legacy map[string]json.RawMessage
	if err := json.Unmarshal(store.state, &legacy); err != nil {
		t.Fatal(err)
	}
	delete(legacy, "mailboxCorrelations")
	store.state, err = json.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	built, err = build(spec)
	if err != nil {
		t.Fatal(err)
	}
	restored := requireService(t, built)
	if len(restored.mailbox) != 1 || len(restored.mailboxCorr) != 1 || !restored.mailboxCorr[0].IsZero() {
		t.Fatalf("legacy mailbox restoration = messages %d correlations %#v",
			len(restored.mailbox), restored.mailboxCorr)
	}
}

type memoryStationState struct {
	state []byte
	fail  bool
}

type blockingStationState struct {
	started chan struct{}
	release chan struct{}
}

func (*blockingStationState) LoadStationState(context.Context, string) ([]byte, bool, error) {
	return nil, false, nil
}

func (b *blockingStationState) SaveStationState(context.Context, string, []byte) error {
	close(b.started)
	<-b.release
	return nil
}

func TestSlowStationPersistenceDoesNotHoldRuntimeStateLock(t *testing.T) {
	store := &blockingStationState{started: make(chan struct{}), release: make(chan struct{})}
	spec := testSpec(t)
	spec.State = store
	built, err := build(spec)
	if err != nil {
		t.Fatal(err)
	}
	svc := requireService(t, built)
	mutationDone := make(chan []companion.Response, 1)
	go func() {
		mutationDone <- svc.handle(t.Context(), companion.SetAdvertName{Name: "Persisting"})
	}()
	select {
	case <-store.started:
	case <-time.After(time.Second):
		t.Fatal("station persistence did not start")
	}

	runtimeDone := make(chan struct{})
	go func() {
		_ = svc.Info()
		svc.AttachRadio("", nil, nil, "")
		close(runtimeDone)
	}()
	select {
	case <-runtimeDone:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("slow station persistence retained the runtime state lock")
	}
	close(store.release)
	responses := <-mutationDone
	if len(responses) != 1 || responses[0] != companion.StatusResponse(companion.ResponseOK) {
		t.Fatalf("persisted mutation responses = %#v", responses)
	}
}

func (m *memoryStationState) LoadStationState(context.Context, string) ([]byte, bool, error) {
	if len(m.state) == 0 {
		return nil, false, nil
	}
	return append([]byte(nil), m.state...), true, nil
}

func (m *memoryStationState) SaveStationState(_ context.Context, _ string, state []byte) error {
	if m.fail {
		return errors.New("disk full")
	}
	m.state = append([]byte(nil), state...)
	return nil
}

func TestCompanionPreferencesSurviveStationRestart(t *testing.T) {
	store := &memoryStationState{}
	spec := testSpec(t)
	spec.State = store
	built, err := build(spec)
	if err != nil {
		t.Fatal(err)
	}
	first := requireService(t, built)
	if got := first.handle(t.Context(), companion.SetAdvertName{Name: "Persisted"}); len(got) != 1 {
		t.Fatalf("set name responses = %#v", got)
	}
	if got := first.handle(t.Context(), companion.SetRadioParams{
		FrequencyKHz: 869_525, BandwidthHz: 125_000, Spreading: 9, CodingRate: 5,
	}); len(got) != 1 {
		t.Fatalf("set waveform responses = %#v", got)
	}
	secret := [16]byte{1, 2, 3}
	_ = first.handle(t.Context(), companion.SetChannel{Index: 2, Name: "ops", Secret: secret})
	_ = first.handle(t.Context(), companion.SetDefaultFloodScope{Name: "fr", Key: [16]byte{9}})
	_ = first.handle(t.Context(), companion.SetTuningParams{
		RXDelayMilli: 2_500, AirtimeFactorMilli: 1_250,
	})
	if got := first.handle(t.Context(), companion.SetDevicePIN{PIN: 1234}); len(got) != 1 || got[0] != (companion.ErrorResponse{Code: companion.ErrIllegalArgument}) {
		t.Fatalf("invalid PIN response = %#v", got)
	}
	_ = first.handle(t.Context(), companion.SetDevicePIN{PIN: 123456})
	_ = first.handle(t.Context(), companion.SetOtherParams{
		ManualContacts: true, TelemetryMode: 0xff, AdvertLocPolicy: 2, MultiACKs: 7,
		HasTelemetry: true, HasAdvertLoc: true, HasMultiACKs: true,
	})

	built, err = build(spec)
	if err != nil {
		t.Fatal(err)
	}
	second := requireService(t, built)
	if second.p.NodeName != "Persisted" || second.RadioDemand().Waveform.BandwidthHz != 125_000 ||
		second.p.SpreadingFactor != 9 || second.channels[2].name != "ops" ||
		second.channels[2].secret != secret || second.defaultScope != "fr" || second.defaultKey[0] != 9 ||
		second.p.RXDelayMilli != 2_500 || second.p.AirFactorMilli != 1_250 || second.p.PIN != 123456 ||
		!second.p.ManualContacts || second.p.TelemetryMode != 0x3f || second.p.AdvertLoc != 2 ||
		second.p.MultiACKs != 7 {
		t.Fatalf("restored service = params %+v channel %+v scope %q/%x",
			second.p, second.channels[2], second.defaultScope, second.defaultKey)
	}
	responses := second.handle(t.Context(), companion.SimpleCommand{Kind: companion.CommandGetTuningParams})
	if len(responses) != 1 || responses[0] != (companion.TuningParams{
		RXDelayMilli: 2_500, AirtimeFactorMilli: 1_250,
	}) {
		t.Fatalf("restored tuning response = %#v", responses)
	}
}

func TestPreferenceWriteFailureRollsBackAndReportsFileIO(t *testing.T) {
	store := &memoryStationState{fail: true}
	spec := testSpec(t)
	spec.State = store
	built, err := build(spec)
	if err != nil {
		t.Fatal(err)
	}
	svc := requireService(t, built)
	responses := svc.handle(t.Context(), companion.SetAdvertName{Name: "Lost"})
	wire, _ := companion.MarshalResponse(responses[0])
	want, _ := companion.MarshalResponse(companion.ErrorResponse{Code: companion.ErrFileIO})
	if !bytes.Equal(wire, want) || svc.p.NodeName != "Alice" {
		t.Fatalf("failed save = % X name %q, want % X and rollback", wire, svc.p.NodeName, want)
	}
}

func TestRebootDisconnectsTheCompanionWithoutLosingDurableState(t *testing.T) {
	store := &memoryStationState{}
	spec := testSpec(t)
	spec.State = store
	built, err := build(spec)
	if err != nil {
		t.Fatal(err)
	}
	svc := requireService(t, built)
	if got := svc.handle(t.Context(), companion.SetAdvertName{Name: "Persisted"}); len(got) != 1 || got[0] != companion.StatusResponse(companion.ResponseOK) {
		t.Fatalf("set name responses = %#v", got)
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go func() { _ = svc.Run(ctx) }()
	conn, err := (&net.Dialer{}).DialContext(t.Context(), "tcp", awaitListener(t, svc))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	payload, err := companion.MarshalCommand(companion.Reboot{})
	if err != nil {
		t.Fatal(err)
	}
	if err := companion.WriteFrame(conn, companion.ToDevice, payload); err != nil {
		t.Fatal(err)
	}
	if err := conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := companion.ReadFrame(conn, companion.ToApplication); err == nil {
		t.Fatal("reboot returned a response or kept the companion session open")
	}
	if svc.p.NodeName != "Persisted" {
		t.Fatalf("reboot reset durable name to %q", svc.p.NodeName)
	}
}

func TestFactoryResetRestoresConfiguredStateAndPersistsIt(t *testing.T) {
	store := &memoryStationState{}
	spec := testSpec(t)
	spec.State = store
	built, err := build(spec)
	if err != nil {
		t.Fatal(err)
	}
	svc := requireService(t, built)
	configuredKey := svc.id.PubKey
	configuredWaveform := svc.p.Waveform

	_ = svc.handle(t.Context(), companion.SetAdvertName{Name: "Changed"})
	_ = svc.handle(t.Context(), companion.SetRadioParams{
		FrequencyKHz: 869_525, BandwidthHz: 125_000, Spreading: 9, CodingRate: 5,
	})
	_ = svc.handle(t.Context(), companion.SetChannel{Index: 2, Name: "ops", Secret: [16]byte{1}})
	_ = svc.handle(t.Context(), companion.SetDefaultFloodScope{Name: "fr", Key: [16]byte{2}})
	imported, err := mesh.LocalIdentityFromSeed(bytes.Repeat([]byte{17}, mesh.SeedSize))
	if err != nil {
		t.Fatal(err)
	}
	command := companion.ImportPrivateKey{}
	copy(command.PrivateKey[:], imported.PrvKey())
	_ = svc.handle(t.Context(), command)
	svc.enqueueMailbox(t.Context(), companion.MessagesWaiting{})

	svc.mu.Lock()
	svc.stats.sent = 7
	svc.appVersion = protocolVersion
	svc.pending = pendingRequest{kind: pendingStatus, tag: 42}
	svc.signData = []byte("partial")
	svc.sendUnscoped = true
	if !svc.outbound.offer(emission{kind: "queued-before-reset"}) {
		t.Fatal("could not seed outbound queue")
	}
	svc.mu.Unlock()

	responses := svc.handle(t.Context(), companion.FactoryReset{})
	if len(responses) != 1 || responses[0] != companion.StatusResponse(companion.ResponseOK) {
		t.Fatalf("factory reset responses = %#v", responses)
	}
	if svc.p.NodeName != "Alice" || svc.p.Waveform != configuredWaveform || svc.id.PubKey != configuredKey {
		t.Fatalf("factory state = name %q waveform %+v key %x", svc.p.NodeName, svc.p.Waveform, svc.id.PubKey[:6])
	}
	if len(svc.channels) != 1 || svc.channels[0].name != "Public" ||
		len(svc.contacts) != 0 || len(svc.mailbox) != 0 ||
		svc.defaultScope != "" || svc.stats.sent != 0 || svc.appVersion != 0 ||
		svc.pending.kind != pendingNone || svc.signData != nil || svc.sendUnscoped || svc.outbound.len() != 0 {
		t.Fatalf("factory reset left state behind: channels %d contacts %d mailbox %d scope %q stats %+v",
			len(svc.channels), len(svc.contacts), len(svc.mailbox), svc.defaultScope, svc.stats)
	}

	built, err = build(spec)
	if err != nil {
		t.Fatal(err)
	}
	restored := requireService(t, built)
	if restored.p.NodeName != "Alice" || restored.p.Waveform != configuredWaveform ||
		restored.id.PubKey != configuredKey || len(restored.channels) != 1 ||
		restored.channels[0].name != "Public" || len(restored.mailbox) != 0 {
		t.Fatalf("persisted factory state = name %q waveform %+v key %x channels %d mailbox %d",
			restored.p.NodeName, restored.p.Waveform, restored.id.PubKey[:6],
			len(restored.channels), len(restored.mailbox))
	}
}

func TestFactoryResetWriteFailureRollsBackWithoutRestartingSession(t *testing.T) {
	store := &memoryStationState{}
	spec := testSpec(t)
	spec.State = store
	built, err := build(spec)
	if err != nil {
		t.Fatal(err)
	}
	svc := requireService(t, built)
	_ = svc.handle(t.Context(), companion.SetAdvertName{Name: "Persisted"})
	svc.stats.sent = 4
	svc.generation = 7
	store.fail = true

	responses := svc.handle(t.Context(), companion.FactoryReset{})
	if len(responses) != 1 || responses[0] != (companion.ErrorResponse{Code: companion.ErrFileIO}) {
		t.Fatalf("factory reset failure responses = %#v", responses)
	}
	if svc.p.NodeName != "Persisted" || svc.stats.sent != 4 || svc.disconnect != 0 {
		t.Fatalf("failed reset left name %q stats %d disconnect generation %d",
			svc.p.NodeName, svc.stats.sent, svc.disconnect)
	}
}

func TestEveryReferenceCompanionCommandReachesAStationHandler(t *testing.T) {
	built, err := build(testSpec(t))
	if err != nil {
		t.Fatal(err)
	}
	svc := requireService(t, built)
	privateKey := companion.ImportPrivateKey{}
	copy(privateKey.PrivateKey[:], svc.id.PrvKey())
	contactKey := [mesh.PubKeySize]byte{1}
	commands := []companion.Command{
		companion.AppStart{Name: "test"},
		companion.SendText{TextType: mesh.TxtTypePlain, RecipientPrefix: [6]byte{1}, Text: "hello"},
		companion.SendChannelText{TextType: mesh.TxtTypePlain, Channel: 1, Text: "hello"},
		companion.GetContacts{},
		companion.SimpleCommand{Kind: companion.CommandGetDeviceTime},
		companion.SetDeviceTime{UnixSeconds: uint32(time.Now().Add(time.Minute).Unix())},
		companion.SendSelfAdvert{},
		companion.SetAdvertName{Name: "station"},
		companion.AddUpdateContact{Contact: companion.Contact{
			PublicKey: contactKey, Type: mesh.AdvTypeChat, PathLen: 0xff, Name: "peer",
		}},
		companion.SimpleCommand{Kind: companion.CommandSyncNextMessage},
		companion.SetRadioParams{FrequencyKHz: svc.p.FrequencyHz / 1_000,
			BandwidthHz: uint32(svc.p.BandwidthHz), Spreading: uint8(svc.p.SpreadingFactor),
			CodingRate: uint8(svc.p.CodingRate)},
		companion.SetRadioTXPower{PowerDBm: svc.p.TXPowerDBm},
		companion.ContactKey{Kind: companion.CommandResetPath, PublicKey: contactKey},
		companion.SetAdvertLocation{},
		companion.ContactKey{Kind: companion.CommandRemoveContact, PublicKey: contactKey},
		companion.ContactKey{Kind: companion.CommandShareContact, PublicKey: contactKey},
		companion.ExportContact{Self: true},
		companion.ImportContact{Packet: make([]byte, 98)},
		companion.SimpleCommand{Kind: companion.CommandGetBatteryAndStorage},
		companion.SetTuningParams{},
		companion.DeviceQuery{TargetVersion: protocolVersion},
		companion.SimpleCommand{Kind: companion.CommandExportPrivateKey},
		privateKey,
		companion.SendRawData{Data: []byte{1, 2, 3, 4}},
		companion.SendLogin{PublicKey: contactKey},
		companion.ContactRequest{Kind: companion.CommandSendStatusRequest, PublicKey: contactKey},
		companion.ContactRequest{Kind: companion.CommandHasConnection, PublicKey: contactKey},
		companion.ContactRequest{Kind: companion.CommandLogout, PublicKey: contactKey},
		companion.ContactKey{Kind: companion.CommandGetContactByKey, PublicKey: contactKey},
		companion.GetChannel{Index: 1},
		companion.SetChannel{Index: 1, Name: "ops"},
		companion.SimpleCommand{Kind: companion.CommandSignStart},
		companion.SignData{Data: []byte("data")},
		companion.SimpleCommand{Kind: companion.CommandSignFinish},
		companion.SendTracePath{Flags: 0, Path: []byte{1}},
		companion.SetDevicePIN{PIN: 123456},
		companion.SetOtherParams{ManualContacts: true},
		companion.SendTelemetryRequest{Self: true},
		companion.SimpleCommand{Kind: companion.CommandGetCustomVars},
		companion.SetCustomVar{Name: "gps", Value: "1"},
		companion.GetAdvertPath{PublicKey: contactKey},
		companion.SimpleCommand{Kind: companion.CommandGetTuningParams},
		companion.ContactDataRequest{Kind: companion.CommandSendBinaryRequest,
			PublicKey: contactKey, Data: []byte{1}},
		companion.SendPathDiscovery{PublicKey: contactKey},
		companion.SetFloodScope{Null: true},
		companion.SendControlData{Data: []byte{0x80}},
		companion.GetStats{Type: companion.StatsCore},
		companion.ContactDataRequest{Kind: companion.CommandSendAnonymousRequest,
			PublicKey: [mesh.PubKeySize]byte{2}, Data: []byte{mesh.AnonReqClock}},
		companion.SetAutoAddConfig{},
		companion.SimpleCommand{Kind: companion.CommandGetAutoAddConfig},
		companion.SimpleCommand{Kind: companion.CommandGetAllowedRepeatFreq},
		companion.SetPathHashMode{},
		companion.SendChannelData{Channel: 1, PathLen: 0xff, DataType: 1},
		companion.SetDefaultFloodScope{Clear: true},
		companion.SimpleCommand{Kind: companion.CommandGetDefaultFloodScope},
		companion.SendRawPacket{Packet: []byte{1, 2}},
		companion.FactoryReset{},
		companion.Reboot{},
	}
	if len(commands) != 58 {
		t.Fatalf("command matrix has %d entries, want 58", len(commands))
	}
	seen := make(map[companion.CommandCode]struct{}, len(commands))
	for _, command := range commands {
		if _, duplicate := seen[command.Code()]; duplicate {
			t.Fatalf("command code %d appears twice", command.Code())
		}
		seen[command.Code()] = struct{}{}
		for _, response := range svc.handle(t.Context(), command) {
			if failure, ok := response.(companion.ErrorResponse); ok &&
				failure.Code == companion.ErrUnsupportedCommand {
				t.Fatalf("command %d (%T) reached unsupported fallback", command.Code(), command)
			}
		}
	}
}

func TestCompanionIdentityImportExportAndSigningSurviveRestart(t *testing.T) {
	store := &memoryStationState{}
	spec := testSpec(t)
	spec.State = store
	built, err := build(spec)
	if err != nil {
		t.Fatal(err)
	}
	first := requireService(t, built)
	imported, err := mesh.LocalIdentityFromSeed(bytes.Repeat([]byte{17}, mesh.SeedSize))
	if err != nil || !imported.FirmwareImportable() {
		t.Fatalf("test identity: %v, key %x", err, imported.PubKey[:2])
	}
	command := companion.ImportPrivateKey{}
	copy(command.PrivateKey[:], imported.PrvKey())
	responses := first.handle(t.Context(), command)
	if len(responses) != 1 || responses[0] != companion.StatusResponse(companion.ResponseOK) {
		t.Fatalf("identity import = %#v", responses)
	}

	built, err = build(spec)
	if err != nil {
		t.Fatal(err)
	}
	second := requireService(t, built)
	if second.id.PubKey != imported.PubKey {
		t.Fatalf("restored public key = %x, want %x", second.id.PubKey, imported.PubKey)
	}
	responses = second.handle(t.Context(), companion.SimpleCommand{Kind: companion.CommandExportPrivateKey})
	exported, ok := responses[0].(companion.PrivateKey)
	if !ok || !bytes.Equal(exported.Key[:], imported.PrvKey()) {
		t.Fatalf("identity export = %#v", responses)
	}

	responses = second.handle(t.Context(), companion.SimpleCommand{Kind: companion.CommandSignStart})
	start, ok := responses[0].(companion.SignStart)
	if !ok || start.MaxBytes != maxSignData {
		t.Fatalf("sign start = %#v", responses)
	}
	for _, fragment := range [][]byte{[]byte("mesh"), []byte("core")} {
		responses = second.handle(t.Context(), companion.SignData{Data: fragment})
		if len(responses) != 1 || responses[0] != companion.StatusResponse(companion.ResponseOK) {
			t.Fatalf("sign fragment = %#v", responses)
		}
	}
	responses = second.handle(t.Context(), companion.SimpleCommand{Kind: companion.CommandSignFinish})
	signed, ok := responses[0].(companion.Signature)
	if !ok || !second.id.Verify(signed.Value[:], []byte("meshcore")) {
		t.Fatalf("signature = %#v", responses)
	}
	responses = second.handle(t.Context(), companion.SimpleCommand{Kind: companion.CommandSignFinish})
	if len(responses) != 1 || responses[0] != (companion.ErrorResponse{Code: companion.ErrBadState}) {
		t.Fatalf("second sign finish = %#v", responses)
	}
}

func TestContactCRUDFiltersAndPersists(t *testing.T) {
	store := &memoryStationState{}
	spec := testSpec(t)
	spec.State = store
	built, err := build(spec)
	if err != nil {
		t.Fatal(err)
	}
	svc := requireService(t, built)
	contact := companion.Contact{
		PublicKey: [32]byte{1: 2, 31: 3}, Type: 2, Flags: 1, PathLen: 0x01,
		Path: [64]byte{7},
		Name: "Relay", LastAdvertUnix: 10, LatitudeE6: 1, LongitudeE6: 2,
		LastModifiedUnix: 50,
	}
	responses := svc.handle(t.Context(), companion.AddUpdateContact{
		Contact: contact, HasLocation: true, HasLastModified: true,
	})
	if len(responses) != 1 {
		t.Fatalf("add responses = %#v", responses)
	}
	responses = svc.handle(t.Context(), companion.GetContacts{Since: 49, HasSince: true})
	if len(responses) != 3 {
		t.Fatalf("contact stream = %#v", responses)
	}
	start, ok := responses[0].(companion.ContactsStart)
	item, itemOK := responses[1].(companion.ContactResponse)
	end, endOK := responses[2].(companion.EndOfContacts)
	if !ok || start.Count != 1 || !itemOK || item.Contact != contact || !endOK || end.MostRecent != 50 {
		t.Fatalf("contact stream = %#v", responses)
	}
	if got := svc.handle(t.Context(), companion.GetContacts{Since: 50, HasSince: true}); len(got) != 2 {
		t.Fatalf("filtered stream = %#v", got)
	}

	built, err = build(spec)
	if err != nil {
		t.Fatal(err)
	}
	restored := requireService(t, built)
	responses = restored.handle(t.Context(), companion.ContactKey{
		Kind: companion.CommandGetContactByKey, PublicKey: contact.PublicKey,
	})
	if len(responses) != 1 {
		t.Fatalf("restored contact = %#v", responses)
	}
	if got := restored.handle(t.Context(), companion.ContactKey{
		Kind: companion.CommandResetPath, PublicKey: contact.PublicKey,
	}); len(got) != 1 || restored.contacts[contact.PublicKey].info.PathLen != 0xff {
		t.Fatalf("reset path = %#v", got)
	}
	_ = restored.handle(t.Context(), companion.ContactKey{
		Kind: companion.CommandRemoveContact, PublicKey: contact.PublicKey,
	})
	if len(restored.contacts) != 0 {
		t.Fatal("contact survived removal")
	}
}

func TestImportedAdvertCanBeExported(t *testing.T) {
	built, err := build(testSpec(t))
	if err != nil {
		t.Fatal(err)
	}
	svc := requireService(t, built)
	peer, err := mesh.NewLocalIdentity(bytes.NewReader(bytes.Repeat([]byte{9}, 64)))
	if err != nil {
		t.Fatal(err)
	}
	packet, err := mesh.BuildAdvert(peer, time.Unix(1_800_000_000, 0),
		&mesh.AdvertData{Type: mesh.AdvTypeChat, Name: "Peer"})
	if err != nil {
		t.Fatal(err)
	}
	packet.SetPathHashSizeAndCount(2, 1)
	packet.Path = []byte{4, 5}
	raw, err := packet.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	responses := svc.handle(t.Context(), companion.ImportContact{Packet: raw})
	var push companion.Push
	var pushed bool
	if len(responses) == 2 {
		push, pushed = responses[1].(companion.Push)
	}
	if len(responses) != 2 || responses[0] != companion.StatusResponse(companion.ResponseOK) ||
		!pushed || push.Code != companion.PushNewAdvert || len(svc.contacts) != 1 {
		t.Fatalf("import = %#v contacts %d", responses, len(svc.contacts))
	}
	responses = svc.handle(t.Context(), companion.ExportContact{PublicKey: peer.PubKey})
	exported, ok := responses[0].(companion.ExportedContact)
	if !ok {
		t.Fatalf("export = %#v", responses)
	}
	back, err := mesh.ParsePacket(exported.Packet)
	if err != nil || back.Route() != mesh.RouteFlood || back.PathHashCount() != 0 || back.HasTransportCodes() {
		t.Fatalf("exported packet = %#v, %v", back, err)
	}
	if _, err := mesh.ParseAdvert(back.Payload); err != nil {
		t.Fatalf("exported advert: %v", err)
	}
}

func requireService(t *testing.T, built station.Service) *service {
	t.Helper()
	svc, ok := built.(*service)
	if !ok {
		t.Fatalf("built service is %T", built)
	}
	return svc
}

func awaitListener(t *testing.T, svc *service) string {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		info := svc.Info()
		if info.State == station.StateRunning {
			return info.Listen
		}
		if info.State == station.StateError {
			t.Fatalf("station startup: %s", info.Cause)
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("station did not listen")
	return ""
}

func exchange(t *testing.T, conn net.Conn, command companion.Command) []byte {
	t.Helper()
	payload, err := companion.MarshalCommand(command)
	if err != nil {
		t.Fatal(err)
	}
	if err := companion.WriteFrame(conn, companion.ToDevice, payload); err != nil {
		t.Fatal(err)
	}
	frame, err := companion.ReadFrame(conn, companion.ToApplication)
	if err != nil {
		t.Fatal(err)
	}
	return frame.Payload
}

func takeEmission(t *testing.T, svc *service) emission {
	t.Helper()
	item, ok := svc.outbound.takeUntil(t.Context(), time.Now().Add(time.Second))
	if !ok {
		t.Fatal("station outbound queue did not yield an emission")
	}
	return item
}

func pollEmission(svc *service) (emission, bool) {
	return svc.outbound.takeUntil(context.Background(), time.Now())
}

func TestAnUnnamedStationStillHasAName(t *testing.T) {
	// A relay refuses to announce unnamed — it is infrastructure, and
	// its name is a deliberate act. A station is somebody's companion:
	// its application names it on connection, so the default only has
	// to carry the moments before that. What it may never be is empty
	// on the air.
	spec := testSpec(t)
	cfg := spec.Config
	delete(cfg, "node_name")
	built, err := build(spec)
	if err != nil {
		t.Fatal(err)
	}
	s, ok := built.(*service)
	if !ok {
		t.Fatalf("build returned %T", built)
	}
	want := product.Name + "-" + hex.EncodeToString(s.id.PubKey[:4])
	if s.p.NodeName != want {
		t.Errorf("node name = %q, want %q", s.p.NodeName, want)
	}
	// It reaches the air, and the group-text budget sizes itself on
	// the same string rather than on the empty one it replaced.
	packet, err := s.selfAdvert(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	advert, err := mesh.ParseAdvert(packet.Payload)
	if err != nil {
		t.Fatal(err)
	}
	if advert.Data.Name != want {
		t.Errorf("advertised name = %q, want %q", advert.Data.Name, want)
	}
	// The factory snapshot carries it, so a reset restores a named
	// station rather than a nameless one.
	if s.factoryState.NodeName != want {
		t.Errorf("factory name = %q, want %q", s.factoryState.NodeName, want)
	}

	// An operator's own name outranks the default, and a companion's
	// choice outranks both — proved by the ordinary fixture.
	named, err := build(testSpec(t))
	if err != nil {
		t.Fatal(err)
	}
	configured, ok := named.(*service)
	if !ok {
		t.Fatalf("build returned %T", named)
	}
	if got := configured.p.NodeName; got != "Alice" {
		t.Errorf("configured name = %q, want Alice", got)
	}
}
