package meshcore

import (
	"bytes"
	"context"
	"encoding/hex"
	"net"
	"testing"
	"time"

	"go.uber.org/zap"

	"meshrunner.dev/lotor/internal/meshcorecfg"
	"meshrunner.dev/lotor/internal/station"
	"meshrunner.dev/lotor/internal/version"

	"meshrunner.dev/pkg/meshcore/companion"
)

func testSpec(t *testing.T, listen string) station.Spec {
	t.Helper()
	cfg := meshcorecfg.Presets()["eu-868-narrow"]
	cfg["identity"] = hex.EncodeToString(bytes.Repeat([]byte{1}, 32))
	cfg["node_name"] = "Alice"
	cfg["tx_power_dbm"] = 14
	return station.Spec{
		Name: "alice", Protocol: protocolName, Listen: listen, Config: cfg,
		Log: zap.NewNop(), Build: version.Info{
			Version: "1.2.3", SourceTime: time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC),
		},
	}
}

func TestDetachedStationAnswersStartupProtocol(t *testing.T) {
	built, err := build(testSpec(t, "127.0.0.1:0"))
	if err != nil {
		t.Fatal(err)
	}
	svc := built.(*service)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- svc.Run(ctx) }()
	addr := awaitListener(t, svc)

	conn, err := net.Dial("tcp", addr)
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

func TestNewCompanionClientReplacesThePreviousOne(t *testing.T) {
	built, err := build(testSpec(t, "127.0.0.1:0"))
	if err != nil {
		t.Fatal(err)
	}
	svc := built.(*service)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = svc.Run(ctx) }()
	addr := awaitListener(t, svc)

	first, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = first.Close() }()
	_ = exchange(t, first, companion.DeviceQuery{TargetVersion: protocolVersion})

	second, err := net.Dial("tcp", addr)
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

func TestDetachedStationForbidsRepeatingAndKeepsRadioPreferences(t *testing.T) {
	built, err := build(testSpec(t, "127.0.0.1:0"))
	if err != nil {
		t.Fatal(err)
	}
	svc := built.(*service)
	responses := svc.handle(companion.SetRadioParams{Repeat: true})
	wire, _ := companion.MarshalResponse(responses[0])
	want, _ := companion.MarshalResponse(companion.ErrorResponse{Code: companion.ErrorIllegalArgument})
	if !bytes.Equal(wire, want) {
		t.Fatalf("repeat response = % X, want % X", wire, want)
	}
	responses = svc.handle(companion.SetRadioParams{
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
