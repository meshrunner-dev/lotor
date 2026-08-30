package meshcore

import (
	"bytes"
	"testing"

	"meshrunner.dev/lotor/internal/radio"

	mesh "meshrunner.dev/pkg/meshcore"
	"meshrunner.dev/pkg/meshcore/companion"
)

func TestTraceUsesPayloadRouteAndSNRPath(t *testing.T) {
	svc := testRFService(t)
	command := companion.SendTracePath{
		Tag: 0x11223344, Auth: 0x55667788, Flags: 1, Path: []byte{1, 2, 3, 4},
	}
	responses := svc.handle(t.Context(), command)
	sent, ok := responses[0].(companion.Sent)
	if !ok || sent.Flood || sent.ExpectedACK != command.Tag || sent.TimeoutMillis != 3_050 {
		t.Fatalf("trace sent = %#v", responses)
	}
	emission := <-svc.outbound
	if emission.packet.PayloadType() != mesh.PayloadTypeTrace || emission.packet.PathHashCount() != 0 ||
		!bytes.Equal(emission.packet.Payload[9:], command.Path) {
		t.Fatalf("trace emission = %#v", emission.packet)
	}

	emission.packet.SetPathHashSizeAndCount(1, 2)
	emission.packet.Path = []byte{4, 8}
	app := attachApplication(t, svc)
	got := readPushAfter(t, app, func() {
		svc.receiveTrace(emission.packet, radio.Frame{SNR: 3})
	})
	want := []byte{byte(companion.PushTraceData), 0, 4, 1,
		0x44, 0x33, 0x22, 0x11, 0x88, 0x77, 0x66, 0x55,
		1, 2, 3, 4, 4, 8, 12}
	if !bytes.Equal(got, want) {
		t.Fatalf("trace push = % X, want % X", got, want)
	}
}

func TestAdvertPathCacheReportsTheLatestWitnessedRoute(t *testing.T) {
	svc := testRFService(t)
	peer := testPeer(t, 37)
	packet, err := mesh.BuildAdvert(peer, svc.startedAt, &mesh.AdvertData{Type: mesh.AdvTypeChat, Name: "peer"})
	if err != nil {
		t.Fatal(err)
	}
	packet.SetPathHashSizeAndCount(2, 2)
	packet.Path = []byte{1, 2, 3, 4}
	raw, err := packet.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	svc.processRF(t.Context(), radio.Frame{Payload: raw})
	responses := svc.handle(t.Context(), companion.GetAdvertPath{PublicKey: peer.PubKey})
	path, ok := responses[0].(companion.AdvertPath)
	if !ok || path.ReceivedUnix == 0 || path.PathLen != 0x42 || !bytes.Equal(path.Path, packet.Path) {
		t.Fatalf("advert path = %#v", responses)
	}
	unknown := peer.PubKey
	unknown[6]++
	responses = svc.handle(t.Context(), companion.GetAdvertPath{PublicKey: unknown})
	if len(responses) != 1 || responses[0] != (companion.ErrorResponse{Code: companion.ErrorNotFound}) {
		t.Fatalf("unknown advert path = %#v", responses)
	}
}

func TestVirtualStationCustomSettingsAreExplicitlyEmpty(t *testing.T) {
	svc := testRFService(t)
	responses := svc.handle(t.Context(), companion.SimpleCommand{Kind: companion.CommandGetCustomVars})
	if len(responses) != 1 || responses[0] != (companion.CustomVars{}) {
		t.Fatalf("custom vars = %#v", responses)
	}
	responses = svc.handle(t.Context(), companion.SetCustomVar{Name: "gps", Value: "1"})
	if len(responses) != 1 || responses[0] != (companion.ErrorResponse{Code: companion.ErrorIllegalArgument}) {
		t.Fatalf("set custom var = %#v", responses)
	}
}
