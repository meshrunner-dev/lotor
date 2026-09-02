package room

import (
	"context"
	"strings"
	"testing"
	"time"

	"meshrunner.dev/lotor/internal/application"
	"meshrunner.dev/lotor/internal/config"
)

func baseConfig() map[string]any {
	return map[string]any{
		"frequency_hz": 869_618_000, "spreading_factor": 8, "bandwidth_hz": 62_500, "coding_rate": 8,
		// A fixed seed: identity=new is minted by the manager before a
		// type ever sees it, so the type only ever reads hex.
		"identity": strings.Repeat("11", 32), "node_name": "lobby",
	}
}

func TestTheRoomRegistersAsAMeshCoreType(t *testing.T) {
	b, err := application.Lookup("meshcore", "meshcore-room")
	if err != nil {
		t.Fatal(err)
	}
	if b.Check == nil || b.Asks == nil || b.Build == nil || len(b.Schema) == 0 || len(b.Presets) == 0 {
		t.Fatal("the room's builder is incomplete")
	}
}

func TestTheReferenceDefaultsApplyWhenUnsaid(t *testing.T) {
	cfg := baseConfig()
	p, id, err := resolve(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if id == nil {
		t.Fatal("the seed resolved to no identity")
	}
	if p.FloodAdvert != defaultFloodAdvert || p.LocalAdvert != defaultLocalAdvert ||
		p.History != defaultHistory || !p.PersistHistory {
		t.Errorf("defaults = %+v", p)
	}
	// A configured zero means never, not the default.
	cfg["advert_local_interval"] = "0s"
	cfg["persist_history"] = false
	p, _, err = resolve(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if p.LocalAdvert != 0 || p.PersistHistory {
		t.Errorf("explicit zero/false overridden: %+v", p)
	}
}

func TestTheRoomRefusesWhatItCannotServe(t *testing.T) {
	for name, bend := range map[string]func(map[string]any){
		"no identity":       func(c map[string]any) { delete(c, "identity") },
		"same passwords":    func(c map[string]any) { c["admin_password"], c["guest_password"] = "pw", "pw" },
		"long password":     func(c map[string]any) { c["admin_password"] = strings.Repeat("x", maxPassword+1) },
		"negative interval": func(c map[string]any) { c["advert_flood_interval"] = "-1h" },
		"history too deep":  func(c map[string]any) { c["history"] = maxHistory + 1 },
		"bad latitude":      func(c map[string]any) { c["node_lat"] = 91.0 },
		"unknown key":       func(c map[string]any) { c["posts"] = 3 },
	} {
		cfg := baseConfig()
		bend(cfg)
		if err := check(cfg); err == nil {
			t.Errorf("%s was accepted", name)
		}
	}
}

// A shadow room queues its adverts for the pipeline; detached from any
// radio, the pipeline refuses them as radio-down and the room says so.
// A dry room composes and counts, and queues nothing.
func TestAdvertsTravelThePipelineUnlessTheGateIsDry(t *testing.T) {
	for _, mode := range []string{config.TXShadow, config.TXDry} {
		cfg := baseConfig()
		cfg["advert_local_interval"] = "20ms"
		cfg["advert_flood_interval"] = "0s"
		svc, err := build(application.Spec{Name: "lobby", Config: cfg, TX: application.TXPolicy{Mode: mode}})
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- svc.Run(ctx) }()
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			info := svc.Info()
			if info.Summary["adverts due"] != "0" && (mode == config.TXDry || info.Summary["dropped"] != "0") {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		info := svc.Info()
		cancel()
		<-done
		if info.State != application.StateRunning || info.Summary["adverts due"] == "0" || info.Summary["tx"] != mode {
			t.Fatalf("%s room = %+v", mode, info)
		}
		if mode == config.TXDry && info.Summary["dropped"] != "0" {
			t.Errorf("a dry room queued an advert: %+v", info.Summary)
		}
		if mode == config.TXShadow && info.Summary["dropped"] == "0" {
			t.Errorf("a detached shadow room never offered its advert to the pipeline: %+v", info.Summary)
		}
	}
}

func TestADryRoomRunsDetachedAndReportsItself(t *testing.T) {
	cfg := baseConfig()
	cfg["advert_local_interval"] = "20ms"
	cfg["advert_flood_interval"] = "0s"
	svc, err := build(application.Spec{Name: "lobby", Protocol: "meshcore", Type: "meshcore-room", Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- svc.Run(ctx) }()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if info := svc.Info(); info.State == application.StateRunning && info.Summary["adverts due"] != "0" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	info := svc.Info()
	if info.State != application.StateRunning || info.RF != application.RFDetached {
		t.Fatalf("detached dry room = %s / %s (%s)", info.State, info.RF, info.Cause)
	}
	if info.Summary["adverts due"] == "0" {
		t.Error("the local advert clock never fired")
	}
	if info.Type != "meshcore-room" || info.Protocol != "meshcore" || len(info.PublicKey) != 64 ||
		info.Summary["node"] != "lobby" {
		t.Errorf("info = %+v", info)
	}
	cancel()
	if err := <-done; err != nil {
		t.Errorf("Run = %v", err)
	}
	if svc.Info().State != application.StateStopped {
		t.Error("the room did not report stopped")
	}
}
