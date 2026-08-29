package meshcore

import (
	"math"
	"testing"
	"time"

	"meshrunner.dev/lotor/internal/radio"
)

func TestPacketScore(t *testing.T) {
	cases := []struct {
		name string
		snr  float64
		sf   int
		len  int
		want float64
	}{
		// SF8's threshold is -10: at it, no margin, no score.
		{"at threshold", -10, 8, 0, 0},
		{"below threshold", -12, 8, 0, 0},
		// Ten dB of margin saturates the rate; an empty frame pays no
		// collision penalty.
		{"full margin", 0, 8, 0, 1},
		// Half a maximum frame halves the score.
		{"half length", 0, 8, 128, 0.5},
		// A frame at the assumed maximum scores nothing whatever the
		// margin.
		{"max length", 20, 8, 256, 0},
		{"sf10 threshold", -15, 10, 0, 0},
		{"sf10 margin", -5, 10, 0, 1},
		// Factors outside the table score zero rather than reading
		// past it.
		{"sf too low", 10, 5, 0, 0},
		{"sf too high", 10, 13, 0, 0},
	}
	for _, c := range cases {
		if got := packetScore(c.snr, c.sf, c.len); math.Abs(got-c.want) > 1e-9 {
			t.Errorf("%s: score %g, want %g", c.name, got, c.want)
		}
	}
}

func TestRxDelayCurve(t *testing.T) {
	e, _ := testEngine(t) // SF8: threshold -10
	at := func(base, snr float64, air time.Duration) time.Duration {
		e.p.RxDelayBase = base
		return e.rxDelay(radio.Frame{SNR: snr, Airtime: air})
	}

	if d := at(0, -10, time.Second); d != 0 {
		t.Errorf("base 0 held %s, want nothing", d)
	}
	// Score 0 at one second of airtime: (10^0.85 − 1) × 1 s.
	want := time.Duration((math.Pow(10, 0.85) - 1) * float64(time.Second))
	if d := at(10, -10, time.Second); d != want {
		t.Errorf("score 0 held %s, want %s", d, want)
	}
	// A strong reception outruns the pivot: the curve goes negative
	// and the frame is judged now.
	if d := at(10, 5, time.Second); d != 0 {
		t.Errorf("strong reception held %s, want nothing", d)
	}
	// Below the floor the hold is not worth the queue.
	if d := at(10, -10, time.Millisecond); d != 0 {
		t.Errorf("tiny airtime held %s, want nothing", d)
	}
	// And no score holds a frame past the cap.
	if d := at(20, -10, 10*time.Second); d != rxDelayCap {
		t.Errorf("held %s, want the %s cap", d, rxDelayCap)
	}
}

func TestFloodHeldUntilDue(t *testing.T) {
	e, sub := testEngine(t)
	e.p.RxDelayBase = 10
	dev := newFakeDevice()

	// A weakly-heard flood is held, not judged.
	e.judge(dev, radio.Frame{Payload: floodAdvert, At: time.Now(),
		SNR: -10, Airtime: time.Second})
	if got := drainJudged(t, sub); len(got) != 0 {
		t.Fatalf("judged %d frames during the hold", len(got))
	}
	if len(e.held) != 1 {
		t.Fatalf("held %d frames, want 1", len(e.held))
	}
	if _, ok := e.heldWait(time.Now()); !ok {
		t.Fatal("heldWait reports nothing due")
	}

	// Draining before the due time keeps it; at the due time it is
	// judged like any reception.
	e.drainHeld(dev, time.Now())
	if len(e.held) != 1 {
		t.Fatalf("drained early: %d held", len(e.held))
	}
	e.drainHeld(dev, time.Now().Add(rxDelayCap))
	if len(e.held) != 0 {
		t.Fatalf("still holding %d frames past due", len(e.held))
	}
	judged := drainJudged(t, sub)
	if len(judged) != 1 || judged[0].Verdict != "would-drop-invalid-advert" {
		t.Fatalf("judged %+v after the hold", judged)
	}

	// Routed traffic is never held, whatever the base: the stagger
	// exists for the copies a flood fans out, and a directed frame
	// has exactly one intended next hop.
	e.judge(dev, radio.Frame{Payload: directPath, At: time.Now(),
		SNR: -10, Airtime: time.Second})
	if len(e.held) != 0 {
		t.Fatal("a direct frame was held")
	}
	if got := drainJudged(t, sub); len(got) != 1 {
		t.Fatalf("direct frame judged %d times, want 1", len(got))
	}
}

func TestDelayParamBounds(t *testing.T) {
	base := func() map[string]any { return map[string]any{"frequency_hz": 869_618_000} }
	for _, good := range []float64{0, 0.5, 2} {
		cfg := base()
		cfg["tx_delay_factor"] = good
		cfg["direct_tx_delay_factor"] = good
		if _, err := paramsFrom(cfg); err != nil {
			t.Errorf("factor %v refused: %v", good, err)
		}
	}
	for _, bad := range []float64{-0.01, 2.01, math.NaN(), math.Inf(1)} {
		for _, attr := range []string{"tx_delay_factor", "direct_tx_delay_factor"} {
			cfg := base()
			cfg[attr] = bad
			if _, err := paramsFrom(cfg); err == nil {
				t.Errorf("%s %v accepted", attr, bad)
			}
		}
	}
	for _, good := range []float64{0, 10, 20} {
		cfg := base()
		cfg["rx_delay_base"] = good
		if _, err := paramsFrom(cfg); err != nil {
			t.Errorf("rx_delay_base %v refused: %v", good, err)
		}
	}
	for _, bad := range []float64{-0.01, 20.01, math.NaN(), math.Inf(-1)} {
		cfg := base()
		cfg["rx_delay_base"] = bad
		if _, err := paramsFrom(cfg); err == nil {
			t.Errorf("rx_delay_base %v accepted", bad)
		}
	}
}

func TestPathHashAndLoopParams(t *testing.T) {
	base := func() map[string]any { return map[string]any{"frequency_hz": 869_618_000} }
	for _, good := range []int{0, 1, 2} {
		cfg := base()
		cfg["path_hash_mode"] = good
		if _, err := paramsFrom(cfg); err != nil {
			t.Errorf("path_hash_mode %d refused: %v", good, err)
		}
	}
	for _, bad := range []int{-1, 3} {
		cfg := base()
		cfg["path_hash_mode"] = bad
		if _, err := paramsFrom(cfg); err == nil {
			t.Errorf("path_hash_mode %d accepted", bad)
		}
	}
	for _, good := range []string{"off", "minimal", "moderate", "strict"} {
		cfg := base()
		cfg["loop_detect"] = good
		if _, err := paramsFrom(cfg); err != nil {
			t.Errorf("loop_detect %s refused: %v", good, err)
		}
	}
	cfg := base()
	cfg["loop_detect"] = "banana"
	if _, err := paramsFrom(cfg); err == nil {
		t.Error("loop_detect banana accepted")
	}
	// Unset resolves to this node's deliberate defaults: two-byte
	// origination, minimal orbit armour.
	var p params
	if w := p.pathHashWidth(); w != 2 {
		t.Errorf("unset width %d, want 2", w)
	}
	if m := p.loopDetect(); m != loopMinimal {
		t.Errorf("unset loop mode %q, want minimal", m)
	}
	// An explicit mode 0 is the reference's own one-byte choice.
	zero := 0
	p.PathHashMode = &zero
	if w := p.pathHashWidth(); w != 1 {
		t.Errorf("mode 0 width %d, want 1", w)
	}
}

func TestFloodAdvertDeclaresItsWidth(t *testing.T) {
	e := armedEngine(t, "on-air")
	e.advert(newFakeDevice(), time.Now(), "advert-flood", false)
	entry, ok := e.queue.pop(time.Now().Add(time.Minute))
	if !ok {
		t.Fatal("no advert queued")
	}
	if w := entry.pkt.PathHashSize(); w != 2 {
		t.Errorf("flood advert declares %d-byte hashes, want 2", w)
	}
	if n := entry.pkt.PathHashCount(); n != 0 {
		t.Errorf("fresh advert carries %d hops", n)
	}
}

func TestDelayFactorsResolveDefaults(t *testing.T) {
	var p params
	if got := p.txDelayFactor(); got != defaultTxDelayFactor {
		t.Errorf("unset tx factor %g, want %g", got, defaultTxDelayFactor)
	}
	if got := p.directTxDelayFactor(); got != defaultDirectTxDelayFactor {
		t.Errorf("unset direct factor %g, want %g", got, defaultDirectTxDelayFactor)
	}
	// An explicit zero is a choice, not an absence.
	zero := 0.0
	p.TxDelayFactor, p.DirectTxDelayFactor = &zero, &zero
	if p.txDelayFactor() != 0 || p.directTxDelayFactor() != 0 {
		t.Error("explicit 0 did not survive resolution")
	}
}
