package meshcore

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"meshrunner.dev/pkg/meshcore"
)

func TestNeighbourOrderings(t *testing.T) {
	// The four the query may ask for, and the one it may not.
	now := time.Now()
	// As the table hands them over: newest heard first.
	base := []Neighbour{
		{SNR: -2, Heard: now},                  // newest, weakest
		{SNR: 3, Heard: now.Add(-time.Minute)}, // middle both ways
		{SNR: 9, Heard: now.Add(-time.Hour)},   // oldest, strongest
	}
	for _, c := range []struct {
		order byte
		first float64 // the SNR the first row should carry
		what  string
	}{
		{meshcore.NeighboursNewestFirst, -2, "newest heard"},
		{meshcore.NeighboursOldestFirst, 9, "oldest heard"},
		{meshcore.NeighboursStrongestFirst, 9, "strongest"},
		{meshcore.NeighboursWeakestFirst, -2, "weakest"},
		{200, -2, "an order nobody defined leaves what the table gave"},
	} {
		all := append([]Neighbour(nil), base...)
		orderNeighbours(all, c.order)
		if all[0].SNR != c.first {
			t.Errorf("order %d (%s): first is %v dB, want %v", c.order, c.what, all[0].SNR, c.first)
		}
	}
}

func TestTelemetryAlwaysLeadsWithAVoltage(t *testing.T) {
	// Companion apps show no telemetry at all without a voltage on
	// the self channel, and every emitter in the reference sends one
	// first. A relay with no battery still owes them the reading.
	e := &engine{}
	body := e.telemetryBody(0xFF, meshcore.ResponseBodyBudget())
	if len(body) < 4 {
		t.Fatalf("telemetry body is %d bytes, want at least one reading", len(body))
	}
	if body[0] != telemChannelSelf {
		t.Errorf("first reading rides channel %d, want %d", body[0], telemChannelSelf)
	}
	if body[1] != meshcore.LPPVoltage {
		t.Errorf("first reading is type %d, want LPPVoltage (%d)", body[1], meshcore.LPPVoltage)
	}
}

func TestSensorTelemetryFitsTheRouteItTravels(t *testing.T) {
	// The dormant hook was the one variable producer left composing
	// blind: a sensor list that outgrew the packet was refused after
	// the asker's replay guard was already spent. The encoder is now
	// bounded to the route's budget, and running out of room is the
	// end of the list, not an error and not a truncated buffer.
	e, _ := identifiedEngine(t)
	added := 0
	e.AttachTelemetry(func(_ byte, enc *meshcore.LPPEncoder) error {
		// The review's reproduction: 38 valid GPS readings, 422 bytes
		// unbounded, against a 94-byte path-return budget.
		for i := range 38 {
			err := enc.Add(meshcore.LPPReading{
				Channel: byte(i + 2), Type: meshcore.LPPGPS,
				Value: meshcore.LPPGPSValue{Latitude: 48.85, Longitude: 2.35, Altitude: 35},
			})
			if errors.Is(err, meshcore.ErrLPPFull) {
				return err // the normal end of the list
			}
			if err != nil {
				return err
			}
			added++
		}
		return nil
	})

	budget := meshcore.PathReturnBodyBudget(63)
	body := e.telemetryBody(0xFF, budget)
	if body == nil {
		t.Fatal("a full route produced no telemetry at all")
	}
	if len(body) > budget {
		t.Fatalf("body of %d bytes for a budget of %d", len(body), budget)
	}
	if added == 38 {
		t.Error("every reading fit — the reproduction lost its premise")
	}
	// Whole records only: the buffer decodes cleanly to the base
	// readings plus however many sensor rows fit.
	rows, err := meshcore.LPPDecode(body)
	if err != nil {
		t.Fatalf("the bounded body does not decode: %v", err)
	}
	if len(rows) == 0 {
		t.Error("nothing decoded")
	}
	// And the composition the reply would attempt actually seals.
	secret := bytes.Repeat([]byte{0x11}, meshcore.SharedSecretSize)
	hash := e.id.PubKey[:meshcore.PathHashSize]
	framed := meshcore.FrameAdmin(1, body)
	if _, err := meshcore.BuildPathReturn(hash, hash, secret,
		63, make([]byte, 63), byte(meshcore.PayloadTypeResponse), framed); err != nil {
		t.Errorf("the telemetry this node would send cannot be sealed: %v", err)
	}
}
