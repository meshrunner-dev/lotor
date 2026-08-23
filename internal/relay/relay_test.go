package relay

import (
	"testing"

	"go.uber.org/zap"

	"meshrunner.dev/lotor/internal/bus"
	"meshrunner.dev/lotor/internal/radio"
)

func TestNoiseHistoryConfigOverridesTheBuildDefault(t *testing.T) {
	b := bus.New()
	mk := func(choice *bool) *Relay {
		return New("r", radio.Driver{}, nil, nil, b, zap.NewNop(), choice)
	}
	if got := mk(nil).noiseHistory; got != NoiseHistoryDefault {
		t.Errorf("unset noise_history = %v, want the build default %v", got, NoiseHistoryDefault)
	}
	on, off := true, false
	if !mk(&on).noiseHistory {
		t.Error("noise_history: true ignored")
	}
	if mk(&off).noiseHistory {
		t.Error("noise_history: false ignored")
	}
}
