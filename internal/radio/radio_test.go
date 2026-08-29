package radio

import (
	"strings"
	"testing"
)

func band() Waveform {
	return Waveform{
		FrequencyHz: 869_618_000, SpreadingFactor: 8, BandwidthHz: 62_500,
		CodingRate: 8, Preamble: 32, SyncWord: 0x12, CRC: true,
	}
}

func TestEnvelopeJudgesFrequencyAndPower(t *testing.T) {
	full := Envelope{
		MaxTxPowerDBm: 22, MaxTxPowerSet: true, ChipMinDBm: -9, ChipMaxDBm: 22,
		FreqRangeLowHz: 850_000_000, FreqRangeHiHz: 930_000_000,
	}
	cases := []struct {
		name     string
		env      Envelope
		w        Waveform
		dbm      int8
		explicit bool
		want     string // empty: allowed
	}{
		{"the band and a power the board serves", full, band(), 6, true, ""},
		{"auto under a reachable ceiling", full, band(), 0, false, ""},
		{"at the ceiling exactly", full, band(), 22, true, ""},
		{"one dB over the ceiling", full, band(), 23, true, "cap"},
		{"below what the part can key", full, band(), -10, true, "outside what this part can key"},
		{"below the band", full, waveAt(840_000_000), 0, false, "outside the radio's range"},
		{"above the band", full, waveAt(940_000_000), 0, false, "outside the radio's range"},
		// An undeclared ceiling judges no power at all: the transmit
		// gate refuses that case in its own words.
		{"no ceiling declared", Envelope{}, band(), 30, true, ""},
		// A ceiling no part can key, reached through auto.
		{"auto resolving past the part", Envelope{
			MaxTxPowerDBm: 127, MaxTxPowerSet: true, ChipMinDBm: -9, ChipMaxDBm: 22,
		}, band(), 0, false, "auto resolves"},
		// An undeclared part leaves the range unchecked.
		{"part unknown", Envelope{MaxTxPowerDBm: 127, MaxTxPowerSet: true},
			band(), 0, false, ""},
	}
	for _, c := range cases {
		err := c.env.Permits(c.w, c.dbm, c.explicit)
		switch {
		case c.want == "" && err != nil:
			t.Errorf("%s: refused: %v", c.name, err)
		case c.want != "" && err == nil:
			t.Errorf("%s: allowed", c.name)
		case c.want != "" && err != nil && !strings.Contains(err.Error(), c.want):
			t.Errorf("%s: refused for the wrong reason: %v", c.name, err)
		}
	}
}

func TestAllowsIgnoresPowerEntirely(t *testing.T) {
	// The waveform half alone, for callers with nothing to say about
	// power: an unbounded band allows any frequency.
	e := Envelope{MaxTxPowerDBm: 5, MaxTxPowerSet: true}
	if err := e.Allows(waveAt(2_400_000_000)); err != nil {
		t.Errorf("an undeclared band refused a frequency: %v", err)
	}
	bounded := Envelope{FreqRangeLowHz: 850_000_000, FreqRangeHiHz: 930_000_000}
	if err := bounded.Allows(band()); err != nil {
		t.Errorf("the band it declares: %v", err)
	}
	if err := bounded.Allows(waveAt(433_000_000)); err == nil {
		t.Error("a frequency outside the declared band was allowed")
	}
}

func waveAt(hz uint32) Waveform {
	w := band()
	w.FrequencyHz = hz
	return w
}
