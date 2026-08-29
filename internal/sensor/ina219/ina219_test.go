package ina219

import (
	"math"
	"testing"
	"time"

	"meshrunner.dev/lotor/internal/sensor"
)

func find(t *testing.T, rs []sensor.Reading, q sensor.Quantity) float64 {
	t.Helper()
	for _, r := range rs {
		if r.Quantity == q {
			return r.Value
		}
	}
	t.Fatalf("no %s among %+v", q, rs)
	return 0
}

func TestTheDatasheetsArithmetic(t *testing.T) {
	at := time.Date(2026, 8, 29, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		what             string
		rawBus, rawShunt uint16
		shunt            float64
		volts, amps      float64
	}{
		{
			// 12.0V is 3000 counts of 4mV, and the reading carries
			// them in its top thirteen bits.
			what: "a charged supply", rawBus: 3000 << 3, rawShunt: 1000, shunt: 0.1,
			volts: 12.0, amps: 0.1, // 1000 × 10µV = 10mV over 0.1Ω
		},
		{
			// The flag bits must not reach the voltage: CNVR and OVF
			// set would add 7 counts if they were read as value.
			what: "flags set under the same voltage", rawBus: 3000<<3 | 0b011, rawShunt: 0, shunt: 0.1,
			volts: 12.0, amps: 0,
		},
		{
			// A discharging battery pushes current the other way, and
			// the shunt register is two's complement.
			what: "current flowing backwards", rawBus: 3000 << 3, rawShunt: 0xFC18, shunt: 0.1,
			volts: 12.0, amps: -0.1, // -1000 counts
		},
		{
			// A ten-times smaller resistor reads ten times the current
			// for the same drop across it.
			what: "a 0.01Ω shunt", rawBus: 3000 << 3, rawShunt: 1000, shunt: 0.01,
			volts: 12.0, amps: 1.0,
		},
	}
	for _, c := range cases {
		got := readings(c.rawBus, c.rawShunt, c.shunt, at)
		if v := find(t, got, sensor.Voltage); math.Abs(v-c.volts) > 1e-9 {
			t.Errorf("%s: volts = %v, want %v", c.what, v, c.volts)
		}
		if a := find(t, got, sensor.Current); math.Abs(a-c.amps) > 1e-9 {
			t.Errorf("%s: amps = %v, want %v", c.what, a, c.amps)
		}
		// Power is the product, never a register of its own.
		if p := find(t, got, sensor.Power); math.Abs(p-c.volts*c.amps) > 1e-9 {
			t.Errorf("%s: watts = %v, want %v", c.what, p, c.volts*c.amps)
		}
		for _, r := range got {
			if !r.At.Equal(at) {
				t.Errorf("%s: %s carried %v", c.what, r.Quantity, r.At)
			}
		}
	}
}

func TestDecodeRefusesWhatNoPartCouldBe(t *testing.T) {
	const bus = "/dev/i2c-2"
	cases := []struct {
		what string
		cfg  map[string]any
		ok   bool
	}{
		{"the bus alone, defaults for the rest", map[string]any{"i2c": bus}, true},
		{"no bus at all", map[string]any{}, false},
		{"a bus that is not a path", map[string]any{"i2c": 2}, false},

		// The part answers on sixteen addresses and no others.
		{"the lowest address", map[string]any{"i2c": bus, "address": addrLow}, true},
		{"the highest address", map[string]any{"i2c": bus, "address": addrHigh}, true},
		{"one below the range", map[string]any{"i2c": bus, "address": addrLow - 1}, false},
		{"one above the range", map[string]any{"i2c": bus, "address": addrHigh + 1}, false},
		{"an address that is not a number", map[string]any{"i2c": bus, "address": "0x40"}, false},

		// A shunt divides the measured drop, so what it may be is
		// what keeps the quotient a current.
		{"the smallest shunt anyone fits", map[string]any{"i2c": bus, "shunt_ohms": shuntMinOhms}, true},
		{"the largest", map[string]any{"i2c": bus, "shunt_ohms": shuntMaxOhms}, true},
		{"zero divides by nothing", map[string]any{"i2c": bus, "shunt_ohms": 0.0}, false},
		{"negative describes no resistor", map[string]any{"i2c": bus, "shunt_ohms": -0.1}, false},
		{"a millionth of an ohm reports thousands of amps", map[string]any{"i2c": bus, "shunt_ohms": 1e-12}, false},
		{"larger than any current monitor", map[string]any{"i2c": bus, "shunt_ohms": 100.0}, false},
		// NaN fails every comparison, so a bare sign check lets it by.
		{"not a number", map[string]any{"i2c": bus, "shunt_ohms": math.NaN()}, false},
		{"infinite", map[string]any{"i2c": bus, "shunt_ohms": math.Inf(1)}, false},
	}
	for _, c := range cases {
		err := inspect(c.cfg)
		if c.ok && err != nil {
			t.Errorf("%s: %v", c.what, err)
		}
		if !c.ok && err == nil {
			t.Errorf("%s: accepted %v", c.what, c.cfg)
		}
	}
}
