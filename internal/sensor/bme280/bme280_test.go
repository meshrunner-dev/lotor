package bme280

import (
	"math"
	"testing"
)

// The coefficient table and one raw sample, read off the part on the
// Luckfox with i2cget. The expected values were computed from the
// datasheet's formulas by hand, independently of the code below —
// which is the only way to check a transcription: comparing it with
// itself proves nothing.
func TestCompensationMatchesTheDatasheet(t *testing.T) {
	block1 := [26]byte{
		0xf7, 0x6e, 0xef, 0x67, 0x32, 0x00, 0xf2, 0x8f, 0x79, 0xd6,
		0xd0, 0x0b, 0xa3, 0x1e, 0x38, 0x00, 0xf9, 0xff, 0xb4, 0x2d,
		0xe8, 0xd1, 0x88, 0x13, 0x00, 0x4b,
	}
	block2 := [7]byte{0x6a, 0x01, 0x00, 0x14, 0x20, 0x03, 0x1e}
	cal := parseCalibration(block1, block2)

	// The table as the datasheet's names, so a wrong byte pairing
	// fails here rather than three formulas later.
	for _, c := range []struct {
		name string
		got  int64
		want int64
	}{
		{"T1", int64(cal.T1), 28407}, {"T2", int64(cal.T2), 26607}, {"T3", int64(cal.T3), 50},
		{"P1", int64(cal.P1), 36850}, {"P2", int64(cal.P2), -10631}, {"P9", int64(cal.P9), 5000},
		{"H1", int64(cal.H1), 75}, {"H2", int64(cal.H2), 362}, {"H3", int64(cal.H3), 0},
		{"H4", int64(cal.H4), 320}, {"H5", int64(cal.H5), 50}, {"H6", int64(cal.H6), 30},
	} {
		if c.got != c.want {
			t.Errorf("dig_%s = %d, want %d", c.name, c.got, c.want)
		}
	}

	// 0xF7..0xFE as they read: pressure, temperature, humidity.
	const adcT, adcP, adcH = 529584, 328192, 32564
	fine := cal.fine(adcT)
	if fine != 121930 {
		t.Errorf("t_fine = %d, want 121930", fine)
	}
	if got := celsius(fine); math.Abs(got-23.81) > 0.005 {
		t.Errorf("temperature = %.2f, want 23.81", got)
	}
	hpa, ok := cal.hectopascals(adcP, fine)
	if !ok {
		t.Fatal("the pressure formula divided by nothing")
	}
	if math.Abs(hpa-1006.53) > 0.005 {
		t.Errorf("pressure = %.2f, want 1006.53", hpa)
	}
	// Tolerance below the smallest transcription slip this formula
	// can hide: dropping the datasheet's +8192 moves the answer by
	// 0.002 %RH, which a coarser check would wave through.
	if got := cal.humidity(adcH, fine); math.Abs(got-66.668945) > 0.0005 {
		t.Errorf("humidity = %.6f, want 66.668945", got)
	}
}

func TestNegativeHumidityCoefficientsKeepTheirSign(t *testing.T) {
	// dig_H4 and dig_H5 are signed twelve-bit, and the part on this
	// bench happens to carry positive ones — so the table above
	// cannot see a lost sign. These bytes are chosen to set bit 11.
	var b [7]byte
	b[3], b[4], b[5] = 0x90, 0x24, 0x88 // 0xE4, 0xE5, 0xE6
	cal := parseCalibration([26]byte{}, b)
	// 0x90<<4 | 0x4 = 2308, which is 2308-4096 once the sign is read.
	if cal.H4 != -1788 {
		t.Errorf("dig_H4 = %d, want -1788", cal.H4)
	}
	// 0x88<<4 | 0x2 = 2178 -> 2178-4096.
	if cal.H5 != -1918 {
		t.Errorf("dig_H5 = %d, want -1918", cal.H5)
	}
	// And a positive one stays itself.
	b[3], b[4], b[5] = 0x14, 0x00, 0x03
	if cal := parseCalibration([26]byte{}, b); cal.H4 != 320 || cal.H5 != 48 {
		t.Errorf("dig_H4=%d dig_H5=%d, want 320 and 48", cal.H4, cal.H5)
	}
}

func TestOnlyABME280IsAccepted(t *testing.T) {
	// A BMP280 answers on the same two addresses and measures no
	// humidity, so opening one as a BME280 would promise a reading
	// that does not exist.
	if !chipIDMatches(0x60) {
		t.Error("a BME280's own id was refused")
	}
	for _, id := range []byte{0x58, 0x00, 0xFF} {
		if chipIDMatches(id) {
			t.Errorf("id 0x%02x was taken for a BME280", id)
		}
	}
}

func TestDecodeRefusesWhatNoPartCouldBe(t *testing.T) {
	const bus = "/dev/i2c-2"
	for _, c := range []struct {
		what string
		cfg  map[string]any
		ok   bool
	}{
		{"the bus alone, the default address", map[string]any{"i2c": bus}, true},
		{"no bus at all", map[string]any{}, false},
		{"a bus that is not a path", map[string]any{"i2c": 2}, false},
		// SDO is strapped one way or the other; nothing sits between.
		{"SDO low", map[string]any{"i2c": bus, "address": addrLow}, true},
		{"SDO high", map[string]any{"i2c": bus, "address": addrHigh}, true},
		{"one below", map[string]any{"i2c": bus, "address": addrLow - 1}, false},
		{"one above", map[string]any{"i2c": bus, "address": addrHigh + 1}, false},
		{"the INA219's address", map[string]any{"i2c": bus, "address": 0x40}, false},
		{"an address that is not a number", map[string]any{"i2c": bus, "address": "0x76"}, false},
	} {
		err := inspect(c.cfg)
		if c.ok && err != nil {
			t.Errorf("%s: %v", c.what, err)
		}
		if !c.ok && err == nil {
			t.Errorf("%s: accepted %v", c.what, c.cfg)
		}
	}
}

func TestABlankCalibrationIsNotATable(t *testing.T) {
	// A short or mis-addressed calibration read leaves zeros, and
	// zeros compensate to 0 °C, 0 %RH and no pressure — three answers
	// that each look like a reading.
	if parseCalibration([26]byte{}, [7]byte{}).usable() {
		t.Error("a blank table was taken for a part's own")
	}
	// The one from the bench says something.
	block1 := [26]byte{
		0xf7, 0x6e, 0xef, 0x67, 0x32, 0x00, 0xf2, 0x8f, 0x79, 0xd6,
		0xd0, 0x0b, 0xa3, 0x1e, 0x38, 0x00, 0xf9, 0xff, 0xb4, 0x2d,
		0xe8, 0xd1, 0x88, 0x13, 0x00, 0x4b,
	}
	if !parseCalibration(block1, [7]byte{0x6a, 0x01, 0x00, 0x14, 0x20, 0x03, 0x1e}).usable() {
		t.Error("a real part's table was refused")
	}
}
