// Package bme280 drives the Bosch BME280 over I2C: the part a machine
// reads to say what the air around it is doing — temperature, relative
// humidity and barometric pressure.
//
// The bus code is Linux's alone, in open_linux.go. What lives here is
// what the datasheet calls compensation: the part measures uncalibrated
// integers and ships a table of coefficients to turn them into
// quantities, and that arithmetic is where a driver like this goes
// wrong. Kept away from the bus, it can be checked on any machine.
package bme280

import (
	"errors"
	"fmt"

	"meshrunner.dev/lotor/internal/schema"
)

// The part answers on one of two addresses, chosen by strapping SDO.
const (
	addrLow     = 0x76
	addrHigh    = 0x77
	addrDefault = addrLow
)

// chipID is what register 0xD0 reads on a BME280. A BMP280 answers
// 0x58 to the same question and has no humidity, so the value is
// checked rather than assumed: promising a humidity the part cannot
// measure would be worse than refusing to open.
const chipID = 0x60

// chipIDMatches says whether a part is the one this driver speaks to.
// It exists so the constant is read on every platform: only the bus
// half compares it, and a value nothing consults is a value nothing
// keeps true.
func chipIDMatches(id byte) bool { return id == chipID }

// The attribute names, spelled once.
const (
	attrI2C     = "i2c"
	attrAddress = "address"
)

// Schema describes every attribute this driver accepts.
func Schema() []schema.Attr {
	return []schema.Attr{
		{Name: attrI2C, Type: schema.String,
			Doc: "the I2C device node, e.g. /dev/i2c-2"},
		{Name: attrAddress, Type: schema.Int,
			Doc: "the 7-bit I2C address, 0x76 or 0x77 as SDO is strapped"},
	}
}

// settings is one resolved configuration.
type settings struct {
	Bus     string
	Address int
}

// inspect validates a configuration without touching hardware.
func inspect(cfg map[string]any) error {
	_, err := decode(cfg)
	return err
}

func decode(cfg map[string]any) (settings, error) {
	s := settings{Bus: "", Address: addrDefault}
	if v, ok := cfg[attrI2C]; ok {
		text, isText := v.(string)
		if !isText {
			return s, fmt.Errorf("bme280: i2c wants a device node, not %v", v)
		}
		s.Bus = text
	}
	if s.Bus == "" {
		return s, errors.New("bme280: i2c names no device node")
	}
	if v, ok := cfg[attrAddress]; ok {
		n, isNum := asInt(v)
		if !isNum {
			return s, fmt.Errorf("bme280: address wants a number, not %v", v)
		}
		s.Address = n
	}
	// Two addresses, not a range: SDO is strapped one way or the
	// other, and nothing sits between them.
	if s.Address != addrLow && s.Address != addrHigh {
		return s, fmt.Errorf("bme280: address 0x%02x — the part answers on 0x%02x or 0x%02x",
			s.Address, addrLow, addrHigh)
	}
	return s, nil
}

// The store returns numbers as whatever JSON round-tripping left them.
func asInt(v any) (int, bool) {
	switch n := v.(type) {
	case int:
		return n, true
	case int64:
		return int(n), true
	case float64:
		return int(n), true
	}
	return 0, false
}

// calibration is the coefficient table the part ships in ROM. The
// names are the datasheet's, kept as its names: every line below is a
// transcription of formulas that are only checkable against it.
type calibration struct {
	T1                             uint16
	T2, T3                         int16
	P1                             uint16
	P2, P3, P4, P5, P6, P7, P8, P9 int16
	H1                             uint8
	H2                             int16
	H3                             uint8
	H4, H5                         int16
	H6                             int8
}

// parseCalibration reads the two blocks the part keeps its table in:
// 26 bytes from 0x88 and 7 from 0xE1. H4 and H5 are twelve-bit values
// sharing a byte, which is why they are picked apart by hand.
func parseCalibration(a [26]byte, b [7]byte) calibration {
	u16 := func(lo, hi byte) uint16 { return uint16(hi)<<8 | uint16(lo) }
	i16 := func(lo, hi byte) int16 { return int16(u16(lo, hi)) }
	return calibration{
		T1: u16(a[0], a[1]), T2: i16(a[2], a[3]), T3: i16(a[4], a[5]),
		P1: u16(a[6], a[7]), P2: i16(a[8], a[9]), P3: i16(a[10], a[11]),
		P4: i16(a[12], a[13]), P5: i16(a[14], a[15]), P6: i16(a[16], a[17]),
		P7: i16(a[18], a[19]), P8: i16(a[20], a[21]), P9: i16(a[22], a[23]),
		H1: a[25],
		H2: i16(b[0], b[1]),
		H3: b[2],
		// H4 is the high eight bits of 0xE4 with the low nibble of
		// 0xE5; H5 is all of 0xE6 with the high nibble of 0xE5. Both
		// are signed twelve-bit, so the sign has to be carried out of
		// bit 11 by hand — Bosch's driver does it by casting the high
		// byte to int8_t before shifting, which is the same thing.
		H4: signed12(int32(b[3])<<4 | int32(b[4]&0x0F)),
		H5: signed12(int32(b[5])<<4 | int32(b[4])>>4),
		H6: int8(b[6]),
	}
}

// signed12 reads a twelve-bit two's-complement value out of the
// eighteen bits a pair of registers hands over.
func signed12(v int32) int16 {
	if v > 2047 {
		v -= 4096
	}
	return int16(v)
}

// usable reports whether the table says anything. dig_P1 divides the
// pressure formula, so a zero there is the datasheet's own signal that
// the coefficients are not a part's — which is what a short or
// mis-addressed calibration read leaves behind.
func (c calibration) usable() bool { return c.P1 != 0 }

// fine carries the temperature reading into the pressure and humidity
// formulas, which the datasheet calls t_fine. It is why a sample is
// compensated as a whole and not one quantity at a time.
func (c calibration) fine(adcT int32) int64 {
	t1, t2, t3 := int64(c.T1), int64(c.T2), int64(c.T3)
	a := int64(adcT)
	v1 := ((a >> 3) - (t1 << 1)) * t2 >> 11
	v2 := (((a >> 4) - t1) * ((a >> 4) - t1) >> 12) * t3 >> 14
	return v1 + v2
}

// celsius is the compensated temperature. The datasheet computes
// hundredths of a degree; the seam above speaks degrees.
func celsius(fine int64) float64 {
	return float64((fine*5+128)>>8) / 100
}

// hectopascals is the compensated pressure. The datasheet's 64-bit
// formula yields Q24.8 pascals, and LPP carries hectopascals, so the
// conversion happens here at the driver's own edge.
func (c calibration) hectopascals(adcP int32, fine int64) (float64, bool) {
	v1 := fine - 128000
	v2 := v1 * v1 * int64(c.P6)
	v2 += (v1 * int64(c.P5)) << 17
	v2 += int64(c.P4) << 35
	v1 = ((v1 * v1 * int64(c.P3)) >> 8) + ((v1 * int64(c.P2)) << 12)
	v1 = (((int64(1) << 47) + v1) * int64(c.P1)) >> 33
	if v1 == 0 {
		// The datasheet's own guard: the table can describe a part
		// whose pressure channel divides by nothing.
		return 0, false
	}
	p := int64(1048576 - adcP)
	p = (((p << 31) - v2) * 3125) / v1
	v1 = (int64(c.P9) * (p >> 13) * (p >> 13)) >> 25
	v2 = (int64(c.P8) * p) >> 19
	p = ((p + v1 + v2) >> 8) + (int64(c.P7) << 4)
	// Q24.8 pascals to hectopascals.
	return float64(p) / 256 / 100, true
}

// humidity is the compensated relative humidity, in percent.
func (c calibration) humidity(adcH int32, fine int64) float64 {
	v := fine - 76800
	v = (((int64(adcH)<<14 - int64(c.H4)<<20 - int64(c.H5)*v) + 16384) >> 15) *
		((((((v*int64(c.H6))>>10)*(((v*int64(c.H3))>>11)+32768))>>10+2097152)*int64(c.H2) + 8192) >> 14)
	v -= ((((v >> 15) * (v >> 15)) >> 7) * int64(c.H1)) >> 4
	// The datasheet clamps: the formula can leave the range a
	// hygrometer has.
	if v < 0 {
		v = 0
	}
	if v > 419430400 {
		v = 419430400
	}
	return float64(v>>12) / 1024
}
