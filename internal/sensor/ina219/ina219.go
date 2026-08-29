// Package ina219 drives the TI INA219 current and voltage monitor over
// I2C: the part a machine reads to say honestly what its supply is
// doing — one of the few that does measure the host itself.
//
// Open is absent for now — this package declares what the part is and
// what a configuration for it must say, so an operator can wire one up
// and have it validated before any bus transaction exists to run.
package ina219

import (
	"errors"
	"fmt"

	"meshrunner.dev/lotor/internal/schema"
	"meshrunner.dev/lotor/internal/sensor"
)

// The part answers on one of sixteen addresses, selected by strapping
// A0 and A1; 0x40 is both the datasheet's default and what every
// breakout ships with unstrapped.
const (
	addrLow     = 0x40
	addrHigh    = 0x4f
	addrDefault = addrLow
)

// shuntDefault is the resistor almost every INA219 breakout carries.
// It is a property of the board, not of the chip — the chip measures a
// voltage across whatever is fitted, and only the operator knows what
// that is, which is why this is configuration rather than a constant.
const shuntDefault = 0.1

// The attribute names, spelled once: the schema declares them and the
// decoder reads them, and a typo between the two is silent.
const (
	attrI2C     = "i2c"
	attrAddress = "address"
	attrShunt   = "shunt_ohms"
)

func init() {
	sensor.Register("ina219", sensor.Driver{
		Open:    nil, // the bus transaction lands in its own change
		Inspect: inspect,
		Schema:  Schema(),
	})
}

// A band preset earns its keep because a relay switches between bands
// and each keeps its own tuning; a part has one configuration, decided
// by what is soldered to the board. The two values a preset could
// carry here are the defaults decode already applies, and the bus
// node — which belongs to the attachment, not to any resistor — would
// have been stored under the preset's name and vanished the moment
// somebody switched.

// Schema describes every attribute this driver accepts, declared
// beside the code that reads them.
func Schema() []schema.Attr {
	return []schema.Attr{
		{Name: attrI2C, Type: schema.String,
			Doc: "the I2C device node, e.g. /dev/i2c-2"},
		{Name: attrAddress, Type: schema.Int,
			Doc: "the 7-bit I2C address, 0x40..0x4f as A0/A1 are strapped"},
		{Name: attrShunt, Type: schema.Float,
			Doc: "the shunt resistor fitted on the board, ohms (0.1 on most breakouts)"},
	}
}

// settings is one resolved configuration, decoded once so the checks
// below and the bus code to come read the same fields.
type settings struct {
	Bus       string
	Address   int
	ShuntOhms float64
}

// inspect validates a configuration without touching hardware — the
// config loader's dry run, and what refuses a typo on a machine where
// no INA219 is attached.
func inspect(cfg map[string]any) error {
	_, err := decode(cfg)
	return err
}

func decode(cfg map[string]any) (settings, error) {
	s := settings{Bus: "", Address: addrDefault, ShuntOhms: shuntDefault}
	if v, ok := cfg[attrI2C]; ok {
		text, isText := v.(string)
		if !isText {
			return s, fmt.Errorf("ina219: i2c wants a device node, not %v", v)
		}
		s.Bus = text
	}
	if s.Bus == "" {
		return s, errors.New("ina219: i2c names no device node")
	}
	if v, ok := cfg[attrAddress]; ok {
		n, isNum := asInt(v)
		if !isNum {
			return s, fmt.Errorf("ina219: address wants a number, not %v", v)
		}
		s.Address = n
	}
	if s.Address < addrLow || s.Address > addrHigh {
		return s, fmt.Errorf("ina219: address 0x%02x is outside the part's 0x%02x..0x%02x",
			s.Address, addrLow, addrHigh)
	}
	if v, ok := cfg[attrShunt]; ok {
		f, isNum := asFloat(v)
		if !isNum {
			return s, fmt.Errorf("ina219: shunt_ohms wants a resistance, not %v", v)
		}
		s.ShuntOhms = f
	}
	// A shunt of zero would divide the current computation by nothing;
	// a negative one describes no resistor that exists.
	if s.ShuntOhms <= 0 {
		return s, fmt.Errorf("ina219: shunt_ohms %v — a resistor is a positive number of ohms", s.ShuntOhms)
	}
	return s, nil
}

// The store returns numbers as whatever JSON round-tripping left them,
// so both integer shapes and float64 arrive here for the same value.
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

func asFloat(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	}
	return 0, false
}
