//go:build linux

package ina219

// The bus half. What it knows about I2C it gets from internal/sensor/i2c;
// what it knows about the part is here.

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"

	"meshrunner.dev/lotor/internal/sensor"
	"meshrunner.dev/lotor/internal/sensor/i2c"
)

func init() {
	sensor.Register("ina219", sensor.Driver{
		Open: open, Inspect: inspect, Schema: Schema(),
	})
}

// The register map. A datasheet's numbers, kept as its numbers.
const (
	regConfig = 0x00
	regShunt  = 0x01
	regBus    = 0x02
)

// configContinuous is the part's own reset value: 32V bus range, the
// ±320mV shunt range, 12-bit conversions on both, sampling without
// being asked. Written rather than assumed, because whatever ran
// before us on this bus may have left the part somewhere else.
const configContinuous = 0x399F

// device is one INA219 on one bus.
type device struct {
	bus   *i2c.Device
	shunt float64
}

func open(cfg map[string]any, _ *zap.Logger) (sensor.Device, error) {
	s, err := decode(cfg)
	if err != nil {
		return nil, err
	}
	bus, err := i2c.Open(s.Bus, s.Address)
	if err != nil {
		return nil, fmt.Errorf("ina219: %w", err)
	}
	if err := bus.WriteRegister16(regConfig, configContinuous); err != nil {
		_ = bus.Close()
		return nil, fmt.Errorf("ina219: configuring: %w", err)
	}
	return &device{bus: bus, shunt: s.ShuntOhms}, nil
}

// Read takes one sample: the bus voltage and the shunt voltage, from
// which the current and the power follow. The calibration register is
// left alone — the part's own current register needs it, and computing
// from the shunt voltage instead costs one division and no state.
func (d *device) Read(ctx context.Context) ([]sensor.Reading, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	rawBus, err := d.readReg(regBus)
	if err != nil {
		return nil, fmt.Errorf("ina219: bus voltage: %w", err)
	}
	rawShunt, err := d.readReg(regShunt)
	if err != nil {
		return nil, fmt.Errorf("ina219: shunt voltage: %w", err)
	}
	return readings(rawBus, rawShunt, d.shunt, time.Now()), nil
}

func (d *device) Close() error { return d.bus.Close() }

func (d *device) readReg(reg byte) (uint16, error) {
	var buf [2]byte
	if err := d.bus.ReadRegister(reg, buf[:]); err != nil {
		return 0, err
	}
	return uint16(buf[0])<<8 | uint16(buf[1]), nil
}
