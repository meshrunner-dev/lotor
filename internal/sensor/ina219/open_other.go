//go:build !linux

package ina219

import (
	"fmt"

	"go.uber.org/zap"

	"meshrunner.dev/lotor/internal/sensor"
)

func init() {
	sensor.Register("ina219", sensor.Driver{
		Open: open, Inspect: inspect, Schema: Schema(),
	})
}

// open refuses away from Linux: the part is reached through an i2c-dev
// character device, which no other platform here offers. Declaring and
// validating one still works, so a configuration written on a laptop
// is the same configuration the board will run.
func open(cfg map[string]any, _ *zap.Logger) (sensor.Device, error) {
	s, err := decode(cfg)
	if err != nil {
		return nil, err
	}
	return nil, fmt.Errorf("ina219: %s is an i2c-dev node, which this platform has not got", s.Bus)
}
