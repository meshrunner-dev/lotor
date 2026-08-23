//go:build !linux

package sx126x

import (
	"errors"

	"go.uber.org/zap"

	"meshrunner.dev/lotor/internal/radio"
)

func init() {
	radio.Register("sx126x-spi", radio.Driver{Open: open, Inspect: Inspect, Presets: Presets()})
}

func open(cfg map[string]any, _ *zap.Logger) (radio.Device, error) {
	if _, err := settingsFrom(cfg); err != nil {
		return nil, err
	}
	return nil, errors.New("sx126x-spi: SPI and GPIO require linux")
}
