//go:build !linux

package i2c

// The same door, on a platform that has no i2c-dev to open. This file
// exists so the package does: with only a linux file, naming the
// package anywhere else is a build failure rather than a refusal, and
// `go test ./internal/sensor/i2c/` reports "build constraints exclude
// all Go files" instead of running.

import "errors"

// Device is one part on one bus, which this platform cannot offer.
// Empty on purpose: Open never returns one, so what the two files
// share is the exported surface and not the innards behind it.
type Device struct{}

// Open refuses: i2c-dev is a Linux character device, and a
// configuration naming one is still worth validating on a laptop.
func Open(bus string, _ int) (*Device, error) {
	return nil, errors.New("i2c: " + bus + " is an i2c-dev node, which this platform has not got")
}

// ReadRegister never runs: no Device is ever handed out here.
func (d *Device) ReadRegister(byte, []byte) error { return errUnsupported }

// WriteRegister never runs.
func (d *Device) WriteRegister(byte, byte) error { return errUnsupported }

// WriteRegister16 never runs.
func (d *Device) WriteRegister16(byte, uint16) error { return errUnsupported }

// Close never runs.
func (d *Device) Close() error { return errUnsupported }

var errUnsupported = errors.New("i2c: this platform has no i2c-dev")
