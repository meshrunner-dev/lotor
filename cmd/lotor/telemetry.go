package main

// What the parts say, said on the air. Both closures here run on the
// engine's goroutine while it composes an answer, so they read the
// view under viewMu and never take mu — and never a bus, since a
// sampler's cache is already a copy.

import (
	"sort"

	"meshrunner.dev/pkg/meshcore"

	"meshrunner.dev/lotor/internal/sensor"
)

// telemLPP is what a quantity becomes on the wire. A quantity absent
// here is not sent — the map is the whole of what a client will see.
var telemLPP = map[sensor.Quantity]byte{
	sensor.Voltage: meshcore.LPPVoltage,
	sensor.Current: meshcore.LPPCurrent,
	sensor.Power:   meshcore.LPPPower,
}

// sensorNames lists the running parts in a stable order, which is what
// fixes each one's channel from one answer to the next.
func (m *manager) sensorNames() []string {
	m.viewMu.RLock()
	defer m.viewMu.RUnlock()
	names := make([]string, 0, len(m.sensorViews))
	for n := range m.sensorViews {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

func (m *manager) sensorLatest(name string) []sensor.Reading {
	m.viewMu.RLock()
	smp, ok := m.sensorViews[name]
	m.viewMu.RUnlock()
	if !ok {
		return nil
	}
	return smp.Latest()
}

// supplyVoltage answers what this node runs on: the first part that
// measures a voltage, in name order. One machine has one supply, so
// one part reports it; the day two do, the configuration will have to
// say which — a guess made here would be silent about being one.
func (m *manager) supplyVoltage() (float64, bool) {
	for _, name := range m.sensorNames() {
		for _, r := range m.sensorLatest(name) {
			if r.Quantity == sensor.Voltage {
				return r.Value, true
			}
		}
	}
	return 0, false
}

// sensorTelemetry adds every part's readings to an answer, each on its
// own channel from TELEM_CHANNEL_SELF + 1 upward — the reference's
// EnvironmentSensorManager rule, so a client reading two nodes finds
// the same shape.
func (m *manager) sensorTelemetry(_ byte, enc *meshcore.LPPEncoder) error {
	channel := telemChannelSelf + 1
	for _, name := range m.sensorNames() {
		for _, r := range m.sensorLatest(name) {
			lpp, carried := telemLPP[r.Quantity]
			if !carried {
				continue
			}
			if err := enc.Add(meshcore.LPPReading{
				Channel: uint8(channel), Type: lpp, Value: r.Value,
			}); err != nil {
				return err
			}
		}
		channel++
	}
	return nil
}

// telemChannelSelf mirrors the engine's own constant: channel 1 is the
// node's, and a part's readings start after it.
const telemChannelSelf = 1
