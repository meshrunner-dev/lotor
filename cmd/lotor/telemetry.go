package main

// What the parts say, said on the air. Both closures here run on the
// engine's goroutine while it composes an answer, so they read the
// view under viewMu and never take mu — and never a bus, since a
// sampler's cache is already a copy.

import (
	"sort"

	"meshrunner.dev/pkg/meshcore"

	enginemc "meshrunner.dev/lotor/internal/protocol/meshcore"
	"meshrunner.dev/lotor/internal/sensor"
)

// telemLPP is what a quantity becomes on the wire. A quantity absent
// here is not sent — the map is the whole of what a client will see.
var telemLPP = map[sensor.Quantity]byte{
	sensor.Voltage:     meshcore.LPPVoltage,
	sensor.Current:     meshcore.LPPCurrent,
	sensor.Power:       meshcore.LPPPower,
	sensor.Temperature: meshcore.LPPTemperature,
	sensor.Humidity:    meshcore.LPPRelativeHumidity,
	sensor.Pressure:    meshcore.LPPBarometricPressure,
}

// sensorSnapshot reads every running part once, in name order, so the
// supply and the sensor channels of one reply describe the same
// instant. The view is held only long enough to copy the samplers out
// — Latest takes a lock of its own, and viewMu is a leaf that spans
// map access and nothing else.
func (m *manager) sensorSnapshot() [][]sensor.Reading {
	m.viewMu.RLock()
	names := make([]string, 0, len(m.sensorViews))
	for n := range m.sensorViews {
		names = append(names, n)
	}
	sort.Strings(names)
	smps := make([]*sensor.Sampler, 0, len(names))
	for _, n := range names {
		smps = append(smps, m.sensorViews[n])
	}
	m.viewMu.RUnlock()

	out := make([][]sensor.Reading, 0, len(smps))
	for _, smp := range smps {
		out = append(out, smp.Latest())
	}
	return out
}

// supplyVoltage answers what this node runs on: the first part that
// measures a voltage, in name order. One machine has one supply, so
// one part reports it; the day two do, the configuration will have to
// say which — a guess made here would be silent about being one.
func (m *manager) supplyVoltage() (float64, bool) {
	for _, readings := range m.sensorSnapshot() {
		for _, r := range readings {
			if r.Quantity == sensor.Voltage {
				return r.Value, true
			}
		}
	}
	return 0, false
}

// sensorTelemetry adds every part's readings to an answer, each on the
// next channel after the node's own — the reference's
// EnvironmentSensorManager rule, numbering active parts in order. A
// part added or removed therefore renumbers the ones after it, as it
// does there: the channel names a position, not a part.
//
// An asker without the environment bit gets none of this. The engine
// forces a guest's mask to zero, and honouring that here is what makes
// the gate mean anything.
func (m *manager) sensorTelemetry(permMask byte, enc *meshcore.LPPEncoder) error {
	if permMask&enginemc.TelemPermEnvironment == 0 {
		return nil
	}
	channel := enginemc.TelemChannelSelf + 1
	for _, readings := range m.sensorSnapshot() {
		for _, r := range readings {
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
