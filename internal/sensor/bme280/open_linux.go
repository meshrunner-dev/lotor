//go:build linux

package bme280

// The bus half. What it knows about I2C it gets from
// internal/sensor/i2c; what it knows about the part is here.

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.uber.org/zap"

	"meshrunner.dev/lotor/internal/sensor"
	"meshrunner.dev/lotor/internal/sensor/i2c"
)

func init() {
	sensor.Register("bme280", sensor.Driver{
		Open: open, Inspect: inspect, Schema: Schema(),
	})
}

// The register map, as the datasheet numbers it.
const (
	regID       = 0xD0
	regCtrlHum  = 0xF2
	regStatus   = 0xF3
	regCtrlMeas = 0xF4
	regConfig   = 0xF5
	regData     = 0xF7
	regCalib1   = 0x88 // 26 bytes: dig_T*, dig_P*, dig_H1
	regCalib2   = 0xE1 // 7 bytes: dig_H2..dig_H6
)

// What is written to take one sample. The part sleeps between them,
// which suits a cadence measured in seconds: forced mode wakes it,
// converts once, and lets it sleep again.
const (
	oversampling1  = 0x01 // ×1 on each channel: 8ms, and finer than the air carries
	ctrlMeasForced = oversampling1<<5 | oversampling1<<2 | 0x01
	// filterOff matters in forced mode: the IIR filter would carry
	// the part's last reading into this one, and its last reading may
	// be from before it slept.
	filterOff = 0x00
	// statusMeasuring is set while a conversion runs.
	statusMeasuring = 0x08
)

// How long one conversion is waited for. The part is polled rather
// than slept against, so a fast conversion is not paid for in full —
// but the bound is finite, and it is sized for the oversampling this
// driver asks for. The datasheet puts ×1 on all three channels under
// 10ms; ×16 on all three is nearer 112ms, so raising oversampling
// means raising this with it or every sample times out.
const (
	pollEvery    = 2 * time.Millisecond
	pollAttempts = 25 // 50ms, five times what ×1 needs
)

type device struct {
	bus *i2c.Device
	cal calibration
}

func open(cfg map[string]any, _ *zap.Logger) (sensor.Device, error) {
	s, err := decode(cfg)
	if err != nil {
		return nil, err
	}
	bus, err := i2c.Open(s.Bus, s.Address)
	if err != nil {
		return nil, fmt.Errorf("bme280: %w", err)
	}
	d := &device{bus: bus, cal: calibration{}}
	// Asked before anything else: a BMP280 answers on the same
	// addresses and measures no humidity, and opening it as a BME280
	// would promise a reading that does not exist.
	var id [1]byte
	if err := bus.ReadRegister(regID, id[:]); err != nil {
		_ = bus.Close()
		return nil, fmt.Errorf("bme280: reading the part's id: %w", err)
	}
	if !chipIDMatches(id[0]) {
		_ = bus.Close()
		return nil, fmt.Errorf("bme280: the part at 0x%02x answers id 0x%02x, not a BME280's 0x%02x",
			s.Address, id[0], chipID)
	}
	var block1 [26]byte
	var block2 [7]byte
	if err := bus.ReadRegister(regCalib1, block1[:]); err != nil {
		_ = bus.Close()
		return nil, fmt.Errorf("bme280: reading the calibration: %w", err)
	}
	if err := bus.ReadRegister(regCalib2, block2[:]); err != nil {
		_ = bus.Close()
		return nil, fmt.Errorf("bme280: reading the calibration: %w", err)
	}
	d.cal = parseCalibration(block1, block2)
	// Refused rather than opened: a table that says nothing yields
	// 0 °C, 0 %RH and no pressure at all, three answers that each
	// look like a reading. The sampler's retry and the console's
	// cause line are already there to say what happened.
	if !d.cal.usable() {
		_ = bus.Close()
		return nil, errors.New("bme280: the calibration table reads blank — the part answered, its coefficients did not")
	}
	return d, nil
}

// Read wakes the part for one conversion and compensates what it
// measured. The three quantities come from one sample because the
// temperature carries into the other two.
func (d *device) Read(ctx context.Context) ([]sensor.Reading, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// ctrl_hum only takes effect on the following ctrl_meas write,
	// which is why the order matters and the datasheet says so.
	if err := d.bus.WriteRegister(regCtrlHum, oversampling1); err != nil {
		return nil, fmt.Errorf("bme280: setting humidity oversampling: %w", err)
	}
	if err := d.bus.WriteRegister(regConfig, filterOff); err != nil {
		return nil, fmt.Errorf("bme280: setting the filter: %w", err)
	}
	if err := d.bus.WriteRegister(regCtrlMeas, ctrlMeasForced); err != nil {
		return nil, fmt.Errorf("bme280: asking for a measurement: %w", err)
	}
	if err := d.settle(ctx); err != nil {
		return nil, err
	}
	var raw [8]byte
	if err := d.bus.ReadRegister(regData, raw[:]); err != nil {
		return nil, fmt.Errorf("bme280: reading the sample: %w", err)
	}
	adcP := int32(raw[0])<<12 | int32(raw[1])<<4 | int32(raw[2])>>4
	adcT := int32(raw[3])<<12 | int32(raw[4])<<4 | int32(raw[5])>>4
	adcH := int32(raw[6])<<8 | int32(raw[7])

	at := time.Now()
	fine := d.cal.fine(adcT)
	out := []sensor.Reading{
		{Quantity: sensor.Temperature, Value: celsius(fine), At: at},
		{Quantity: sensor.Humidity, Value: d.cal.humidity(adcH, fine), At: at},
	}
	// A part whose table divides by nothing reports what it can
	// rather than nothing at all.
	if hpa, ok := d.cal.hectopascals(adcP, fine); ok {
		out = append(out, sensor.Reading{Quantity: sensor.Pressure, Value: hpa, At: at})
	}
	return out, nil
}

// settle waits for the conversion to finish. Polled rather than slept
// against, because the time depends on the oversampling and a fixed
// wait is either too short or spent doing nothing — but it waits
// before it looks. The measuring bit goes high when the part starts
// converting, not when the write asking for it lands, so a status
// read that beats the start sees an idle part and lets the caller
// read the previous sample as though it were this one.
func (d *device) settle(ctx context.Context) error {
	for range pollAttempts {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(pollEvery):
		}
		var st [1]byte
		if err := d.bus.ReadRegister(regStatus, st[:]); err != nil {
			return fmt.Errorf("bme280: reading the status: %w", err)
		}
		if st[0]&statusMeasuring == 0 {
			return nil
		}
	}
	return errors.New("bme280: the measurement never finished")
}

func (d *device) Close() error { return d.bus.Close() }
