//go:build linux

package sx126x

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.uber.org/zap"

	"meshrunner.dev/lotor/internal/radio"
	"meshrunner.dev/pkg/lora"
	"meshrunner.dev/pkg/lora/linux"
	"meshrunner.dev/pkg/lora/sx126x"
)

func init() {
	radio.Register("sx126x-spi", radio.Driver{Open: open, Inspect: Inspect, Presets: Presets()})
}

type device struct {
	r     *sx126x.Radio
	env   radio.Envelope
	held  []lora.OutputPin
	floor floorTracker
	log   *zap.Logger
}

func open(cfg map[string]any, log *zap.Logger) (radio.Device, error) {
	s, err := settingsFrom(cfg)
	if err != nil {
		return nil, err
	}
	spi, pins, held, err := attach(s)
	if err != nil {
		return nil, err
	}
	release := func() {
		for _, h := range held {
			_ = h.Close()
		}
		_ = pins.Close()
		_ = spi.Close()
	}

	tcxo, err := tcxoFrom(s.TCXO)
	if err != nil {
		release()
		return nil, err
	}
	chip, err := chipFrom(s.Chip)
	if err != nil {
		release()
		return nil, err
	}
	r, err := sx126x.Open(spi, pins, sx126x.Config{
		TCXO:           tcxo,
		UseDCDC:        s.DCDC,
		DIO2AsRFSwitch: s.DIO2RFSwitch,
		RXBoostedGain:  s.RXBoosted,
		Chip:           chip,
		MaxTxPower:     s.MaxTxPowerDBm,
	})
	if err != nil {
		release()
		return nil, err
	}
	log.Info("radio open", zap.String("driver", "sx126x-spi"), zap.String("spi", s.SPI))
	return &device{r: r, env: s.envelope(), held: held, log: log}, nil
}

// attach opens the bus and every pin, releasing what it opened on any
// failure.
func attach(s Settings) (lora.SPI, lora.Pins, []lora.OutputPin, error) {
	spi, err := linux.OpenSPI(s.SPI, s.SPIHz)
	if err != nil {
		return nil, lora.Pins{}, nil, fmt.Errorf("open %s: %w", s.SPI, err)
	}
	fail := func(err error, pins lora.Pins, held []lora.OutputPin) (lora.SPI, lora.Pins, []lora.OutputPin, error) {
		for _, h := range held {
			_ = h.Close()
		}
		_ = pins.Close()
		_ = spi.Close()
		return nil, lora.Pins{}, nil, err
	}

	pins := lora.Pins{}
	if pins.Reset, err = linux.Output(s.GPIOChip, s.ResetPin, true); err != nil {
		return fail(fmt.Errorf("reset pin %d: %w", s.ResetPin, err), pins, nil)
	}
	if pins.Busy, err = linux.Input(s.GPIOChip, s.BusyPin); err != nil {
		return fail(fmt.Errorf("busy pin %d: %w", s.BusyPin, err), pins, nil)
	}
	if pins.DIO1, err = linux.Interrupt(s.GPIOChip, s.DIO1Pin); err != nil {
		return fail(fmt.Errorf("dio1 pin %d: %w", s.DIO1Pin, err), pins, nil)
	}

	// Front-end enables are held high for the whole session; the chip
	// steers TX/RX itself over DIO2 when the board is wired that way.
	held := make([]lora.OutputPin, 0, len(s.EnablePins))
	for _, n := range s.EnablePins {
		p, err := linux.Output(s.GPIOChip, n, true)
		if err != nil {
			return fail(fmt.Errorf("enable pin %d: %w", n, err), pins, held)
		}
		held = append(held, p)
	}
	return spi, pins, held, nil
}

func (d *device) Envelope() radio.Envelope { return d.env }

func (d *device) Configure(w radio.Waveform) error {
	p, err := paramsFrom(w)
	if err != nil {
		return err
	}
	return d.r.Configure(p)
}

func (d *device) StartReceive() error { return d.r.StartReceive() }

// sampleEvery paces the idle noise-floor sampling. It matches the
// library's own poll floor, so it doubles as the lost-edge insurance
// the library's blocking receive provides.
const sampleEvery = 20 * time.Millisecond

// Receive waits for the next frame, and measures while it waits: the
// radio has exactly one owning goroutine, so the noise floor is
// sampled here, between polls, never from outside.
func (d *device) Receive(ctx context.Context) (radio.Frame, error) {
	tick := time.NewTicker(sampleEvery)
	defer tick.Stop()
	edges := d.r.Events()
	for {
		f, err := d.r.Poll()
		if err != nil {
			if errors.Is(err, sx126x.ErrCRC) || errors.Is(err, sx126x.ErrHeader) {
				return radio.Frame{}, fmt.Errorf("%w: %w", radio.ErrCorrupt, err)
			}
			return radio.Frame{}, err
		}
		if f != nil {
			return radio.Frame{
				Payload: f.Payload,
				RSSI:    f.RSSI,
				SNR:     f.SNR,
				Airtime: f.Airtime,
				At:      f.At,
			}, nil
		}
		select {
		case <-ctx.Done():
			return radio.Frame{}, ctx.Err()
		case <-edges:
		case <-tick.C:
			d.sampleFloor()
		}
	}
}

// sampleFloor takes one idle RSSI reading when it is safe to: not
// while a frame may be arriving — a tripped preamble detector already
// disqualifies the sample — and only in receive mode, which rules out
// a future transmit path by the chip's own account of itself.
func (d *device) sampleFloor() {
	preamble, header, err := d.r.ReceiveInProgress()
	if err != nil || preamble || header {
		return
	}
	rssi, err := d.r.RSSI()
	if err != nil {
		return // not receiving (ErrNotReceiving covers TX and standby)
	}
	if d.floor.sample(rssi, time.Now()) {
		nf, _ := d.floor.value()
		d.log.Debug("noise floor measured", zap.Float64("floor_dbm", nf.DBm))
	}
}

// NoiseFloor reports the last measured floor without touching the
// hardware; any goroutine may ask.
func (d *device) NoiseFloor() (radio.NoiseFloor, bool) {
	return d.floor.value()
}

func (d *device) Close() error {
	err := d.r.Close()
	for _, h := range d.held {
		_ = h.Close()
	}
	return err
}

func tcxoFrom(s string) (sx126x.TCXOVoltage, error) {
	switch s {
	case "":
		return sx126x.TCXONone, nil
	case "1.6":
		return sx126x.TCXO1V6, nil
	case "1.8":
		return sx126x.TCXO1V8, nil
	case "3.3":
		return sx126x.TCXO3V3, nil
	}
	return sx126x.TCXONone, fmt.Errorf("unsupported tcxo voltage %q", s)
}

func chipFrom(s string) (sx126x.ChipVariant, error) {
	switch s {
	case "":
		return sx126x.ChipUnset, nil
	case "sx1261":
		return sx126x.SX1261, nil
	case "sx1262":
		return sx126x.SX1262, nil
	case "sx1268":
		return sx126x.SX1268, nil
	}
	return sx126x.ChipUnset, fmt.Errorf("unknown chip %q", s)
}

func paramsFrom(w radio.Waveform) (lora.Params, error) {
	p := lora.Params{
		Frequency: w.FrequencyHz,
		Preamble:  uint16(w.Preamble),
		SyncWord:  w.SyncWord,
		CRC:       w.CRC,
	}
	switch w.SpreadingFactor {
	case 5, 6, 7, 8, 9, 10, 11, 12:
		p.SF = lora.SpreadingFactor(w.SpreadingFactor)
	default:
		return p, fmt.Errorf("spreading factor %d out of range", w.SpreadingFactor)
	}
	switch w.BandwidthHz {
	case 7810:
		p.BW = lora.BW7810
	case 15630:
		p.BW = lora.BW15630
	case 31250:
		p.BW = lora.BW31250
	case 62500:
		p.BW = lora.BW62500
	case 125000:
		p.BW = lora.BW125000
	case 250000:
		p.BW = lora.BW250000
	case 500000:
		p.BW = lora.BW500000
	default:
		return p, fmt.Errorf("unsupported bandwidth %d Hz", w.BandwidthHz)
	}
	switch w.CodingRate {
	case 5:
		p.CR = lora.CR5
	case 6:
		p.CR = lora.CR6
	case 7:
		p.CR = lora.CR7
	case 8:
		p.CR = lora.CR8
	default:
		return p, fmt.Errorf("coding rate 4/%d out of range", w.CodingRate)
	}
	return p, nil
}
