//go:build linux

package sx126x

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"meshrunner.dev/lotor/internal/logging"
	"meshrunner.dev/lotor/internal/radio"
	"meshrunner.dev/pkg/lora"
	"meshrunner.dev/pkg/lora/linux"
	"meshrunner.dev/pkg/lora/sx126x"
)

func init() {
	radio.Register("sx126x-spi", radio.Driver{
		Open: open, Inspect: Inspect, CheckTransmit: checkTransmit,
		CheckWaveform: CheckWaveform,
		Presets:       Presets(), Schema: Schema(),
	})
}

type device struct {
	r    *sx126x.Radio
	env  radio.Envelope
	held []lora.OutputPin
	// dio1 is kept for its level: the line stays high while an IRQ is
	// latched, so reading it before a sleep catches any transition the
	// edge path missed — a GPIO read, never the SPI bus.
	dio1   lora.InterruptPin
	floor  floorTracker
	params lora.Params
	log    *zap.Logger
	// watchdog optionally bounds a transition degraded while asleep in
	// the rest phase; nil channel — the default — never fires.
	wdTicker *time.Ticker
	watchdog <-chan time.Time
	// stats caches the chip's counters, refreshed by the receive loop
	// so readers never touch the single-owner hardware.
	stats     atomic.Value
	statsRead time.Time
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
		MaxTxPower:     s.libraryTxCap(),
	})
	if err != nil {
		release()
		return nil, err
	}
	log.Info("radio open", zap.String("driver", "sx126x-spi"), zap.String("spi", s.SPI))
	d := &device{r: r, env: s.envelope(), held: held, dio1: pins.DIO1, log: log}
	if s.DIO1Watchdog > 0 {
		d.wdTicker = time.NewTicker(s.DIO1Watchdog)
		d.watchdog = d.wdTicker.C
	}
	return d, nil
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
	if pins.Reset, err = linux.Output(s.ResetPin.Chip, s.ResetPin.Offset, true); err != nil {
		return fail(fmt.Errorf("reset pin %s: %w", s.ResetPin, err), pins, nil)
	}
	if pins.Busy, err = linux.Input(s.BusyPin.Chip, s.BusyPin.Offset); err != nil {
		return fail(fmt.Errorf("busy pin %s: %w", s.BusyPin, err), pins, nil)
	}
	if pins.DIO1, err = linux.Interrupt(s.DIO1Pin.Chip, s.DIO1Pin.Offset); err != nil {
		return fail(fmt.Errorf("dio1 pin %s: %w", s.DIO1Pin, err), pins, nil)
	}

	// Front-end enables are held high for the whole session; the chip
	// steers TX/RX itself over DIO2 when the board is wired that way.
	held := make([]lora.OutputPin, 0, len(s.EnablePins))
	for _, n := range s.EnablePins {
		p, err := linux.Output(n.Chip, n.Offset, true)
		if err != nil {
			return fail(fmt.Errorf("enable pin %s: %w", n, err), pins, held)
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
	if err := d.r.Configure(p); err != nil {
		return err
	}
	// Kept for airtime arithmetic; read-only once the engine runs.
	d.params = p
	return nil
}

// Airtime is pure arithmetic over the configured waveform.
func (d *device) Airtime(bytes int) time.Duration { return d.params.Airtime(bytes) }

// AssessChannel is the LBT verdict: the optional RSSI stage first —
// cheapest, and only meaningful once a floor is known — then the
// chip's own channel-activity detection.
func (d *device) AssessChannel(ctx context.Context, thresholdDB float64) (bool, error) {
	if thresholdDB > 0 {
		if nf, ok := d.floor.value(); ok {
			rssi, err := d.r.RSSI()
			if err == nil && rssi > nf.DBm+thresholdDB {
				logging.Trace(d.log, "lbt rssi stage: busy",
					zap.Float64("rssi_dbm", rssi), zap.Float64("floor_dbm", nf.DBm),
					zap.Float64("threshold_db", thresholdDB))
				return true, nil
			}
		}
	}
	busy, err := d.r.AssessChannel(ctx, sx126x.CAD{})
	if logging.On(d.log) {
		logging.Trace(d.log, "lbt cad verdict",
			zap.Bool("busy", busy), zap.NamedError("chip", err))
	}
	return busy, receptionPending(err)
}

// receptionPending re-labels the library's refusals of a destructive
// operation: a reception in progress, or a frame latched and unread,
// is the channel being busy — not a radio fault. Anything else passes
// through untouched.
func receptionPending(err error) error {
	if errors.Is(err, sx126x.ErrReceiveInProgress) || errors.Is(err, sx126x.ErrUnreadFrame) {
		return fmt.Errorf("%w: %w", radio.ErrBusyReceiving, err)
	}
	return err
}

func (d *device) StartReceive() error { return d.r.StartReceive() }

// sampleEvery paces the noise-floor sampling while a batch collects.
const sampleEvery = 20 * time.Millisecond

// Receive waits for the next frame, and measures while it waits: the
// radio has exactly one owning goroutine, so the noise floor is
// sampled here, between polls, never from outside.
//
// The wait is bi-modal, following the tracker's own state. While a
// batch collects (~1.3 s of every cycle), a 20 ms tick paces the
// sampling. While the tracker rests, the wait is purely edge-driven —
// no wake-ups at all — behind a DIO1 level check that catches any
// transition the edge path missed, plus the optional board watchdog.
func (d *device) Receive(ctx context.Context) (radio.Frame, error) {
	tick := time.NewTicker(sampleEvery)
	defer tick.Stop()
	ticking := true
	rechecked := false
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
			if logging.On(d.log) {
				logging.Trace(d.log, "rx frame off the chip",
					zap.Int("bytes", len(f.Payload)),
					zap.Float64("rssi_dbm", f.RSSI), zap.Float64("snr_db", f.SNR),
					zap.Float64("signal_rssi_dbm", f.SignalRSSI),
					zap.Float64("freq_err_hz", f.FreqErr),
					zap.Duration("airtime", f.Airtime))
			}
			return mapFrame(f), nil
		}
		now := time.Now()
		if d.floor.collecting(now) {
			rechecked = false
			err = d.collectPhase(ctx, edges, tick, &ticking)
		} else {
			if ticking {
				tick.Stop() // frames wake us by edge; no clock at rest
				ticking = false
			}
			err = d.restPhase(ctx, d.floor.restLeft(now), &rechecked)
		}
		if err != nil {
			return radio.Frame{}, err
		}
	}
}

func mapFrame(f *sx126x.RxFrame) radio.Frame {
	return radio.Frame{
		Payload:    f.Payload,
		RSSI:       f.RSSI,
		SNR:        f.SNR,
		SignalRSSI: f.SignalRSSI,
		FreqErrHz:  f.FreqErr,
		Airtime:    f.Airtime,
		At:         f.At,
	}
}

// collectPhase paces one sampling tick while a batch collects.
func (d *device) collectPhase(ctx context.Context, edges <-chan struct{},
	tick *time.Ticker, ticking *bool,
) error {
	if !*ticking {
		tick.Reset(sampleEvery)
		*ticking = true
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-edges:
		logging.Trace(d.log, "irq dio1 edge while collecting")
	case <-tick.C:
		d.sampleFloor()
		d.refreshStats()
	}
	return nil
}

// restPhase parks the loop between batches. The DIO1 level is read
// before sleeping — the level outlives any missed transition, so high
// means an IRQ latched since Poll and the caller must look again;
// rechecked guards a line stuck high from spinning on the bus. Then a
// pure edge sleep, bounded by the rest deadline and the optional
// board watchdog.
func (d *device) restPhase(ctx context.Context, until time.Duration, rechecked *bool) error {
	if !*rechecked {
		high, err := d.dio1.Get()
		if err != nil {
			return err
		}
		if high {
			logging.Trace(d.log, "irq dio1 level high at rest — polling again")
			*rechecked = true
			return nil // look again
		}
	}
	*rechecked = false
	timer := time.NewTimer(until)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-d.r.Events():
		logging.Trace(d.log, "irq dio1 edge at rest")
	case <-timer.C:
	case <-d.watchdog:
		logging.Trace(d.log, "board watchdog woke the receiver")
	}
	return nil
}

// Transmit keys the radio from the owning goroutine; the library
// hands the chip back to reception on every path out.
func (d *device) Transmit(ctx context.Context, payload []byte, powerDBm int8) (radio.TxReport, error) {
	// A result alongside an error means the frame reached the air and
	// the trouble came after — a hand-back that failed on the bus. The
	// caller needs both: the airtime was radiated and must be charged
	// whatever happens to the session next.
	logging.Trace(d.log, "tx keying",
		zap.Int("bytes", len(payload)), zap.Int8("power_dbm", powerDBm))
	res, err := d.r.Transmit(ctx, payload, powerDBm)
	if res == nil {
		return radio.TxReport{}, receptionPending(err)
	}
	if logging.On(d.log) {
		logging.Trace(d.log, "tx done — chip handed back to rx",
			zap.Duration("airtime", res.Airtime), zap.Duration("keyed", res.Duration),
			zap.NamedError("handback", err))
	}
	return radio.TxReport{
		At:       res.At,
		Airtime:  res.Airtime,
		Duration: res.Duration,
		PowerDBm: res.PowerDBm,
	}, receptionPending(err)
}

// statsEvery paces the chip-counter refresh: diagnostics, not a feed.
const statsEvery = time.Minute

// refreshStats caches the chip's own counters once a minute, from the
// owning goroutine — readers get the cache, never the bus.
func (d *device) refreshStats() {
	if time.Since(d.statsRead) < statsEvery {
		return
	}
	d.statsRead = time.Now()
	s, err := d.r.Stats()
	if err != nil {
		return // Poll will surface a bus that is truly sick
	}
	d.stats.Store(radio.ChipStats{
		Received:     s.Received,
		CRCErrors:    s.CRCErrors,
		HeaderErrors: s.HeaderErrors,
	})
}

// ChipStats reports the cached counters; any goroutine may ask.
func (d *device) ChipStats() (radio.ChipStats, bool) {
	s, ok := d.stats.Load().(radio.ChipStats)
	return s, ok
}

// sampleFloor takes one idle RSSI reading when it is safe to: not
// while a frame may be arriving — a tripped preamble detector already
// disqualifies the sample — and only in receive mode, which rules out
// a future transmit path by the chip's own account of itself.
func (d *device) sampleFloor() {
	if !d.floor.collecting(time.Now()) {
		return // resting: the bus stays untouched
	}
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

// NoiseStarved counts abandoned measurement batches; any goroutine.
func (d *device) NoiseStarved() uint64 { return d.floor.starvedCount() }

func (d *device) Close() error {
	if d.wdTicker != nil {
		d.wdTicker.Stop()
	}
	err := d.r.Close()
	for _, h := range d.held {
		_ = h.Close()
	}
	return err
}
