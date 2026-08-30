//go:build linux

package sx126x

// The adaptation itself, without a GPIO line or an SPI bus: which
// library errors become a busy channel rather than a radio fault,
// what a reception carries across the seam, and what an emission
// reports when the chip falls over after the frame is already out.
//
// The library's own transcripts prove the wire. Nothing there can see
// a mistake made on this side of it, which is where the audit found
// the lost power sentinel, the narrowed preamble and the report
// dropped beside its error.

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"meshrunner.dev/lotor/internal/logging"
	"meshrunner.dev/lotor/internal/radio"
	"meshrunner.dev/pkg/lora"
	"meshrunner.dev/pkg/lora/sx126x"
)

// fakeChip is a scripted stand-in for the driver library. The mutex
// covers the fields a test rewrites while Receive runs in its own
// goroutine, and the counters say which bus operations a phase took.
type fakeChip struct {
	mu          sync.Mutex
	configured  lora.Params
	configErr   error
	startErr    error
	pollFrame   *sx126x.RxFrame
	pollErr     error
	events      chan struct{}
	rssi        float64
	rssiErr     error
	rssiCalls   int
	inProgress  [2]bool
	ripErr      error
	cadBusy     bool
	cadErr      error
	txResult    *sx126x.TxResult
	txErr       error
	txPower     int8
	stats       sx126x.Stats
	statsErr    error
	statsCalls  int
	closeErr    error
	closedCount int
}

func (c *fakeChip) Configure(p lora.Params) error {
	c.configured = p
	return c.configErr
}
func (c *fakeChip) StartReceive() error     { return c.startErr }
func (c *fakeChip) Events() <-chan struct{} { return c.events }

func (c *fakeChip) Poll() (*sx126x.RxFrame, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.pollFrame, c.pollErr
}

func (c *fakeChip) RSSI() (float64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.rssiCalls++
	return c.rssi, c.rssiErr
}

func (c *fakeChip) Stats() (sx126x.Stats, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.statsCalls++
	return c.stats, c.statsErr
}

func (c *fakeChip) ReceiveInProgress() (bool, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.inProgress[0], c.inProgress[1], c.ripErr
}

func (c *fakeChip) rssiReads() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.rssiCalls
}

func (c *fakeChip) AssessChannel(context.Context, sx126x.CAD) (bool, error) {
	return c.cadBusy, c.cadErr
}

func (c *fakeChip) Transmit(_ context.Context, _ []byte, powerDBm int8) (*sx126x.TxResult, error) {
	c.txPower = powerDBm
	return c.txResult, c.txErr
}

func (c *fakeChip) Close() error {
	c.closedCount++
	return c.closeErr
}

func newDevice(c *fakeChip) *device {
	return &device{r: c, log: zap.NewNop(), dio1: nil}
}

func TestConfigureRefusesBeforeItTouchesTheChip(t *testing.T) {
	// The waveform conversion is the same one the preflight runs, and
	// a refusal here must not have reached the hardware at all.
	c := &fakeChip{}
	d := newDevice(c)
	bad := meshcoreWaveform()
	bad.SpreadingFactor = 13
	if err := d.Configure(bad); err == nil {
		t.Fatal("SF 13 was configured")
	}
	if c.configured.SF != 0 {
		t.Error("the chip was programmed with a waveform the conversion refused")
	}
	// A sound waveform reaches the chip, and its airtime arithmetic
	// then answers from what was actually programmed.
	if err := d.Configure(meshcoreWaveform()); err != nil {
		t.Fatal(err)
	}
	if c.configured.SF != lora.SF8 || c.configured.Preamble != 32 {
		t.Errorf("programmed %+v", c.configured)
	}
	if d.Airtime(16) == 0 {
		t.Error("airtime is not answering from the configured waveform")
	}
}

func TestBusyChannelIsNotARadioFault(t *testing.T) {
	// The distinction the supervisor turns on: a reception in
	// progress reschedules an emission, a fault tears the session
	// down and reopens the hardware.
	cases := []struct {
		name  string
		err   error
		busy  bool
		fault bool
	}{
		{"clear", nil, false, false},
		{"receive in progress", sx126x.ErrReceiveInProgress, true, false},
		{"frame latched unread", sx126x.ErrUnreadFrame, true, false},
		{"a real bus fault", errors.New("spi gave way"), false, true},
	}
	for _, tc := range cases {
		d := newDevice(&fakeChip{cadErr: tc.err})
		_, err := d.AssessChannel(context.Background(), 0)
		switch {
		case tc.busy && !errors.Is(err, radio.ErrBusyReceiving):
			t.Errorf("%s: %v, want a busy channel", tc.name, err)
		case tc.fault && (err == nil || errors.Is(err, radio.ErrBusyReceiving)):
			t.Errorf("%s: %v, want a fault", tc.name, err)
		case !tc.busy && !tc.fault && err != nil:
			t.Errorf("%s: %v", tc.name, err)
		}
	}
}

func TestCADTraceDistinguishesVerdictSkipAndFailure(t *testing.T) {
	cases := []struct {
		name    string
		busy    bool
		err     error
		message string
		field   string
		want    any
	}{
		{"clear verdict", false, nil, "lbt cad verdict", "busy", false},
		{"busy verdict", true, nil, "lbt cad verdict", "busy", true},
		{"reception in progress", false, sx126x.ErrReceiveInProgress,
			"lbt cad skipped", "reason", "reception-in-progress"},
		{"unread frame", false, sx126x.ErrUnreadFrame,
			"lbt cad skipped", "reason", "unread-frame"},
		{"radio fault", false, errors.New("spi gave way"),
			"lbt cad failed", "error", "spi gave way"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			core, observed := observer.New(logging.TraceLevel)
			d := newDevice(&fakeChip{cadBusy: tc.busy, cadErr: tc.err})
			d.log = zap.New(core)

			_, _ = d.AssessChannel(context.Background(), 0)

			entries := observed.FilterMessage(tc.message).All()
			if len(entries) != 1 || observed.Len() != 1 {
				t.Fatalf("trace entries = %+v, want one %q", observed.All(), tc.message)
			}
			fields := entries[0].ContextMap()
			if got := fields[tc.field]; got != tc.want {
				t.Errorf("%s = %v, want %v; fields = %+v", tc.field, got, tc.want, fields)
			}
			if _, exists := fields["chip"]; exists {
				t.Errorf("ambiguous chip field survived: %+v", fields)
			}
			if tc.message != "lbt cad verdict" {
				if _, exists := fields["busy"]; exists {
					t.Errorf("non-verdict trace carries busy: %+v", fields)
				}
			}
		})
	}
}

func TestCorruptReceptionIsToldApartFromAFault(t *testing.T) {
	// A CRC or header error is one frame lost, not a sick radio: the
	// engine counts it and keeps listening.
	for _, chipErr := range []error{sx126x.ErrCRC, sx126x.ErrHeader} {
		d := newDevice(&fakeChip{pollErr: chipErr, events: make(chan struct{})})
		_, err := d.Receive(context.Background())
		if !errors.Is(err, radio.ErrCorrupt) {
			t.Errorf("%v became %v, want a corrupt reception", chipErr, err)
		}
	}
	// Anything else is the radio's own trouble, passed through.
	fault := errors.New("bus gave way")
	d := newDevice(&fakeChip{pollErr: fault, events: make(chan struct{})})
	if _, err := d.Receive(context.Background()); !errors.Is(err, fault) {
		t.Errorf("a bus fault became %v", err)
	}
}

func TestAReceivedFrameCrossesTheSeamWhole(t *testing.T) {
	at := time.Now()
	c := &fakeChip{events: make(chan struct{}), pollFrame: &sx126x.RxFrame{
		Payload: []byte{1, 2, 3}, RSSI: -92.5, SNR: 7.25,
		SignalRSSI: -95, FreqErr: 1350, Airtime: 250 * time.Millisecond, At: at,
	}}
	f, err := newDevice(c).Receive(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(f.Payload) != 3 || f.RSSI != -92.5 || f.SNR != 7.25 ||
		f.SignalRSSI != -95 || f.FreqErrHz != 1350 || f.Airtime != 250*time.Millisecond {
		t.Errorf("frame = %+v", f)
	}
	if !f.At.Equal(at) {
		t.Errorf("At = %v, want the chip's own %v", f.At, at)
	}
}

func TestAnEmissionReportsItselfEvenWhenTheRadioThenFalls(t *testing.T) {
	// The regulatory half of the contract: TxDone happened, so the
	// airtime is owed to the ledger whatever the chip did next.
	at := time.Now()
	fault := errors.New("hand-back failed")
	c := &fakeChip{
		txResult: &sx126x.TxResult{At: at, Airtime: 300 * time.Millisecond,
			Duration: 310 * time.Millisecond, PowerDBm: -5},
		txErr: fault,
	}
	rep, err := newDevice(c).Transmit(context.Background(), []byte{1}, -5)
	if !errors.Is(err, fault) {
		t.Errorf("error = %v", err)
	}
	if rep.Airtime != 300*time.Millisecond || rep.PowerDBm != -5 || !rep.At.Equal(at) {
		t.Errorf("report = %+v — the emission must survive the fault", rep)
	}
	// Nothing radiated is an empty report, and the busy channel still
	// reads as a busy channel here too.
	c = &fakeChip{txErr: sx126x.ErrReceiveInProgress}
	rep, err = newDevice(c).Transmit(context.Background(), []byte{1}, -5)
	if !errors.Is(err, radio.ErrBusyReceiving) {
		t.Errorf("error = %v, want a busy channel", err)
	}
	if rep.Airtime != 0 {
		t.Errorf("report = %+v, want nothing charged", rep)
	}
}

func TestCloseReleasesEverythingAndSaysWhatFailed(t *testing.T) {
	chipErr := errors.New("chip close failed")
	pinErr := errors.New("line still held")
	pin := &fakePin{err: pinErr}
	c := &fakeChip{closeErr: chipErr}
	d := newDevice(c)
	d.held = []lora.OutputPin{pin, &fakePin{}}

	err := d.Close()
	if !errors.Is(err, chipErr) || !errors.Is(err, pinErr) {
		t.Errorf("Close reported %v — both failures belong in the diagnosis", err)
	}
	if c.closedCount != 1 || pin.closed != 1 {
		t.Errorf("chip closed %d times, pin %d", c.closedCount, pin.closed)
	}
}

// fakePin is a held enable line that may refuse to let go.
type fakePin struct {
	err    error
	closed int
}

func (p *fakePin) Set(bool) error { return nil }
func (p *fakePin) Close() error {
	p.closed++
	return p.err
}
