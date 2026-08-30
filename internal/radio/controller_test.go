package radio

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"

	"meshrunner.dev/lotor/internal/correlation"
)

type controllerFakeDevice struct {
	waveform Waveform
	rx       chan receiveResult

	mu         sync.Mutex
	order      []string
	hold       chan struct{}
	holdStart  chan struct{}
	configured int
	closed     int
}

func newControllerFakeDevice() *controllerFakeDevice {
	return &controllerFakeDevice{rx: make(chan receiveResult, 8)}
}

func (*controllerFakeDevice) Envelope() Envelope {
	return Envelope{FreqRangeLowHz: 400_000_000, FreqRangeHiHz: 930_000_000}
}
func (d *controllerFakeDevice) Configure(w Waveform) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.waveform = w
	d.configured++
	return nil
}
func (*controllerFakeDevice) StartReceive() error { return nil }
func (d *controllerFakeDevice) Receive(ctx context.Context) (Frame, error) {
	select {
	case result := <-d.rx:
		return result.frame, result.err
	case <-ctx.Done():
		return Frame{}, ctx.Err()
	}
}
func (*controllerFakeDevice) NoiseFloor() (NoiseFloor, bool) { return NoiseFloor{DBm: -100}, true }
func (*controllerFakeDevice) NoiseStarved() uint64           { return 0 }
func (*controllerFakeDevice) ChipStats() (ChipStats, bool)   { return ChipStats{Received: 1}, true }
func (*controllerFakeDevice) Airtime(bytes int) time.Duration {
	return time.Duration(bytes) * time.Millisecond
}
func (d *controllerFakeDevice) Transmit(_ context.Context, payload []byte, power int8) (TxReport, error) {
	word := string(payload)
	d.mu.Lock()
	d.order = append(d.order, word)
	hold, started := d.hold, d.holdStart
	d.mu.Unlock()
	if word == "hold" && hold != nil {
		close(started)
		<-hold
	}
	return TxReport{At: time.Now(), Airtime: d.Airtime(len(payload)), PowerDBm: power}, nil
}
func (*controllerFakeDevice) AssessChannel(context.Context, float64) (bool, error) {
	return false, nil
}
func (d *controllerFakeDevice) Close() error {
	d.mu.Lock()
	d.closed++
	d.mu.Unlock()
	return nil
}

func controllerRig(t *testing.T, dev *controllerFakeDevice) (*Controller, context.CancelFunc) {
	t.Helper()
	driver := Driver{
		Inspect: func(map[string]any) (Envelope, error) { return dev.Envelope(), nil },
		Open:    func(map[string]any, *zap.Logger) (Device, error) { return dev, nil },
	}
	c, err := NewController("slot1", driver, nil, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	c.backoffFirst, c.backoffCap = time.Millisecond, time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	go c.Run(ctx)
	t.Cleanup(cancel)
	return c, cancel
}

func awaitController(t *testing.T, c *Controller) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if state, _ := c.ControllerStatus(); state == ControllerRunning {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("controller state = %s", func() ControllerState { state, _ := c.ControllerStatus(); return state }())
}

func awaitBinding(t *testing.T, binding *Binding) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if state, _ := binding.State(); state == BindingActive {
			return
		}
		time.Sleep(time.Millisecond)
	}
	state, cause := binding.State()
	t.Fatalf("binding state = %s: %s", state, cause)
}

func TestBindingExposesControllerEnvelopeBeforeHardwareOpens(t *testing.T) {
	dev := newControllerFakeDevice()
	driver := Driver{
		Inspect: func(map[string]any) (Envelope, error) {
			return Envelope{
				MaxTxPowerSet: true, MaxTxPowerDBm: 10,
				ChipMinDBm: -9, ChipMaxDBm: 22,
				FreqRangeLowHz: 863_000_000, FreqRangeHiHz: 870_000_000,
			}, nil
		},
		Open: func(map[string]any, *zap.Logger) (Device, error) { return dev, nil },
	}
	controller, err := NewController("slot1", driver, nil, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	binding, err := controller.Bind("alice", RoleStation, Waveform{FrequencyHz: 869_618_000})
	if err != nil {
		t.Fatal(err)
	}

	want := controller.Envelope()
	if got := binding.Envelope(); got != want {
		t.Fatalf("binding envelope = %+v, want %+v", got, want)
	}
}

func TestControllerRelayOwnsWaveformAndRXIsShared(t *testing.T) {
	dev := newControllerFakeDevice()
	c, _ := controllerRig(t, dev)
	eu := Waveform{FrequencyHz: 869_618_000, SpreadingFactor: 8, BandwidthHz: 62_500, CodingRate: 8}
	us := Waveform{FrequencyHz: 915_000_000, SpreadingFactor: 9, BandwidthHz: 125_000, CodingRate: 5}
	wrong, err := c.Bind("wrong", RoleStation, eu)
	if err != nil {
		t.Fatal(err)
	}
	station, err := c.Bind("alice", RoleStation, us)
	if err != nil {
		t.Fatal(err)
	}
	relay, err := c.Bind("mc", RoleRelay, us)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Bind("mc2", RoleRelay, us); err == nil {
		t.Fatal("a second relay bound to one radio")
	}
	awaitController(t, c)
	awaitBinding(t, relay)
	if state, _ := wrong.State(); state != BindingBlocked {
		t.Fatalf("incompatible station state = %s", state)
	}
	if _, err := wrong.Open(); !errors.Is(err, ErrBindingBlocked) {
		t.Fatalf("incompatible station opened: %v", err)
	}
	relayDev, err := relay.Open()
	if err != nil {
		t.Fatal(err)
	}
	stationDev, err := station.Open()
	if err != nil {
		t.Fatal(err)
	}
	id := correlation.New()
	dev.rx <- receiveResult{frame: Frame{Correlation: id, Payload: []byte{1, 2, 3}}}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	relayFrame, err := relayDev.Receive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	stationFrame, err := stationDev.Receive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if relayFrame.Correlation != id || stationFrame.Correlation != id ||
		!reflect.DeepEqual(relayFrame.Payload, stationFrame.Payload) {
		t.Fatalf("broadcast = relay %+v station %+v", relayFrame, stationFrame)
	}
}

func TestControllerQueuesRelayFirstAndStationsRoundRobin(t *testing.T) {
	dev := newControllerFakeDevice()
	dev.hold, dev.holdStart = make(chan struct{}), make(chan struct{})
	c, _ := controllerRig(t, dev)
	w := Waveform{FrequencyHz: 869_618_000, SpreadingFactor: 8, BandwidthHz: 62_500, CodingRate: 8}
	relayBinding, _ := c.Bind("mc", RoleRelay, w)
	aBinding, _ := c.Bind("alice", RoleStation, w)
	bBinding, _ := c.Bind("bob", RoleStation, w)
	awaitController(t, c)
	awaitBinding(t, relayBinding)
	relay, _ := relayBinding.Open()
	a, _ := aBinding.Open()
	b, _ := bBinding.Open()

	type result struct{ err error }
	done := make(chan result, 4)
	go func() { _, err := a.Transmit(context.Background(), []byte("hold"), 1); done <- result{err} }()
	select {
	case <-dev.holdStart:
	case <-time.After(time.Second):
		t.Fatal("first station transmit did not start")
	}
	go func() { _, err := a.Transmit(context.Background(), []byte("a2"), 1); done <- result{err} }()
	go func() { _, err := b.Transmit(context.Background(), []byte("b1"), 1); done <- result{err} }()
	go func() { _, err := relay.Transmit(context.Background(), []byte("r1"), 1); done <- result{err} }()
	time.Sleep(10 * time.Millisecond)
	close(dev.hold)
	for range 4 {
		if got := <-done; got.err != nil {
			t.Fatal(got.err)
		}
	}
	dev.mu.Lock()
	order := append([]string(nil), dev.order...)
	dev.mu.Unlock()
	if want := []string{"hold", "r1", "b1", "a2"}; !reflect.DeepEqual(order, want) {
		t.Fatalf("physical order = %v, want %v", order, want)
	}
}

func TestCompatibleStationDoesNotBounceRelayHardware(t *testing.T) {
	dev := newControllerFakeDevice()
	c, _ := controllerRig(t, dev)
	relayWave := Waveform{
		FrequencyHz: 869_618_000, SpreadingFactor: 8, BandwidthHz: 62_500, CodingRate: 8,
	}
	otherWave := relayWave
	otherWave.FrequencyHz = 868_000_000
	relayBinding, err := c.Bind("mc", RoleRelay, relayWave)
	if err != nil {
		t.Fatal(err)
	}
	awaitController(t, c)
	awaitBinding(t, relayBinding)

	dev.mu.Lock()
	configured, closed := dev.configured, dev.closed
	dev.mu.Unlock()
	stationBinding, err := c.Bind("alice", RoleStation, relayWave)
	if err != nil {
		t.Fatal(err)
	}
	awaitBinding(t, stationBinding)
	time.Sleep(10 * time.Millisecond)
	dev.mu.Lock()
	if dev.configured != configured || dev.closed != closed {
		t.Fatalf("compatible bind bounced hardware: configure %d -> %d, close %d -> %d",
			configured, dev.configured, closed, dev.closed)
	}
	dev.mu.Unlock()

	if err := stationBinding.SetWaveform(otherWave); err != nil {
		t.Fatal(err)
	}
	if state, _ := stationBinding.State(); state != BindingBlocked {
		t.Fatalf("station changing away from relay = %s", state)
	}
	time.Sleep(10 * time.Millisecond)
	dev.mu.Lock()
	if dev.configured != configured || dev.closed != closed {
		t.Fatalf("blocked station retuned hardware: configure %d, close %d",
			dev.configured, dev.closed)
	}
	dev.mu.Unlock()

	if err := stationBinding.SetWaveform(relayWave); err != nil {
		t.Fatal(err)
	}
	awaitBinding(t, stationBinding)
	stationBinding.Unbind()
	time.Sleep(10 * time.Millisecond)
	dev.mu.Lock()
	defer dev.mu.Unlock()
	if dev.configured != configured || dev.closed != closed {
		t.Fatalf("compatible unbind bounced hardware: configure %d, close %d",
			dev.configured, dev.closed)
	}
}

func TestControllerCloseConcurrentWithBroadcast(t *testing.T) {
	dev := newControllerFakeDevice()
	c, _ := controllerRig(t, dev)
	w := Waveform{FrequencyHz: 869_618_000, SpreadingFactor: 8, BandwidthHz: 62_500, CodingRate: 8}
	binding, err := c.Bind("alice", RoleStation, w)
	if err != nil {
		t.Fatal(err)
	}
	awaitController(t, c)
	awaitBinding(t, binding)

	for range 1_000 {
		logical, err := binding.Open()
		if err != nil {
			t.Fatal(err)
		}
		start := make(chan struct{})
		finished := make(chan struct{}, 2)
		go func() {
			<-start
			c.broadcast(context.Background(), receiveResult{frame: Frame{Payload: []byte{1}}})
			finished <- struct{}{}
		}()
		go func() {
			<-start
			_ = logical.Close()
			finished <- struct{}{}
		}()
		close(start)
		<-finished
		<-finished
	}
}

func TestRelayReceiveBacklogDoesNotBlockHardwareOperations(t *testing.T) {
	dev := newControllerFakeDevice()
	c, _ := controllerRig(t, dev)
	w := Waveform{FrequencyHz: 869_618_000, SpreadingFactor: 8, BandwidthHz: 62_500, CodingRate: 8}
	relayBinding, err := c.Bind("mc", RoleRelay, w)
	if err != nil {
		t.Fatal(err)
	}
	stationBinding, err := c.Bind("alice", RoleStation, w)
	if err != nil {
		t.Fatal(err)
	}
	awaitController(t, c)
	awaitBinding(t, relayBinding)
	relay, err := relayBinding.Open()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = relay.Close() })
	station, err := stationBinding.Open()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = station.Close() })

	// More than the old relay channel capacity, without reading the relay.
	for i := range 128 {
		c.broadcast(context.Background(), receiveResult{frame: Frame{Payload: []byte{byte(i)}}})
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if busy, err := station.AssessChannel(ctx, 0); err != nil || busy {
		t.Fatalf("station channel assessment behind relay backlog = busy %t, err %v", busy, err)
	}
}

func TestCanceledQueuedOperationNeverReachesHardware(t *testing.T) {
	dev := newControllerFakeDevice()
	dev.hold, dev.holdStart = make(chan struct{}), make(chan struct{})
	c, _ := controllerRig(t, dev)
	w := Waveform{FrequencyHz: 869_618_000, SpreadingFactor: 8, BandwidthHz: 62_500, CodingRate: 8}
	binding, err := c.Bind("alice", RoleStation, w)
	if err != nil {
		t.Fatal(err)
	}
	awaitController(t, c)
	logical, err := binding.Open()
	if err != nil {
		t.Fatal(err)
	}

	holdDone := make(chan error, 1)
	go func() {
		_, txErr := logical.Transmit(context.Background(), []byte("hold"), 1)
		holdDone <- txErr
	}()
	select {
	case <-dev.holdStart:
	case <-time.After(time.Second):
		t.Fatal("holding operation did not start")
	}

	ctx, cancel := context.WithCancel(context.Background())
	queuedDone := make(chan error, 1)
	go func() {
		_, txErr := logical.Transmit(ctx, []byte("canceled"), 1)
		queuedDone <- txErr
	}()
	awaitPendingOperations(t, c, 1)
	cancel()
	if err := <-queuedDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled operation returned %v", err)
	}
	close(dev.hold)
	if err := <-holdDone; err != nil {
		t.Fatal(err)
	}

	dev.mu.Lock()
	defer dev.mu.Unlock()
	if want := []string{"hold"}; !reflect.DeepEqual(dev.order, want) {
		t.Fatalf("physical order = %v, want %v", dev.order, want)
	}
}

func TestCloseRejectsQueuedOperationButLetsStartedTransmitFinish(t *testing.T) {
	dev := newControllerFakeDevice()
	dev.hold, dev.holdStart = make(chan struct{}), make(chan struct{})
	c, _ := controllerRig(t, dev)
	w := Waveform{FrequencyHz: 869_618_000, SpreadingFactor: 8, BandwidthHz: 62_500, CodingRate: 8}
	binding, err := c.Bind("alice", RoleStation, w)
	if err != nil {
		t.Fatal(err)
	}
	awaitController(t, c)
	logical, err := binding.Open()
	if err != nil {
		t.Fatal(err)
	}

	holdDone := make(chan error, 1)
	go func() {
		_, txErr := logical.Transmit(context.Background(), []byte("hold"), 1)
		holdDone <- txErr
	}()
	<-dev.holdStart
	queuedDone := make(chan error, 1)
	go func() {
		_, txErr := logical.Transmit(context.Background(), []byte("closed"), 1)
		queuedDone <- txErr
	}()
	awaitPendingOperations(t, c, 1)
	if err := logical.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-queuedDone; !errors.Is(err, ErrBindingClosed) {
		t.Fatalf("queued operation after close returned %v", err)
	}
	close(dev.hold)
	if err := <-holdDone; err != nil {
		t.Fatalf("started operation was truncated: %v", err)
	}

	dev.mu.Lock()
	defer dev.mu.Unlock()
	if want := []string{"hold"}; !reflect.DeepEqual(dev.order, want) {
		t.Fatalf("physical order = %v, want %v", dev.order, want)
	}
}

func TestBlockedWaveformEndsExistingLogicalSession(t *testing.T) {
	dev := newControllerFakeDevice()
	c, _ := controllerRig(t, dev)
	w := Waveform{FrequencyHz: 869_618_000, SpreadingFactor: 8, BandwidthHz: 62_500, CodingRate: 8}
	other := w
	other.FrequencyHz = 868_000_000
	relay, err := c.Bind("mc", RoleRelay, w)
	if err != nil {
		t.Fatal(err)
	}
	station, err := c.Bind("alice", RoleStation, w)
	if err != nil {
		t.Fatal(err)
	}
	awaitController(t, c)
	awaitBinding(t, relay)
	logical, err := station.Open()
	if err != nil {
		t.Fatal(err)
	}
	if err := station.SetWaveform(other); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := logical.Receive(ctx); !errors.Is(err, ErrBindingBlocked) {
		t.Fatalf("blocked session returned %v", err)
	}
	if _, err := station.Open(); !errors.Is(err, ErrBindingBlocked) {
		t.Fatalf("blocked binding opened: %v", err)
	}
	if err := station.SetWaveform(w); err != nil {
		t.Fatal(err)
	}
	awaitBinding(t, station)
	if reopened, err := station.Open(); err != nil {
		t.Fatalf("compatible station did not reopen: %v", err)
	} else {
		_ = reopened.Close()
	}
}

func TestControllerValidatesLiveWaveformAgainstDriver(t *testing.T) {
	dev := newControllerFakeDevice()
	driver := Driver{
		Inspect: func(map[string]any) (Envelope, error) { return dev.Envelope(), nil },
		Open:    func(map[string]any, *zap.Logger) (Device, error) { return dev, nil },
		CheckWaveform: func(w Waveform) error {
			if w.BandwidthHz != 62_500 {
				return errors.New("unsupported bandwidth")
			}
			return nil
		},
	}
	c, err := NewController("slot1", driver, nil, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	valid := Waveform{FrequencyHz: 869_618_000, SpreadingFactor: 8, BandwidthHz: 62_500, CodingRate: 8}
	invalid := valid
	invalid.BandwidthHz = 100_000
	if _, err := c.Bind("bad", RoleStation, invalid); err == nil {
		t.Fatal("driver-invalid initial waveform was accepted")
	}
	binding, err := c.Bind("alice", RoleStation, valid)
	if err != nil {
		t.Fatal(err)
	}
	if err := binding.SetWaveform(invalid); err == nil {
		t.Fatal("driver-invalid live waveform was accepted")
	}
}

func TestClosedLogicalPortKeepsItsAirtimeButNotNewTelemetry(t *testing.T) {
	dev := newControllerFakeDevice()
	c, _ := controllerRig(t, dev)
	w := Waveform{FrequencyHz: 869_618_000, SpreadingFactor: 8, BandwidthHz: 62_500, CodingRate: 8}
	binding, err := c.Bind("alice", RoleStation, w)
	if err != nil {
		t.Fatal(err)
	}
	awaitController(t, c)
	logical, err := binding.Open()
	if err != nil {
		t.Fatal(err)
	}
	if err := logical.Close(); err != nil {
		t.Fatal(err)
	}
	if got := logical.Airtime(3); got != 3*time.Millisecond {
		t.Fatalf("closed logical airtime = %s", got)
	}
	if _, ok := logical.NoiseFloor(); ok {
		t.Fatal("closed logical port observed another physical generation")
	}
}

func awaitPendingOperations(t *testing.T, c *Controller, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		c.mu.Lock()
		pending := len(c.relayQueue)
		for _, queue := range c.stationQueues {
			pending += len(queue)
		}
		c.mu.Unlock()
		if pending >= want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("pending operations did not reach %d", want)
}
