package radio

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"go.uber.org/zap"
)

// ConsumerRole is the authority class of one logical user of a physical
// radio. A relay is singular and authoritative; stations are never allowed to
// retune it while that relay exists.
type ConsumerRole string

const (
	// RoleRelay is the singular authoritative repeater consumer.
	RoleRelay ConsumerRole = "relay"
	// RoleStation is a non-forwarding companion identity.
	RoleStation ConsumerRole = "station"
)

// ControllerState is the physical attachment lifecycle.
type ControllerState string

const (
	// ControllerStarting is waiting for an attachment or opening hardware.
	ControllerStarting ControllerState = "starting"
	// ControllerRunning means the physical attachment is configured and receiving.
	ControllerRunning ControllerState = "running"
	// ControllerError means the physical attachment failed and will be retried.
	ControllerError ControllerState = "error"
	// ControllerStopped is the terminal state after context cancellation.
	ControllerStopped ControllerState = "stopped"
)

// BindingState is deliberately independent of ControllerState. A station may
// be blocked by the relay's waveform while the physical radio is healthy.
type BindingState string

const (
	// BindingDown has a compatible waveform but no usable physical session.
	BindingDown BindingState = "down"
	// BindingActive may receive and submit hardware operations.
	BindingActive BindingState = "active"
	// BindingBlocked conflicts with the authoritative consumer's waveform.
	BindingBlocked BindingState = "blocked"
)

var (
	// ErrControllerDown reports an operation attempted without live hardware.
	ErrControllerDown = errors.New("radio controller is down")
	// ErrBindingBlocked reports a station incompatible with radio authority.
	ErrBindingBlocked = errors.New("radio binding is blocked by the authoritative waveform")
	// ErrBindingClosed reports a logical attachment removed from its controller.
	ErrBindingClosed = errors.New("radio binding is detached")
	errReconfigure   = errors.New("radio controller configuration changed")
)

// Controller owns one physical attachment. Bindings are logical consumers;
// their Device sessions never touch the driver concurrently.
type Controller struct {
	name     string
	driver   Driver
	cfg      map[string]any
	envelope Envelope
	log      *zap.Logger

	mu             sync.Mutex
	state          ControllerState
	cause          string
	physical       Device
	configuredWave Waveform
	bindings       map[string]*Binding
	nextBinding    uint64
	configVersion  uint64
	ports          map[*controllerPort]struct{}
	relayQueue     []*radioOperation
	stationQueues  map[string][]*radioOperation
	rrOrder        []string
	rrLast         string
	receiveCancel  context.CancelFunc
	wake           chan struct{}
	changed        chan struct{}
	backoffFirst   time.Duration
	backoffCap     time.Duration
}

// NewController inspects the attachment without opening hardware. The same
// resolved driver configuration is used for every later supervised open.
func NewController(name string, driver Driver, cfg map[string]any, log *zap.Logger) (*Controller, error) {
	if driver.Open == nil || driver.Inspect == nil {
		return nil, errors.New("radio controller: driver needs Open and Inspect")
	}
	envelope, err := driver.Inspect(cfg)
	if err != nil {
		return nil, err
	}
	if log == nil {
		log = zap.NewNop()
	}
	return &Controller{
		name: name, driver: driver, cfg: cfg, envelope: envelope,
		log: log.With(zap.String("radio", name)), state: ControllerStarting,
		bindings: map[string]*Binding{}, ports: map[*controllerPort]struct{}{},
		stationQueues: map[string][]*radioOperation{}, wake: make(chan struct{}, 1),
		changed:      make(chan struct{}),
		backoffFirst: 5 * time.Second, backoffCap: time.Minute,
	}, nil
}

// Binding is one durable logical attachment. Open creates the short-lived
// Device session consumed by an engine; closing that session does not discard
// authority or configuration.
type Binding struct {
	controller *Controller
	name       string
	role       ConsumerRole
	waveform   Waveform
	sequence   uint64
}

func bindingKey(role ConsumerRole, name string) string { return string(role) + ":" + name }

// Bind registers a logical consumer. A second relay is refused. Incompatible
// stations remain registered but blocked, so their TCP service can keep
// running and report why RF is unavailable.
func (c *Controller) Bind(name string, role ConsumerRole, waveform Waveform) (*Binding, error) {
	if name == "" {
		return nil, errors.New("radio binding needs a name")
	}
	if role != RoleRelay && role != RoleStation {
		return nil, fmt.Errorf("unknown radio consumer role %q", role)
	}
	if err := c.envelope.Allows(waveform); err != nil {
		return nil, err
	}
	key := bindingKey(role, name)
	c.mu.Lock()
	defer c.mu.Unlock()
	before, hadBefore := c.authoritativeWaveformLocked()
	if _, exists := c.bindings[key]; exists {
		return nil, fmt.Errorf("radio binding %q already exists", key)
	}
	if role == RoleRelay {
		for _, binding := range c.bindings {
			if binding.role == RoleRelay {
				return nil, fmt.Errorf("radio %q already has relay %q", c.name, binding.name)
			}
		}
	}
	c.nextBinding++
	binding := &Binding{
		controller: c, name: name, role: role, waveform: waveform, sequence: c.nextBinding,
	}
	c.bindings[key] = binding
	c.rebuildRROrderLocked()
	after, hasAfter := c.authoritativeWaveformLocked()
	if hadBefore != hasAfter || before != after {
		c.configurationChangedLocked()
	}
	return binding, nil
}

// Unbind removes a logical consumer and ends its current Device session.
func (b *Binding) Unbind() {
	c := b.controller
	c.mu.Lock()
	before, hadBefore := c.authoritativeWaveformLocked()
	key := bindingKey(b.role, b.name)
	if c.bindings[key] != b {
		c.mu.Unlock()
		return
	}
	delete(c.bindings, key)
	for port := range c.ports {
		if port.binding == b {
			c.closePortLocked(port, ErrControllerDown)
		}
	}
	c.dropBindingOperationsLocked(b, ErrControllerDown)
	c.rebuildRROrderLocked()
	after, hasAfter := c.authoritativeWaveformLocked()
	if hadBefore != hasAfter || before != after {
		c.configurationChangedLocked()
	}
	c.mu.Unlock()
}

func (c *Controller) dropBindingOperationsLocked(binding *Binding, err error) {
	fail := func(queue []*radioOperation) []*radioOperation {
		kept := queue[:0]
		for _, operation := range queue {
			if operation.port.binding != binding {
				kept = append(kept, operation)
				continue
			}
			select {
			case operation.done <- operationResult{err: err}:
			default:
			}
		}
		return kept
	}
	c.relayQueue = fail(c.relayQueue)
	for queueKey, queue := range c.stationQueues {
		queue = fail(queue)
		if len(queue) == 0 {
			delete(c.stationQueues, queueKey)
		} else {
			c.stationQueues[queueKey] = queue
		}
	}
}

// SetWaveform changes one consumer's requested waveform. A station never
// retunes a relay-owned radio: when its request differs it simply becomes
// blocked. On a station-only radio, changing the authoritative station
// reconfigures the physical device and all other stations are judged against
// the new request.
func (b *Binding) SetWaveform(waveform Waveform) error {
	c := b.controller
	if err := c.envelope.Allows(waveform); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.bindings[bindingKey(b.role, b.name)] != b {
		return ErrControllerDown
	}
	before, hadBefore := c.authoritativeWaveformLocked()
	b.waveform = waveform
	after, hasAfter := c.authoritativeWaveformLocked()
	if hadBefore != hasAfter || before != after {
		c.configurationChangedLocked()
	}
	return nil
}

func (c *Controller) configurationChangedLocked() {
	c.configVersion++
	c.rebuildRROrderLocked()
	c.notifyChangedLocked()
	if c.receiveCancel != nil {
		c.receiveCancel()
	}
	select {
	case c.wake <- struct{}{}:
	default:
	}
}

func (c *Controller) notifyChangedLocked() {
	close(c.changed)
	c.changed = make(chan struct{})
}

// authoritativeLocked chooses the relay when present. Without one, the first
// station bound is stable authority until the controller is rebuilt.
func (c *Controller) authoritativeLocked() *Binding {
	var station *Binding
	for _, binding := range c.bindings {
		if binding.role == RoleRelay {
			return binding
		}
		if station == nil || binding.sequence < station.sequence {
			station = binding
		}
	}
	return station
}

func (c *Controller) authoritativeWaveformLocked() (Waveform, bool) {
	binding := c.authoritativeLocked()
	if binding == nil {
		return Waveform{}, false
	}
	return binding.waveform, true
}

func (b *Binding) stateLocked() BindingState {
	c := b.controller
	if c.bindings[bindingKey(b.role, b.name)] != b {
		return BindingDown
	}
	authority := c.authoritativeLocked()
	if authority == nil || authority.waveform != b.waveform {
		return BindingBlocked
	}
	if c.state != ControllerRunning || c.physical == nil || c.configuredWave != b.waveform {
		return BindingDown
	}
	return BindingActive
}

// State reports the logical RF state and a useful cause when blocked/down.
func (b *Binding) State() (BindingState, string) {
	c := b.controller
	c.mu.Lock()
	defer c.mu.Unlock()
	state := b.stateLocked()
	switch state {
	case BindingBlocked:
		authority := c.authoritativeLocked()
		if authority == nil {
			return state, "no authoritative waveform"
		}
		return state, fmt.Sprintf("%s %q is authoritative on a different waveform",
			authority.role, authority.name)
	case BindingDown:
		if c.bindings[bindingKey(b.role, b.name)] != b {
			return state, ErrBindingClosed.Error()
		}
		return state, c.cause
	default:
		return state, ""
	}
}

// ControllerStatus is a coherent physical lifecycle snapshot.
func (c *Controller) ControllerStatus() (ControllerState, string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.state, c.cause
}

// Authority reports which binding selects the physical waveform. A relay is
// always authoritative when present; otherwise the oldest station binding is.
func (c *Controller) Authority() (ConsumerRole, string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	binding := c.authoritativeLocked()
	if binding == nil {
		return "", ""
	}
	return binding.role, binding.name
}

// Envelope reports the immutable physical limits inspected at construction.
func (c *Controller) Envelope() Envelope { return c.envelope }

// Open creates one logical Device session when the physical attachment is
// already up. Call OpenContext when startup should wait for the controller.
func (b *Binding) Open() (Device, error) {
	c := b.controller
	c.mu.Lock()
	defer c.mu.Unlock()
	return b.openLocked()
}

// OpenContext waits through the controller's initial open/configure cycle.
// A concrete hardware failure and a blocked binding are returned immediately;
// cancellation ends the wait without introducing a retry delay in the relay.
func (b *Binding) OpenContext(ctx context.Context) (Device, error) {
	c := b.controller
	for {
		c.mu.Lock()
		device, err := b.openLocked()
		if err == nil || !errors.Is(err, ErrControllerDown) || c.cause != "" {
			if errors.Is(err, ErrControllerDown) && c.cause != "" {
				err = fmt.Errorf("%w: %s", err, c.cause)
			}
			c.mu.Unlock()
			return device, err
		}
		changed := c.changed
		c.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-changed:
		}
	}
}

func (b *Binding) openLocked() (Device, error) {
	c := b.controller
	if c.bindings[bindingKey(b.role, b.name)] != b {
		return nil, ErrBindingClosed
	}
	switch b.stateLocked() {
	case BindingBlocked:
		return nil, ErrBindingBlocked
	case BindingDown:
		return nil, ErrControllerDown
	case BindingActive:
	}
	for port := range c.ports {
		if port.binding == b && !port.closed {
			return nil, fmt.Errorf("radio binding %q already has an open session", b.name)
		}
	}
	port := &controllerPort{binding: b, inbox: make(chan receiveResult, 32)}
	c.ports[port] = struct{}{}
	return port, nil
}

// Run supervises the one physical open/configure/receive loop.
func (c *Controller) Run(ctx context.Context) {
	backoff := c.backoffFirst
	for ctx.Err() == nil {
		waveform, version, ok := c.configuration()
		if !ok {
			c.setPhysicalState(ControllerStarting, nil, nil, Waveform{})
			select {
			case <-ctx.Done():
				break
			case <-c.wake:
				continue
			}
			if ctx.Err() != nil {
				break
			}
		}
		dev, err := c.driver.Open(c.cfg, c.log)
		if err == nil {
			err = dev.Envelope().Allows(waveform)
		}
		if err == nil {
			err = dev.Configure(waveform)
		}
		if err == nil {
			err = dev.StartReceive()
		}
		if err == nil {
			c.setPhysicalState(ControllerRunning, nil, dev, waveform)
			c.log.Info("radio controller running",
				zap.Uint32("frequency_hz", waveform.FrequencyHz),
				zap.Int("sf", waveform.SpreadingFactor), zap.Int("bandwidth_hz", waveform.BandwidthHz))
			err = c.runPhysical(ctx, dev, version)
			backoff = c.backoffFirst
		}
		if dev != nil {
			_ = dev.Close()
		}
		c.endPhysical(err)
		if ctx.Err() != nil {
			break
		}
		if errors.Is(err, errReconfigure) {
			continue
		}
		c.log.Error("radio controller down, will retry", zap.Error(err), zap.Duration("backoff", backoff))
		select {
		case <-ctx.Done():
		case <-c.wake:
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, c.backoffCap)
	}
	c.endPhysical(context.Canceled)
	c.mu.Lock()
	c.state, c.cause = ControllerStopped, ""
	c.notifyChangedLocked()
	c.mu.Unlock()
}

func (c *Controller) configuration() (Waveform, uint64, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	authority := c.authoritativeLocked()
	if authority == nil {
		return Waveform{}, c.configVersion, false
	}
	return authority.waveform, c.configVersion, true
}

func (c *Controller) setPhysicalState(state ControllerState, err error, dev Device, waveform Waveform) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.state, c.cause, c.physical, c.configuredWave = state, "", dev, waveform
	if err != nil {
		c.cause = err.Error()
	}
	c.notifyChangedLocked()
}

func (c *Controller) endPhysical(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.receiveCancel != nil {
		c.receiveCancel()
		c.receiveCancel = nil
	}
	c.physical = nil
	c.configuredWave = Waveform{}
	if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, errReconfigure) {
		c.state, c.cause = ControllerError, err.Error()
	} else if c.state != ControllerStopped {
		c.state, c.cause = ControllerStarting, ""
	}
	for port := range c.ports {
		c.closePortLocked(port, err)
	}
	c.failPendingLocked(err)
	c.notifyChangedLocked()
}

type receiveResult struct {
	frame Frame
	err   error
}

func (c *Controller) runPhysical(ctx context.Context, dev Device, version uint64) error {
	for {
		if c.configurationVersion() != version {
			return errReconfigure
		}
		if operation := c.nextOperation(); operation != nil {
			c.execute(dev, operation)
			continue
		}
		rctx, cancel := context.WithCancel(ctx)
		c.mu.Lock()
		if c.configVersion != version || c.hasPendingLocked() {
			c.mu.Unlock()
			cancel()
			continue
		}
		c.receiveCancel = cancel
		c.mu.Unlock()
		frame, err := dev.Receive(rctx)
		cancel()
		c.mu.Lock()
		c.receiveCancel = nil
		interrupted := c.configVersion != version || c.hasPendingLocked()
		c.mu.Unlock()
		if err == nil {
			c.broadcast(ctx, receiveResult{frame: frame})
			continue
		}
		if errors.Is(err, context.Canceled) && ctx.Err() == nil && interrupted {
			continue
		}
		if errors.Is(err, ErrCorrupt) {
			c.broadcast(ctx, receiveResult{frame: frame, err: err})
			continue
		}
		return err
	}
}

func (c *Controller) configurationVersion() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.configVersion
}

func (c *Controller) broadcast(ctx context.Context, result receiveResult) {
	c.mu.Lock()
	ports := make([]*controllerPort, 0, len(c.ports))
	for port := range c.ports {
		if !port.closed && port.binding.stateLocked() == BindingActive {
			ports = append(ports, port)
		}
	}
	c.mu.Unlock()
	sort.SliceStable(ports, func(i, j int) bool {
		return ports[i].binding.role == RoleRelay && ports[j].binding.role != RoleRelay
	})
	for _, port := range ports {
		if port.binding.role == RoleRelay {
			select {
			case port.inbox <- result:
			case <-ctx.Done():
				return
			}
			continue
		}
		select {
		case port.inbox <- result:
		default:
			c.log.Warn("station receive queue full, dropping frame",
				zap.String("station", port.binding.name))
		}
	}
}

type operationResult struct {
	busy   bool
	report TxReport
	err    error
}

type radioOperation struct {
	port *controllerPort
	run  func(Device) operationResult
	done chan operationResult
}

func (c *Controller) enqueue(operation *radioOperation) error {
	c.mu.Lock()
	if operation.port.closed || operation.port.binding.stateLocked() != BindingActive || c.physical == nil {
		c.mu.Unlock()
		return ErrControllerDown
	}
	if operation.port.binding.role == RoleRelay {
		c.relayQueue = append(c.relayQueue, operation)
	} else {
		key := bindingKey(operation.port.binding.role, operation.port.binding.name)
		c.stationQueues[key] = append(c.stationQueues[key], operation)
		c.rebuildRROrderLocked()
	}
	if c.receiveCancel != nil {
		c.receiveCancel()
	}
	select {
	case c.wake <- struct{}{}:
	default:
	}
	c.mu.Unlock()
	return nil
}

func (c *Controller) hasPendingLocked() bool {
	if len(c.relayQueue) > 0 {
		return true
	}
	for _, queue := range c.stationQueues {
		if len(queue) > 0 {
			return true
		}
	}
	return false
}

func (c *Controller) nextOperation() *radioOperation {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.relayQueue) > 0 {
		op := c.relayQueue[0]
		c.relayQueue = c.relayQueue[1:]
		return op
	}
	if len(c.rrOrder) == 0 {
		return nil
	}
	start := 0
	if c.rrLast != "" {
		if at := sort.SearchStrings(c.rrOrder, c.rrLast); at < len(c.rrOrder) && c.rrOrder[at] == c.rrLast {
			start = (at + 1) % len(c.rrOrder)
		}
	}
	for offset := range c.rrOrder {
		key := c.rrOrder[(start+offset)%len(c.rrOrder)]
		queue := c.stationQueues[key]
		if len(queue) == 0 {
			continue
		}
		op := queue[0]
		c.stationQueues[key] = queue[1:]
		c.rrLast = key
		return op
	}
	return nil
}

func (c *Controller) rebuildRROrderLocked() {
	order := make([]string, 0, len(c.stationQueues))
	for key := range c.stationQueues {
		if binding := c.bindings[key]; binding != nil && binding.role == RoleStation {
			order = append(order, key)
		}
	}
	sort.Strings(order)
	c.rrOrder = order
	if len(order) == 0 {
		c.rrLast = ""
	}
}

func (c *Controller) execute(dev Device, operation *radioOperation) {
	operation.done <- operation.run(dev)
}

func (c *Controller) failPendingLocked(err error) {
	if err == nil {
		err = ErrControllerDown
	}
	queues := make([][]*radioOperation, 1, 1+len(c.stationQueues))
	queues[0] = c.relayQueue
	for _, queue := range c.stationQueues {
		queues = append(queues, queue)
	}
	c.relayQueue = nil
	c.stationQueues = map[string][]*radioOperation{}
	c.rebuildRROrderLocked()
	for _, queue := range queues {
		for _, operation := range queue {
			select {
			case operation.done <- operationResult{err: err}:
			default:
			}
		}
	}
}

type controllerPort struct {
	binding *Binding
	inbox   chan receiveResult
	closed  bool
}

func (p *controllerPort) Envelope() Envelope { return p.binding.controller.envelope }

func (p *controllerPort) Configure(waveform Waveform) error {
	if waveform != p.binding.waveform {
		return errors.New("logical radio cannot be retuned outside its binding")
	}
	state, cause := p.binding.State()
	if state == BindingBlocked {
		return fmt.Errorf("%w: %s", ErrBindingBlocked, cause)
	}
	return nil
}

func (*controllerPort) StartReceive() error { return nil }

func (p *controllerPort) Receive(ctx context.Context) (Frame, error) {
	select {
	case <-ctx.Done():
		return Frame{}, ctx.Err()
	case result, ok := <-p.inbox:
		if !ok {
			return Frame{}, ErrControllerDown
		}
		return result.frame, result.err
	}
}

func (p *controllerPort) operation(ctx context.Context, operation *radioOperation) (operationResult, error) {
	operation.port, operation.done = p, make(chan operationResult, 1)
	if err := p.binding.controller.enqueue(operation); err != nil {
		return operationResult{}, err
	}
	select {
	case <-ctx.Done():
		return operationResult{}, ctx.Err()
	case result := <-operation.done:
		return result, result.err
	}
}

func (p *controllerPort) Transmit(ctx context.Context, payload []byte, powerDBm int8) (TxReport, error) {
	raw := append([]byte(nil), payload...)
	result, err := p.operation(ctx, &radioOperation{run: func(dev Device) operationResult {
		report, err := dev.Transmit(ctx, raw, powerDBm)
		return operationResult{report: report, err: err}
	}})
	return result.report, err
}

func (p *controllerPort) AssessChannel(ctx context.Context, thresholdDB float64) (bool, error) {
	result, err := p.operation(ctx, &radioOperation{run: func(dev Device) operationResult {
		busy, err := dev.AssessChannel(ctx, thresholdDB)
		return operationResult{busy: busy, err: err}
	}})
	return result.busy, err
}

func (p *controllerPort) physical() Device {
	c := p.binding.controller
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.physical
}

func (p *controllerPort) NoiseFloor() (NoiseFloor, bool) {
	if dev := p.physical(); dev != nil {
		return dev.NoiseFloor()
	}
	return NoiseFloor{}, false
}

func (p *controllerPort) NoiseStarved() uint64 {
	if dev := p.physical(); dev != nil {
		return dev.NoiseStarved()
	}
	return 0
}

func (p *controllerPort) ChipStats() (ChipStats, bool) {
	if dev := p.physical(); dev != nil {
		return dev.ChipStats()
	}
	return ChipStats{}, false
}

func (p *controllerPort) Airtime(bytes int) time.Duration {
	if dev := p.physical(); dev != nil {
		return dev.Airtime(bytes)
	}
	return 0
}

func (p *controllerPort) Close() error {
	c := p.binding.controller
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closePortLocked(p, nil)
	return nil
}

func (c *Controller) closePortLocked(port *controllerPort, err error) {
	if port.closed {
		return
	}
	port.closed = true
	delete(c.ports, port)
	if err != nil {
		select {
		case port.inbox <- receiveResult{err: err}:
		default:
		}
	}
	close(port.inbox)
}
