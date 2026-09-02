package radio

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"go.uber.org/zap"

	"meshrunner.dev/lotor/internal/correlation"
)

// ConsumerRole is the authority class of one logical user of a physical
// radio. A relay is singular and authoritative; stations and applications
// are one class beneath it — never allowed to retune the radio while a
// relay exists, served round-robin, their inboxes bounded — and differ
// only in what they are for.
type ConsumerRole string

const (
	// RoleRelay is the singular authoritative repeater consumer.
	RoleRelay ConsumerRole = "relay"
	// RoleStation is a non-forwarding companion identity.
	RoleStation ConsumerRole = "station"
	// RoleApplication is a non-forwarding identity that serves peers
	// over the air — a room server.
	RoleApplication ConsumerRole = "application"
)

// consumer reports whether a role belongs to the non-authoritative class.
func (r ConsumerRole) consumer() bool { return r == RoleStation || r == RoleApplication }

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
	if role != RoleRelay && !role.consumer() {
		return nil, fmt.Errorf("unknown radio consumer role %q", role)
	}
	if err := c.validateWaveform(waveform); err != nil {
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
			c.closePortLocked(port, ErrBindingClosed)
		}
	}
	c.dropBindingOperationsLocked(b, ErrBindingClosed)
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
	if err := c.validateWaveform(waveform); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.bindings[bindingKey(b.role, b.name)] != b {
		return ErrBindingClosed
	}
	before, hadBefore := c.authoritativeWaveformLocked()
	beforeState := b.stateLocked()
	b.waveform = waveform
	after, hasAfter := c.authoritativeWaveformLocked()
	if hadBefore != hasAfter || before != after {
		c.configurationChangedLocked()
	} else if afterState := b.stateLocked(); beforeState != afterState {
		// A non-authoritative station can block or unblock itself without
		// changing the physical waveform. End its existing logical session so
		// the owner observes the new binding state instead of waiting forever
		// on an inbox that will no longer receive frames.
		for port := range c.ports {
			if port.binding == b {
				err := ErrControllerDown
				if afterState == BindingBlocked {
					err = ErrBindingBlocked
				}
				c.closePortLocked(port, err)
			}
		}
		c.notifyChangedLocked()
	}
	return nil
}

func (c *Controller) validateWaveform(waveform Waveform) error {
	if err := c.envelope.Allows(waveform); err != nil {
		return err
	}
	if c.driver.CheckWaveform != nil {
		return c.driver.CheckWaveform(waveform)
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

// Envelope reports the immutable physical bounds of the radio this logical
// consumer is attached to, even while no short-lived Device session is open.
func (b *Binding) Envelope() Envelope { return b.controller.Envelope() }

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
	port := &controllerPort{
		binding: b,
		wake:    make(chan struct{}, 1),
		done:    make(chan struct{}),
		airtime: c.physical.Airtime,
	}
	c.ports[port] = struct{}{}
	return port, nil
}

// Run supervises the one physical open/configure/receive loop.
func (c *Controller) Run(ctx context.Context) {
	backoff := c.backoffFirst
	for ctx.Err() == nil {
		waveform, version, ok := c.configuration()
		if !ok {
			c.waitForConfiguration(ctx)
			if ctx.Err() != nil {
				break
			}
			continue
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

func (c *Controller) waitForConfiguration(ctx context.Context) {
	c.setPhysicalState(ControllerStarting, nil, nil, Waveform{})
	select {
	case <-ctx.Done():
	case <-c.wake:
	}
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

// handOver delivers one binding's emission to its peers on this
// controller: a half-duplex chip cannot hear itself, and two bindings
// on one radio are two nodes at no distance, which hear each other
// everywhere else in the mesh. The frame names its emitter and carries
// no measurements — nothing demodulated it, and a fabricated signal
// would travel on into relay scores and neighbour tables.
func (c *Controller) handOver(ctx context.Context, from *controllerPort, payload []byte) {
	b := from.binding
	causedBy, _ := correlation.FromContext(ctx)
	c.broadcastExcept(ctx, from, receiveResult{frame: Frame{
		// Its own correlation, as any reception gets at the device
		// seam: the peers judging it are not the goroutine that sent
		// it, and the journal must be able to follow their side.
		Correlation: correlation.New(),
		Payload:     append([]byte(nil), payload...),
		Binding:     bindingKey(b.role, b.name),
		CausedBy:    causedBy,
		At:          time.Now(),
	}})
}

func (c *Controller) broadcast(ctx context.Context, result receiveResult) {
	c.broadcastExcept(ctx, nil, result)
}

func (c *Controller) broadcastExcept(ctx context.Context, skip *controllerPort, result receiveResult) {
	c.mu.Lock()
	ports := make([]*controllerPort, 0, len(c.ports))
	for port := range c.ports {
		if port == skip {
			continue
		}
		if !port.closed && port.binding.stateLocked() == BindingActive {
			ports = append(ports, port)
		}
	}
	c.mu.Unlock()
	sort.SliceStable(ports, func(i, j int) bool {
		return ports[i].binding.role == RoleRelay && ports[j].binding.role != RoleRelay
	})
	for _, port := range ports {
		if ctx.Err() != nil {
			return
		}
		accepted, depth := port.deliver(result)
		if !accepted && port.binding.role.consumer() && depth >= stationReceiveQueueDepth {
			c.log.Warn("consumer receive queue full, dropping frame",
				zap.String(string(port.binding.role), port.binding.name))
		}
		if accepted && port.binding.role == RoleRelay && depth >= relayReceiveQueueWarning &&
			depth&(depth-1) == 0 {
			c.log.Warn("relay receive queue is backing up",
				zap.String("relay", port.binding.name), zap.Int("queue_depth", depth))
		}
	}
}

type operationResult struct {
	busy   bool
	report TxReport
	err    error
}

type radioOperation struct {
	port   *controllerPort
	run    func(Device) operationResult
	done   chan operationResult
	ctxErr func() error

	// canceled and started are owned by Controller.mu. Once started is true,
	// cancellation waits for the hardware result so an on-air operation can
	// still be accounted by its caller.
	canceled bool
	started  bool
}

func (c *Controller) enqueue(operation *radioOperation) error {
	c.mu.Lock()
	if operation.contextError() != nil {
		c.mu.Unlock()
		return operation.contextError()
	}
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
		if binding := c.bindings[key]; binding != nil && binding.role.consumer() {
			order = append(order, key)
		}
	}
	sort.Strings(order)
	c.rrOrder = order
	if len(order) == 0 || !containsSorted(order, c.rrLast) {
		c.rrLast = ""
	}
}

func containsSorted(values []string, value string) bool {
	if value == "" {
		return false
	}
	at := sort.SearchStrings(values, value)
	return at < len(values) && values[at] == value
}

func (c *Controller) execute(dev Device, operation *radioOperation) {
	c.mu.Lock()
	var refusal error
	switch {
	case operation.canceled:
		refusal = operation.contextError()
		if refusal == nil {
			refusal = context.Canceled
		}
	case operation.contextError() != nil:
		refusal = operation.contextError()
	case operation.port.closed:
		refusal = ErrBindingClosed
	case operation.port.binding.stateLocked() != BindingActive || c.physical != dev:
		refusal = ErrControllerDown
	default:
		operation.started = true
	}
	c.mu.Unlock()
	if refusal != nil {
		operation.done <- operationResult{err: refusal}
		return
	}
	operation.done <- operation.run(dev)
}

func (o *radioOperation) contextError() error {
	if o.ctxErr == nil {
		return nil
	}
	return o.ctxErr()
}

// cancelOperation abandons an operation that has not started. Once hardware
// execution begins, the caller must wait for the report: returning early would
// let a real transmission escape duty accounting.
func (c *Controller) cancelOperation(operation *radioOperation) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if operation.started {
		return false
	}
	operation.canceled = true
	c.removeOperationLocked(operation)
	return true
}

func (c *Controller) removeOperationLocked(want *radioOperation) {
	remove := func(queue []*radioOperation) []*radioOperation {
		for i, operation := range queue {
			if operation == want {
				return append(queue[:i], queue[i+1:]...)
			}
		}
		return queue
	}
	c.relayQueue = remove(c.relayQueue)
	for key, queue := range c.stationQueues {
		queue = remove(queue)
		if len(queue) == 0 {
			delete(c.stationQueues, key)
		} else {
			c.stationQueues[key] = queue
		}
	}
	c.rebuildRROrderLocked()
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

	// closed is owned by Controller.mu and gates hardware operations. The
	// receive queue has its own ownership so broadcast never blocks the
	// controller's physical operation loop.
	closed  bool
	mu      sync.Mutex
	queue   []receiveResult
	wake    chan struct{}
	done    chan struct{}
	stop    error
	airtime func(int) time.Duration
}

const (
	stationReceiveQueueDepth = 32
	relayReceiveQueueWarning = 64
)

func (p *controllerPort) Envelope() Envelope { return p.binding.controller.envelope }

func (p *controllerPort) Configure(waveform Waveform) error {
	c := p.binding.controller
	c.mu.Lock()
	defer c.mu.Unlock()
	if p.closed || c.bindings[bindingKey(p.binding.role, p.binding.name)] != p.binding {
		return ErrBindingClosed
	}
	if waveform != p.binding.waveform {
		return errors.New("logical radio cannot be retuned outside its binding")
	}
	state := p.binding.stateLocked()
	if state == BindingBlocked {
		authority := c.authoritativeLocked()
		cause := "no authoritative waveform"
		if authority != nil {
			cause = fmt.Sprintf("%s %q is authoritative on a different waveform",
				authority.role, authority.name)
		}
		return fmt.Errorf("%w: %s", ErrBindingBlocked, cause)
	}
	return nil
}

func (*controllerPort) StartReceive() error { return nil }

func (p *controllerPort) Receive(ctx context.Context) (Frame, error) {
	for {
		p.mu.Lock()
		if p.stop != nil {
			err := p.stop
			p.mu.Unlock()
			return Frame{}, err
		}
		if len(p.queue) > 0 {
			result := p.queue[0]
			p.queue[0] = receiveResult{}
			p.queue = p.queue[1:]
			p.mu.Unlock()
			return result.frame, result.err
		}
		p.mu.Unlock()
		select {
		case <-ctx.Done():
			return Frame{}, ctx.Err()
		case <-p.done:
		case <-p.wake:
		}
	}
}

func (p *controllerPort) deliver(result receiveResult) (accepted bool, depth int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stop != nil {
		return false, -1
	}
	if p.binding.role.consumer() && len(p.queue) >= stationReceiveQueueDepth {
		return false, len(p.queue)
	}
	p.queue = append(p.queue, result)
	select {
	case p.wake <- struct{}{}:
	default:
	}
	return true, len(p.queue)
}

func (p *controllerPort) operation(ctx context.Context, operation *radioOperation) (operationResult, error) {
	operation.port, operation.done = p, make(chan operationResult, 1)
	operation.ctxErr = ctx.Err
	if err := p.binding.controller.enqueue(operation); err != nil {
		return operationResult{}, err
	}
	select {
	case <-ctx.Done():
		if p.binding.controller.cancelOperation(operation) {
			return operationResult{}, ctx.Err()
		}
		// Hardware execution already started. Preserve its result so a
		// completed transmission reaches the shared duty ledger.
		result := <-operation.done
		return result, result.err
	case result := <-operation.done:
		return result, result.err
	}
}

func (p *controllerPort) Transmit(ctx context.Context, payload []byte, powerDBm int8) (TxReport, error) {
	return p.transmit(ctx, payload, powerDBm, true)
}

// TransmitForwarded is Transmit for a packet this node is only
// passing on; see Forwarder for why the peers are not told.
func (p *controllerPort) TransmitForwarded(ctx context.Context, payload []byte, powerDBm int8) (TxReport, error) {
	return p.transmit(ctx, payload, powerDBm, false)
}

func (p *controllerPort) transmit(ctx context.Context, payload []byte, powerDBm int8, handOver bool) (TxReport, error) {
	raw := append([]byte(nil), payload...)
	result, err := p.operation(ctx, &radioOperation{run: func(dev Device) operationResult {
		report, err := dev.Transmit(ctx, raw, powerDBm)
		return operationResult{report: report, err: err}
	}})
	if handOver && (err == nil || result.report.Airtime > 0) {
		// The chip was keying, so the peers sharing it heard nothing.
		// The caller's context is dropped deliberately: a transmit
		// cancelled mid-key still went out and still paid the duty
		// ledger, so the peers must hear what the mesh heard.
		// A report carrying airtime means the frame radiated even when
		// the driver's hand-back to receive subsequently failed.
		// handOver never blocks.
		p.binding.controller.handOver(context.WithoutCancel(ctx), p, raw)
	}
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
	if p.closed {
		return nil
	}
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
	if p.airtime != nil {
		// Airtime is pure arithmetic at the configured waveform. Keeping
		// the calculator of this logical session prevents a controller
		// bounce from silently turning a shadow reservation into zero.
		return p.airtime(bytes)
	}
	return 0
}

func (p *controllerPort) Close() error {
	c := p.binding.controller
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closePortLocked(p, ErrBindingClosed)
	return nil
}

func (c *Controller) closePortLocked(port *controllerPort, err error) {
	if port.closed {
		return
	}
	port.closed = true
	delete(c.ports, port)
	if err == nil {
		err = ErrControllerDown
	}
	c.dropPortOperationsLocked(port, err)
	port.mu.Lock()
	port.stop = err
	port.queue = nil
	close(port.done)
	port.mu.Unlock()
}

func (c *Controller) dropPortOperationsLocked(port *controllerPort, err error) {
	fail := func(queue []*radioOperation) []*radioOperation {
		kept := queue[:0]
		for _, operation := range queue {
			if operation.port != port {
				kept = append(kept, operation)
				continue
			}
			operation.canceled = true
			select {
			case operation.done <- operationResult{err: err}:
			default:
			}
		}
		return kept
	}
	c.relayQueue = fail(c.relayQueue)
	for key, queue := range c.stationQueues {
		queue = fail(queue)
		if len(queue) == 0 {
			delete(c.stationQueues, key)
		} else {
			c.stationQueues[key] = queue
		}
	}
	c.rebuildRROrderLocked()
}
