# Radio plumbing

This document describes Lotor's complete radio plumbing: how configuration
becomes one physical transceiver session, how relays and stations attach to it,
and how receive, transmit, airtime, authority, and failure handling cross the
layers. It is an implementation guide for maintainers. The product-level rules
remain in [`DESIGN.md`](../../DESIGN.md).

## Invariants

The architecture protects a small set of non-negotiable properties:

- exactly one controller owns and calls a physical radio device;
- at most one relay binds to a radio, while zero or more stations may share it;
- a relay is always waveform authority when present;
- without a relay, the oldest attached station is stable waveform authority;
- there is no waveform time slicing;
- all hardware operations are serialized;
- queued relay hardware operations have strict priority over station operations;
- station operations are fair to one another through round-robin scheduling;
- every non-dry producer on a radio shares one physical airtime ledger;
- `shadow` consumes that ledger even though it does not key the transmitter;
- one physical reception is delivered to every active open logical session;
- a slow station may lose receptions, but may never stall the relay; and
- a physical radio failure does not stop the daemon, a station listener, or an
  unrelated relay.

## Layer map

The production data path has five distinct layers:

```text
configuration and manager                  cmd/lotor/manager.go
        │
        ├── shared AirtimeLedger (one per physical radio)
        │
        ▼
radio Controller                           internal/radio/controller.go
        │ owns exactly one physical Device
        │
        ├── relay Binding ── controllerPort ── relay ── protocol engine
        │
        ├── station Binding ─ controllerPort ─ station service
        └── station Binding ─ controllerPort ─ station service
        │
        ▼
radio Device interface                     internal/radio/radio.go
        │
        ▼
hardware driver                            internal/radio/sx126x/
```

The manager owns topology: which configured consumer is attached to which
radio and which objects survive a consumer bounce. The controller owns physical
serialization and waveform authority. Relay and station protocol engines own
mesh policy: what to send, when it becomes due, its protocol priority, and
whether it should be dropped. The driver alone owns chip, SPI, GPIO, and IRQ
mechanics.

## Core radio types

### `Driver`

A `radio.Driver` is a registered hardware implementation. `Inspect` validates a
resolved hardware configuration and returns its `Envelope` without touching the
device. `Open` creates the physical `Device`. Optional checks validate transmit
prerequisites and waveform values before a live configuration is admitted.

Hardware profiles resolve into the opaque configuration passed to the driver.
No layer above `internal/radio` may interpret chip pins, SPI settings, TCXO
control, or RF-switch details.

### `Envelope` and `Waveform`

An `Envelope` is immutable physical capability: supported frequency range,
chip power range, and the board integrator's power ceiling. A `Waveform` is a
logical consumer's choice: frequency, SF, bandwidth, coding rate, preamble,
sync word, and CRC.

The distinction is deliberate. A radio says what is possible; a relay or
station asks for a channel. `Envelope.Permits` refuses an invalid choice and
never silently clamps explicit power.

### `Device`

`radio.Device` is the protocol-neutral transceiver seam. It can be configured,
receive frames, assess the channel, transmit, calculate airtime, and expose
cached hardware telemetry. In the production data path, a raw driver device is
called only by the controller. Consumers see a logical implementation of the
same interface: a `controllerPort`.

### `Controller`, `Binding`, and `controllerPort`

A `Controller` is the durable owner of one configured physical radio. It may
outlive many driver opens, relay sessions, and station RF sessions.

A `Binding` is a durable logical attachment between one consumer and that
controller. It records the consumer role, name, requested waveform, and bind
sequence. Registering a binding does not open another physical device.

`Binding.Open` or `OpenContext` creates a short-lived `controllerPort`. This is
the `radio.Device` seen by a relay engine or station service. Its calls are
proxies:

- `Receive` reads the binding's private inbox;
- `AssessChannel` and `Transmit` submit serialized controller operations;
- telemetry reads the currently owned physical device's cached values;
- `Configure` verifies the binding's already-declared waveform rather than
  retuning hardware directly; and
- `Close` closes only the logical session, not the physical radio or binding.

Closing a port therefore ends an engine session. `Binding.Unbind` is the
stronger topology operation: it removes the consumer, closes its port, rejects
its queued operations, and may select a new waveform authority.

## Assembly

### Physical radio

For each configured radio, the manager resolves its hardware profile, looks up
the driver, and calls `radio.NewController`. Construction runs `Driver.Inspect`
but does not open hardware. The controller task starts immediately and waits
until at least one binding supplies an authoritative waveform.

The controller then performs the only physical lifecycle:

```text
driver.Open → Envelope check → Configure(authoritative waveform)
            → StartReceive → receive/operation loop
```

Changing the authoritative waveform increments a configuration generation and
cancels the active receive call. The controller closes and reopens/configures
the physical device against the new generation. An ordinary compatible bind or
unbind does not bounce hardware.

### Relay

Relay assembly resolves the protocol configuration, builds and arms the engine,
validates the waveform against the radio, obtains the radio's shared duty
ledger when TX is enabled, and registers a `RoleRelay` binding. A second relay
binding is refused.

`relay.NewAttached` receives `Binding.OpenContext` as its device opener. The
relay lifecycle can restart its protocol session without opening the driver or
competing for physical ownership. Once its logical port is open, the protocol
engine sees an ordinary `radio.Device` interface.

### Station

A station service is created independently of RF and owns its application
listener, identity, contacts, preferences, and mailbox. Its `RadioDemand`
describes the current waveform, power, and duty budget, including changes made
through its companion protocol.

Attaching validates that demand, registers a `RoleStation` binding, and supplies
the shared ledger when its TX mode is not `dry`. Detaching or moving a station
unbinds only its RF side. The service and its TCP client remain alive while its
RF state becomes `detached`, `down`, or `blocked`.

## Waveform authority and binding state

Authority is deterministic:

1. the relay binding, if present;
2. otherwise the station with the oldest bind sequence; or
3. no authority when the radio has no bindings.

Only bindings whose waveform exactly matches the authority are active. A
different station waveform produces `BindingBlocked`; it never retunes a
relay-owned radio. On a station-only radio, the oldest station remains stable
authority until it changes its own request or unbinds. This prevents map order,
restarts of another station, or connection timing from changing the channel.

Controller and binding state are separate:

| State | Meaning |
|---|---|
| controller `starting` | no authority yet, or opening/configuring hardware |
| controller `running` | one physical device is configured and receiving |
| controller `error` | the physical lifecycle failed and is backing off |
| binding `active` | waveform matches and the physical controller is running |
| binding `down` | compatible, but no usable physical session exists |
| binding `blocked` | its waveform conflicts with the current authority |

A blocked binding is a topology result, not a radio fault. The authoritative
consumer and compatible peers continue normally.

## Receive path

The controller is the only caller of the physical `Device.Receive` method. A
successful frame, and a corrupt frame carrying `radio.ErrCorrupt`, is copied to
every open, active binding.

Delivery deliberately distinguishes the forwarding path from optional clients:

```text
physical Receive
      │
      ├── relay queue       lossless, independently drained
      ├── station A inbox   bounded (32), drop when full
      └── station B inbox   bounded (32), drop when full
```

The controller never waits for a logical consumer while delivering. Each port
owns its receive queue: the relay queue is lossless and warns at exponentially
spaced high-water marks, while station queues are bounded and lossy. A slow
relay therefore consumes memory visibly but cannot deadlock the controller's
hardware-operation scheduler. A companion that stops reading can lose its own
view of traffic, with a warning, but cannot apply backpressure to the relay or
physical receive loop. Companion socket pushes cross a separate bounded writer
queue, so a zero TCP window cannot stop that station's RF processing either.

The driver assigns a correlation identifier when a reception first crosses the
hardware seam. The same `radio.Frame` and correlation then reach every logical
consumer, allowing logs and bus events to describe their different decisions
about one on-air fact. A controller-local hand-over receives its own reception
correlation and carries `caused_by`, the correlation of the composed emission,
plus the emitting binding. Its RF fields are explicitly absent: their zero
values are never measurements.

Noise floor and chip counters are measured and cached by the physical device.
Logical ports only read that cache. The relay monitor mirrors selected values
into runtime status and the event bus; it never performs a second hardware
read.

## Transmit and hardware-operation path

Protocol engines first make their own decisions. They own protocol queues,
not-before times, packet priority, LBT retry/drop policy, and TX mode. The radio
controller does not understand MeshCore packet kinds or priorities.

When an engine needs hardware, its logical port submits a closure to the
controller. Submission cancels the currently blocking physical receive call;
the controller executes the operation on its owning goroutine and then resumes
receive. This serializes `AssessChannel` and `Transmit` with reception and with
every other consumer.

Controller scheduling is intentionally simple:

- pending relay operations are FIFO and always selected first;
- if no relay operation is pending, one station operation is selected;
- stations are visited round-robin, while each station's own operations remain
  FIFO; and
- an operation already executing is not preempted.

Cancellation removes an operation that has not started. Once hardware
execution has begun, the logical call waits for its report even if its parent
session is cancelled; this is required for a real transmission to reach the
shared airtime ledger. Protocol transmitters provide their own bounded context
without inheriting session cancellation, while each LBT request is bounded by
the remaining LBT window.

Strict relay priority can starve stations under sustained relay hardware work.
That is an explicit safety policy: forwarding continuity wins over local
companion origination. Round-robin prevents one station from starving its peers
when relay work leaves capacity.

The normal emission sequence above the controller is:

```text
protocol queue → shared duty reservation → optional LBT/CAD
               → Transmit (on-air only) → reservation commit
               → FrameSent / log / metrics
```

If a receive is pending when CAD is requested, the driver may return
`ErrBusyReceiving`; the protocol layer decides when to requeue. Hardware errors
return through the logical operation and do not create a second physical owner.
`TxReport.Airtime > 0` is the unambiguous radiated boundary: a driver may return
that report with an error when transmission completed but restoring receive
mode failed. Such a frame is accounted and handed to co-located bindings just
like any other radiated composed emission; a zero-airtime failure is not.

### TX modes

- `dry` stops before physical TX accounting and has no shared ledger consumer;
- `shadow` runs the decision pipeline and any configured channel assessment,
  does not call `Transmit`, but commits the estimated airtime to the shared
  ledger; and
- `on-air` calls the physical transmitter and replaces its reserved estimate
  with measured airtime from `TxReport`.

Relays additionally support their relay-specific zero-hop gate. Stations never
forward and therefore expose only `dry`, `shadow`, and `on-air`.

## Shared airtime ledger

The duty ledger belongs to a physical radio, not to a relay, station, binding,
or controller port. The manager keeps one `AirtimeLedger` per radio name and
hands the same pointer to every non-dry producer attached to it.

All consumers of one radio must resolve the same duty budget. A mismatch is a
configuration error; choosing either value would make the other consumer's
configuration false. The initial ledger restores recent real and shadow
emissions from the sentinel journal across all producers on that radio. The
object survives consumer session and binding bounces so restarting a relay does
not erase airtime already spent.

`Reserve` atomically includes outstanding emissions in admission. The caller
must then:

- `Commit` with actual or shadow airtime when the emission happened logically;
  or
- `Cancel` when it did not.

This prevents two producers from both spending the same remaining capacity
while they independently perform LBT. Shadow accounting is deliberate: it
models how much shared capacity the exact decision stream would consume if
enabled on air.

## Failure and reconfiguration

The controller supervises physical failures with capped exponential backoff.
When the physical session ends it closes all logical ports, fails pending
operations, exposes the cause, and retries. Bindings remain registered, so
consumers can open fresh logical sessions after recovery.

The relay and station lifecycles react differently by design:

- a relay session enters an error state and retries its engine against the same
  binding;
- a station's RF loop retries independently, while its application listener and
  durable state remain available; and
- a station `radio=` mutation unbinds and reattaches RF without closing its TCP
  listener or current application connection.

Invalid declarative configuration is different from a recoverable device
failure. Preflight creates a visible stillborn relay or unavailable attachment
instead of retrying hardware forever with a choice that can never work.

Preflight is topological: changing a radio, relay, or attached station judges
every consumer on the resulting radio before persistence. Driver waveform and
transmit checks apply to stations as well as relays, and all non-dry consumers
must request the same physical duty budget.

## Observability

Logs should identify the physical and logical dimensions independently:

- `radio` for controller/driver and on-air facts;
- `relay` or `station` for the consumer decision; and
- `corr` for the frame lineage.

The bus follows the same distinction. The relay pipeline publishes
`FrameHeard` and `FrameJudged`, including the emitting binding and causal
correlation for a local hand-over; stations update their own companion-facing
statistics and notifications from the same controller fan-out. The sentinel
stores that provenance and never presents a hand-over's zero RF fields as
measurements. `FrameSent` and `TxDropped` identify either producer through their
source kind and source. Real and shadow sends remain distinguishable even though
both consume the duty ledger.

Trace logs expose hardware and developer plumbing: controller state, chip
verdicts, airtime, power and physical outcomes, plus companion framing and
mailbox operations. Debug logs describe traffic and routing decisions made by
relays or stations, and successful station configuration mutations. Mailbox
logs carry the originating frame's `corr` through durable storage so enqueue
and later delivery remain one searchable story across a daemon restart.

## Extension rules

When extending this plumbing:

1. Put chip, bus, GPIO, IRQ, and register knowledge behind a radio driver.
2. Put topology and shared-object lifetime in the manager.
3. Put physical ownership, authority, RX fan-out, and operation fairness in the
   controller.
4. Put mesh packet priority, timing, LBT policy, and drop decisions in the
   protocol consumer.
5. Bind every new data-plane consumer through the controller; never open a
   second driver device for the same radio.
6. Reuse the radio's ledger; never create a per-consumer duty account.
7. Decide explicitly whether a new consumer's RX delivery may be lossy. It must
   not weaken the relay's lossless door.
8. Do not add time slicing implicitly. Supporting multiple waveforms would be a
   different architecture and must define retune cost, RX blindness, queue
   semantics, and authority before code changes.

## Code guide

- [`internal/radio/radio.go`](../../internal/radio/radio.go): hardware-neutral
  types, `Driver`, `Envelope`, `Waveform`, and `Device`.
- [`internal/radio/controller.go`](../../internal/radio/controller.go): physical
  ownership, bindings, RX fan-out, and hardware-operation scheduling.
- [`internal/radio/airtime.go`](../../internal/radio/airtime.go): shared sliding
  duty window and atomic reservations.
- [`internal/radio/sx126x/`](../../internal/radio/sx126x/): Semtech hardware driver.
- [`cmd/lotor/manager.go`](../../cmd/lotor/manager.go): controller and ledger
  lifetime, topology, attachment, and rebinding.
- [`internal/relay/relay.go`](../../internal/relay/relay.go): relay session
  lifecycle over a logical controller port.
- [`internal/protocol/meshcore/`](../../internal/protocol/meshcore/): relay RX/TX
  policy and protocol queues.
- [`internal/station/station.go`](../../internal/station/station.go):
  protocol-neutral station lifecycle and RF attachment contract.
- [`internal/station/meshcore/`](../../internal/station/meshcore/): MeshCore station
  application protocol, RX decisions, origination queue, LBT, and TX policy.
