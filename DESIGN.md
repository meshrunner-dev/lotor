# Lotor — design notes

Lotor is a mesh relay daemon. It listens to LoRa mesh networks and
extends them: one machine, one daemon, one or more **relays** — each an
instance of a mesh protocol bound to its radio hardware.

MeshCore is the first supported protocol, and for a long while the only
one. The architecture, the configuration model and the vocabulary are
protocol-neutral from day one anyway: MeshCore is *a* protocol Lotor
speaks, not what Lotor *is*. Early UX may hide the distinction (a
single-protocol setup should not pay a complexity tax); the code and
config never conflate the two.

This is an independent implementation. It is not affiliated with or
endorsed by the MeshCore project.

## Vocabulary

- **Lotor** — the daemon and the project. Capitalised in prose; never
  used in code symbols (types, functions, internal packages), so that a
  forced project rename stays cheap: module path, `cmd/` directory and
  docs, nothing else.
- **relay** — the generic name of the role. Mesh networks name it
  differently (repeater here, router there); *relay* describes what the
  role does — relaying frames — without promising flood or routing
  semantics, which are the protocol's business. In protocol-specific UX
  the native term may surface (a MeshCore relay presents itself as a
  repeater on that network); code, config and logs say relay.
- **radio** — a physical transceiver attachment: a bus, pins, and the
  board's physical envelope. Radios carry no waveform *choice*; they
  declare what choices are possible.
- **sentinel** — the observation and archival instantiation: the
  packet and routing-decision views the UI shows, the disk or network
  archival of heard frames, and the storage behind them. Whether one
  sentinel serves the daemon or each relay carries its own is an open
  question; what is decided is that a deployment may run **none at
  all** — embedded hosts with tight RAM and CPU relay without
  observing, and nothing else may depend on a sentinel existing.

- **scope** — a MeshCore *transport scope*: a named partition of the
  mesh that a flood is confined to. A scope is a shared secret — a
  16-byte key derived from its name — and a scoped flood carries a
  two-byte code, recomputed per packet as `HMAC(key, type‖payload)`,
  that only a node holding the key can match. It is a mesh agreement,
  independent of the radio band, and the two words must not be
  conflated: a *band* is what the radio is tuned to, a *scope* is which
  slice of the traffic on that band a relay carries. A relay declares
  one `default_scope` it originates under and a set of `accept_scopes`
  it will relay; the reference calls the outgoing one the default
  scope, and this follows that name.

  Code, configuration, journal and console say *scope*, always. A web
  UI may present it as a *region* instead, because that is the word
  MeshCore operators arrive with — a deliberate translation at the
  edge, not an inconsistency to tidy away.

## Architecture

One process. N relays, each a supervised task tree. Each radio is owned
exclusively by one relay — the single-owner discipline of the driver
layer, one level up. Config may later allow one relay to own several
radios; it will never allow one radio to serve several relays.

```
lotor daemon
├── relay "meshcore-868"   (protocol: meshcore)
│     └── radio "slot1"    (driver: sx126x-spi)
├── relay "meshcore-433"   (protocol: meshcore)
│     └── radio "hat2"
├── internal event bus  ── journal, metrics, SSE, CLI
└── web UI + telnet CLI
```

The **relay owns the waveform**: frequency, LoRa parameters (SF/BW/CR,
sync word, preamble), TX power, channel-activity detection tuning, LBT
budget, duty-cycle policy. The **radio owns the attachment and the
envelope**: driver, bus, GPIO, TCXO — and the board's physical limits
(maximum TX power, served frequency range). The relay makes choices;
the radio bounds them. This seam is the library seam: radio config
feeds the driver's chip `Config`, relay config feeds the channel
`Params`, and the cap semantics mirror the driver's: it refuses excess
power, it never clamps.

Failure model: a relay whose radio does not answer its identity check
comes up in a visible `error` state and retries with backoff — the
daemon and its other relays keep running. Never start blind on dead
hardware, never take the whole daemon down for one sick relay.

## Configuration

One reusable layering mechanism — a named **profile** plus overrides
scoped *by profile name* — instantiated twice:

- **hardware profiles** on radios (board presets: pins, TCXO, RF
  switch, PA caps);
- **band profiles** on relays (frequency plans, LoRa parameters —
  e.g. a MeshCore EU narrow preset). Called *band* deliberately, not
  *region*: on a MeshCore mesh "region" would collide with a transport
  scope (see the Vocabulary entry), which is a mesh agreement, not a
  radio fact.

```yaml
radios:
  slot1:
    driver: sx126x-spi
    profile: rak6421-13300x-slot1     # or "custom" = empty base
    overrides:
      rak6421-13300x-slot1:
        # this board needs nothing beyond the preset
      custom:
        spi: /dev/spidev0.0
        reset_pin: 16
        busy_pin: 24
        max_tx_power_dbm: 20      # envelope, not a choice
        frequency_range: [863e6, 870e6]

relays:
  meshcore-868:
    protocol: meshcore
    radio: slot1
    profile: eu-868-narrow
    overrides:
      eu-868-narrow:
        tx_power_dbm: 5
      custom:
        frequency_hz: 433500000
        spreading_factor: 10
```

Properties this buys:

- **Switching profiles is non-destructive.** Overrides live under the
  profile they patch; flipping the `profile` knob carries nothing over
  from the previous choice, and several tuned profiles coexist in the
  same file.
- **Strict validation.** Unknown keys are rejected, not ignored — a
  typo is an error at load time, in the driver-parameter spirit of the
  radio library.
- **Provenance on demand.** A resolved view (CLI/API) shows, key by
  key, `profile → override → effective` with the winning source named;
  driver defaults apply beneath these layers, in the driver's own
  code. Config debugging is grep, not archaeology, and every override
  in effect is announced at startup.

### Capabilities vs choices

Hardware profiles legitimately carry more than the bus: a board knows
its maximum TX power and the band its filters and antenna path serve.
Those are **capabilities** — the envelope — not waveform choices, so
they live on the radio without stealing the relay's authority:

- binding a relay whose frequency falls outside the radio's declared
  range is a load-time error (the relay comes up in `error`, visibly);
- `tx_power_dbm: auto` (the relay default) resolves to the lower of
  the band profile's default and the radio's cap, and the resolution
  is logged with provenance;
- an **explicit** `tx_power_dbm` above the radio's cap is refused,
  never clamped — defaults may be prudent, explicit choices never lie.

Defaults mirror the MeshCore reference firmware — the forced transmit
after the CAD-fail budget elapses, the 160-entry count-bounded dedup
ring. Advanced knobs exist for
site operators with real needs, and a relay running non-stock values
says so at startup — traceability includes configuration.

## Traceability

- **Zap**, structured logging everywhere, context enriched as frames
  cross layers: `relay=… radio=… txn=…`.
- **Every frame heard gets a transaction ID** carried by every log line
  that concerns it. IDs are generated OpenTelemetry-compatible
  (128-bit trace id) but displayed truncated (8–12 hex chars) and
  greppable by prefix. Relations are structured fields:
  `txn=a3f9c210 duplicate_of=8e01bb42` — grep one, find the other,
  follow the chain. A future OTel exporter is a mapping, not a
  migration.
- **Metrics** live in an in-RAM registry, with optional periodic
  persistence — embedded systems on write-fragile eMMC can keep them
  RAM-only. A future Prometheus exporter reads the same registry.
- **The message journal is the sentinel's storage**: transiting frames
  recorded (SQLite; RAM/tmpfs mode for eMMC), browsable from the web
  UI, retained on disk for a long default — and entirely absent when
  no sentinel runs.

## Interfaces

- **Internal event bus** — the spine. Typed envelopes with provenance
  for every event: frame heard, judged, corrupt, radio state change.
  Consumers today: the sentinel's journal and the CLI (watch is the
  live view). Metrics and the SSE stream arrive with their features
  and subscribe the same way. The bus is what makes the sentinel's
  optionality free — publishing to zero subscribers costs nothing —
  and what keeps later ambitions cheap (see below).
- **Web UI** — to come: minimalist first, backed by SSE from the bus.
  The visual structure for multi-relay realities will iterate; the
  data feed will not. The whole web server is a build-time option: light builds omit
  it and embed no UI filesystem at all — a headless binary for hosts
  where flash and RAM are counted.
- **CLI over telnet** for now (SSH considered later), plus an always-on
  local console over a unix socket. Privilege follows the transport:
  the socket is **admin** because the OS's file permissions already
  proved privileged access; network transports **authenticate** — SSH
  when it comes, and today's unauthenticated telnet is read-only.
  Read-only and admin are indistinguishable while no command writes;
  the distinction exists so the first admin command lands on a
  contract, not a retrofit.

## Build profiles

Two build flavours share one source tree; the `lean` tag selects the
second. The contract per feature:

| | normal | lean |
|---|---|---|
| noise-floor measurement | always on (a base function of receiving) | always on |
| noise-floor archiving | on by default, `noise_history: false` per relay opts out | off by default, `noise_history: true` opts in |
| shell command history | on | absent |
| web UI | on by default (build-time option) | absent |

Archiving gates exist because disk writes are a budget, like the
sentinel itself: a gate closed keeps the live value in RAM, nothing
else. Measurement never gates — knowing the channel is not optional.

## The transmit path — designed, staged

Decided 2026-08-26; built in stages behind visible gates. Nothing here
contradicts the receive-only present: every stage below `on-air` emits
nothing.

**Gates.** Per relay, a `tx:` block with three modes: `dry` (absent =
default; today's behaviour), `shadow` (the whole pipeline runs — LBT,
queue, duty accounting — and the emission that *would* have happened is
journalled with its exact instant, airtime and power, but the radio is
never keyed), `on-air`. The mode shows in `status`; changing it is
journalled. `on-air` refuses to start without a declared radio power
cap and a resolved `tx_power_dbm` — a misconfigured transmitter is a
stillborn relay, never a silent one.

**Channel politeness (LBT).** Before keying: an RSSI check against the
measured floor (`lbt_threshold_db` above the p50; disabled by default —
field experience says the check is unreliable, and disabled is also
the reference's default) then a hardware CAD scan, cheapest first.
Never while a reception is in progress. On a busy verdict, randomized
~200 ms retries bounded to ~4 s total; exhausted, the frame is sent
anyway — the mesh's convention — unless the site chose
`lbt_exhausted: drop`, in which case the drop is counted and visible.

**Queue.** The reference's shape exactly: one bounded queue of
`(priority, not-before)` entries served by priority once due. Two
priorities: direct traffic and ACKs ahead, flood relays behind. The
flood desynchronisation delay — the reference's SNR-score formula, so
well-heard repeaters yield to badly-heard ones — is simply the entry's
`not-before`. A full queue refuses the newcomer with a counted drop;
nothing is evicted silently. One standing constraint: nothing slow may
sit between collecting a frame and the transmit that answers it.

**Duty cycle.** Every emission, real or shadow, leaves a ledger row in
the journal: instant, airtime, frequency, applied power, type, and the
transaction it relays. Enforcement is a sliding one-hour window whose
percentage comes from the band preset, overridable. At saturation,
candidates wait in the same priority order until a deadline, then drop,
counted. The cap is never exceeded, for anyone. The gauge shows in
`status`; a `tx_duty` series joins the metrics tiers.

**What is sent.** v1 turns the existing verdicts into actions:
`would-relay-flood` retransmits with our hash appended to the path,
`would-relay-direct` and the trace family likewise. No originated
traffic except the adverts a repeater owes the mesh:
`advert_flood_interval` (hours; default 48h, active when on-air) and
`advert_local_interval` (minutes; zero-hop, default 0 = off). On-air
admin responses — the stats the telemetry already gathers — come
later, as their own protocol work.

**Traceability.** `FrameSent` carries the origin transaction, instant,
airtime and applied power; `TxDropped` carries its reason (queue,
duty, channel when drop is chosen). The journal links them, and `txn`
shows the full life: heard → judged → sent. Shadow entries are marked
as such — they are the audit trail that earns `on-air`.

**Validation ladder.** Shadow on the production band first: days of
journalled decisions, audited against what the mesh's real repeaters
did. Then on-air on a test channel at minimal power with a witness
node: reception, path append, mutual dedup, ledger accuracy. Then
on-air on the band with zero-hop adverts only. Relaying on the
production band is the operator's decision, not a default.

## Later — designed for, not built

None of this exists, and nothing above may contradict it:

- **Bridges between relays** (e.g. MeshCore 868 ↔ 433) — a bus consumer
  with programmable rules that re-injects into another relay. The bus's
  typed, provenance-carrying envelopes are the enabler.
- **Prometheus exporter** and **SNMP agent** — readers of the existing
  metrics registry.
- **Alerting** (webhooks, bot messages) — one more bus consumer.
- **Log shipping** to a collector — protocol undecided.
- **ESP32-class targets**, to be studied short-term. A study, not a
  contract: current work takes no constraint from it. Linux stays the
  first-class platform; an embedded build would keep the core — relay,
  protocol, radio seam — and shed the optional layers, the sentinel
  first. The radio library's transport interfaces are the door: a
  bare-metal SPI transport behind the same seam.

## Ground rules

- Low-level correctness is the obsession. The radio and protocol layers
  are the two published, hardware-validated, reference-tested modules;
  Lotor consumes them and reimplements nothing at those layers.
- Wire-protocol fidelity to the MeshCore reference is non-negotiable;
  behavioural defaults align with the reference firmware, deviations
  are opt-in and loudly logged.
- Errors recover automatically where possible and are always visible;
  silent degradation is a bug by definition.
