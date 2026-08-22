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
- **region/channel profiles** on relays (frequency plans, LoRa
  parameters — e.g. a MeshCore EU narrow preset).

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
  key, `driver default → profile → override → effective` — config
  debugging is grep, not archaeology.

### Capabilities vs choices

Hardware profiles legitimately carry more than the bus: a board knows
its maximum TX power and the band its filters and antenna path serve.
Those are **capabilities** — the envelope — not waveform choices, so
they live on the radio without stealing the relay's authority:

- binding a relay whose frequency falls outside the radio's declared
  range is a load-time error (the relay comes up in `error`, visibly);
- `tx_power_dbm: auto` (the relay default) resolves to the lower of
  the region profile's default and the radio's cap, and the resolution
  is logged with provenance;
- an **explicit** `tx_power_dbm` above the radio's cap is refused,
  never clamped — defaults may be prudent, explicit choices never lie.

Defaults mirror the MeshCore reference firmware (e.g. the forced
transmit after the CAD-fail budget elapses). Advanced knobs exist for
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
- **The message journal** records transiting frames (SQLite; RAM/tmpfs
  mode for eMMC), browsable from the web UI, retained on disk for a
  long default.

## Interfaces

- **Internal event bus** — the spine. Typed envelopes with provenance
  for every event: frame heard, relayed, dropped, radio state change.
  Consumers today: journal, metrics, SSE stream, CLI. The bus is what
  keeps later ambitions cheap (see below).
- **Web UI** — minimalist first, backed by SSE from the bus. The visual
  structure for multi-relay realities will iterate; the data feed will
  not.
- **CLI over telnet** for now (SSH considered later).

## Later — designed for, not built

None of this exists, and nothing above may contradict it:

- **Bridges between relays** (e.g. MeshCore 868 ↔ 433) — a bus consumer
  with programmable rules that re-injects into another relay. The bus's
  typed, provenance-carrying envelopes are the enabler.
- **Prometheus exporter** and **SNMP agent** — readers of the existing
  metrics registry.
- **Alerting** (webhooks, bot messages) — one more bus consumer.
- **Log shipping** to a collector — protocol undecided.

## Ground rules

- Low-level correctness is the obsession. The radio and protocol layers
  are the two published, hardware-validated, reference-tested modules;
  Lotor consumes them and reimplements nothing at those layers.
- Wire-protocol fidelity to the MeshCore reference is non-negotiable;
  behavioural defaults align with the reference firmware, deviations
  are opt-in and loudly logged.
- Errors recover automatically where possible and are always visible;
  silent degradation is a bug by definition.
