# Lotor — design notes

Lotor is a mesh relay daemon — <https://meshrunner.dev/lotor>. It listens to LoRa mesh networks and
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
- **station** — a virtual companion endpoint: one protocol identity,
  its durable preferences, contacts and mailbox, exposed to exactly one
  application connection on a dedicated TCP port. *Station* names the
  architectural role without leaking MeshCore's companion vocabulary.
  A station originates and receives traffic; it never forwards it. It
  exists while detached from RF and may move between radios without
  losing its TCP connection or state.
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

- **region** — a MeshCore region: a named partition of the mesh that
  a flood is confined to, plus the administrative model around it. On
  the wire a region is a *transport scope* — a shared secret, the
  16-byte key derived from its name, and a scoped flood carries a
  two-byte code, recomputed per packet as `HMAC(key, type‖payload)`,
  that only a node holding the key can match. Around that wire fact
  the reference keeps a table: up to 32 entries under a wildcard
  root, each with a place in a display hierarchy (the matching stays
  flat), a deny-flood flag, and two designations — the *default*
  region a node's own traffic is scoped to, and its *home*. This
  daemon keeps the same table in the config store. Locally, the
  `/relay/<name>/regions` drawer exposes structured `put`, `def`,
  `allowf`, `denyf` and `drop` commands; `default` and `home` are
  drawer attributes administered with `set` and `unset`. Plain
  `print` is the structured table, while `print meshcore` emits the
  ecosystem's tree with its native indentation. The raw `region`
  grammar is not a local console escape. Over the air, an
  authenticated admin carries that grammar in the binary `CLI_COMMAND`
  text subtype and receives `CLI_DATA`, without a relay restart.

  The wildcard is unfortunately named: it is not a catch-all region,
  but the absence of a region and therefore of transport codes. Its
  flood flag governs plain floods alone. A coded flood moves only when
  its code verifies against a named, flood-allowed entry; an unknown
  code and a named `denyf` entry are both refused. Thus a factory-empty
  repeater relays plain traffic and no transport-coded traffic until
  regions are configured, matching the reference.

  A region is a mesh agreement, independent of the radio band, and
  the two words must not be conflated: a *band* is what the radio is
  tuned to, a *region* is which slice of the traffic on that band a
  relay carries. Code, journal, console and the wire say *region*,
  always — the ecosystem's own word. *Scope* survives only where the
  wire primitive is meant (a transport scope, `TransportKey`), and in
  two deprecated console spellings (`scopes`, `ask-scopes`) that
  leave next release. One legacy the rename does not touch: the
  journal's `would-drop-flood-scoped` verdict keeps its name, because
  renaming a recorded event would cut every archived query in two.

  Divergences, assumed — each one deliberate, none of them wire-visible
  in replies:

  - the reference persists its table only on `region save`; here every
    mutation persists before it installs, and `region save` is an
    honest OK that only bumps the discovery timestamp;
  - a reload (`region load` … blank line) preserves the home/default
    designations and the wildcard's flags by name, where the reference
    silently drops all three — and the loader strips the `^` home mark
    the dump glues onto a name, which read literally would corrupt the
    very region it designates;
  - `region def` composes on a candidate and applies atomically: a
    refused batch answers the reference's exact words but leaves the
    live table untouched, where the firmware keeps the segments that
    ran before the error;
  - the model refuses what the reference lets through and later chokes
    on: empty names, parent cycles, parents that name no entry, and
    duplicate names at restore;
  - private `$` regions are refused at every door — put, def, load,
    default, migration and the restored store alike — because the
    keystore they promise is not implemented, and announcing a region
    this relay can never match would be a lie on the air;
  - a `region load` staging is an exclusive OTA transaction: owned by
    the admin who armed it (full key), expiring after a minute of
    silence, and refusing other region mutations — including the
    structured local drawer — until it commits or lapses. The local
    console does not expose the raw modal grammar.

## Architecture

One process. N relays and N stations, each a supervised task tree. One
radio controller owns each physical driver session. At most one relay
may bind to it, while any number of compatible stations may share it;
this preserves the driver's single-owner discipline without confusing
physical ownership with logical consumers.

The implementation-level walkthrough is in
[`docs/internal-radio-architecture.md`](docs/internal-radio-architecture.md):
assembly, bindings, waveform authority, RX fan-out, TX scheduling, shared
airtime accounting, and failure recovery for both relays and stations.

```
lotor daemon
├── radio controller "slot1" (driver: sx126x-spi)
│     ├── relay "meshcore-868" (protocol: meshcore; authority)
│     ├── station "alice"     (TCP :5001)
│     └── station "bob"       (TCP :5002)
├── relay "meshcore-433"       (radio: hat2)
├── station "carol"            (detached; TCP :5003 stays available)
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

The relay is authoritative whenever it is present: its waveform tunes
the physical radio, and a station requesting another waveform is
`blocked` rather than retuning the relay. On a station-only radio, the
oldest attached station is the stable authority; other waveforms are
likewise blocked. There is no time slicing. Changing a station's radio
parameters may therefore change a station-only radio, but never one
owned by a relay.

Physical operations are serialized by the controller. Relay operations
have strict priority; station operations are served round-robin by
station. Inside a station, the MeshCore dispatcher priority is honoured
(the smaller numeric value wins) and equal priorities remain FIFO; an
emission scheduled for the future never blocks one already due. A physical reception is
broadcast to the relay and every active station binding. Station RX
queues are bounded and lossy so a stalled companion can never stall the
relay; the relay's receive door remains lossless. Every station uses the
same physical duty ledger as the relay and its peers. `shadow` is
deliberately capacity-realistic: it does not key the chip, but reserves
and commits the airtime it would have consumed. Admission is reserved
atomically across producers before LBT and committed with measured
airtime, so two logical consumers cannot spend the same remaining
budget.

A pin the radio owns is written `offset` or `chip:offset`: the bare
form is a line on the radio's `gpiochip`, which is what a board on a
header looks like, and the qualified form names its own chip, because
nothing promises a board keeps every line on one of them.

Failure model: a relay whose radio does not answer its identity check
comes up in a visible `error` state and retries with backoff — the
daemon and its other relays keep running. Never start blind on dead
hardware, never take the whole daemon down for one sick relay.

## Configuration

The configuration lives in **one SQLite file** — `config.db` in the
daemon's state directory, created 0600 because the node identity (the
private key) is in it. Copying that file backs up the whole relay;
putting it back restores it. The journal is deliberately a different
database: it churns, gets pruned, and may live in RAM, none of which a
backup target may do. Every mutation is recorded in a `revisions`
table — who, when, what, and the value it replaced — so the audit
trail travels inside the backup. The daemon is the store's only
writer; `lotor config import <yaml>` migrates a legacy file into it,
whole, and refuses to run beside a live daemon.

Within the store, one reusable layering mechanism — a named
**profile** plus overrides scoped *by profile name* — instantiated
twice:

- **hardware profiles** on radios (board presets: pins, TCXO, RF
  switch, PA caps);
- **band profiles** on relays (frequency plans, LoRa parameters —
  e.g. a MeshCore EU narrow preset). Called *band* deliberately, not
  *region*: on a MeshCore mesh a region is a mesh agreement (see the
  Vocabulary entry), not a radio fact.

Profiles themselves are code, shipped with the daemon; the store holds
only what the operator chose. Properties this buys:

- **Switching profiles is non-destructive.** Overrides live under the
  profile they patch; flipping the `profile` knob carries nothing over
  from the previous choice, and several tuned profiles coexist in the
  same store.
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

Stations reuse band profiles for their initial waveform and persist
companion-side changes separately from declarative configuration. Their
TX gate admits only `dry`, `shadow` and `on-air`; the relay-specific
`on-air-zero-hop` rung has no station meaning. A detached station keeps
answering its application, and an attached-but-incompatible station
reports RF `blocked` with the authoritative binding named.

The station implements the current reference companion protocol on its
dedicated TCP stream, with one application connected at a time. The
application's announced protocol level selects the legacy or v3 mailbox
layout at message reception, and the encoded mailbox entry is durable.
Room contacts also persist their signed-message synchronisation cursor;
login and keep-alive carry it exactly as the reference does. Requests to
unknown anonymous peers use the reference's eight reserved, in-memory
slots: they stay out of the advertised contact count, the oldest slot is
reused when all eight are occupied, and a station restart discards them.

`reboot` closes only that station's application session, clears its
per-boot queues, counters, request/connection state and transient contacts,
and does not restart Lotor or disturb another radio consumer. `factory
reset` additionally replaces every companion-owned preference, identity,
contact, channel and mailbox with the declarative station configuration
that seeded it, and persists the replacement before disconnecting. An I/O
failure rolls the reset back and leaves the session alive. A virtual
station has no board battery, storage or target-specific sensor manager:
those reference queries therefore report zero/empty values rather than
borrowing physical data owned by another attachment.

Defaults mirror the MeshCore reference firmware — the forced transmit
after the CAD-fail budget elapses, the 160-entry count-bounded dedup
ring. Advanced knobs exist for
site operators with real needs, and a relay running non-stock values
says so at startup — traceability includes configuration.

## Traceability

- **Zap**, structured logging everywhere, context enriched as frames
  cross layers: `relay=…` or `station=…`, `radio=… corr=…`.
- **Every frame heard gets a correlation ID** carried by every log line
  that concerns it. IDs are generated OpenTelemetry-compatible
  (128-bit trace id) but displayed truncated (8–12 hex chars) and
  greppable by prefix. Relations are structured fields:
  `corr=a3f9c210 duplicate_of=8e01bb42` — grep one, find the other,
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
- **Web UI** — a scaffold today, minimalist by intent: one page, one
  snapshot shape served twice — `GET /api/status` whole, `GET /events`
  as SSE from the bus (state changes wake it, a tick paces it, the
  client degrades to polling and climbs back on its own). The visual
  structure for multi-relay realities will iterate; the data feed will
  not. Read-only over plain HTTP, loopback by default — the same
  privilege-follows-transport rule as telnet — configured by the
  `web:` block exactly as `cli:` configures the telnet listener. All
  assets ship inside the binary; the static bytes never spell the
  product, the snapshot carries the identity. The whole web server is
  a build-time option: light builds omit it and embed no UI
  filesystem at all — a headless binary for hosts where flash and RAM
  are counted.
- **CLI over telnet** for now (SSH considered later), plus an always-on
  local console over a unix socket. Privilege follows the transport:
  the socket is **admin** because the OS's file permissions already
  proved privileged access; network transports **authenticate** — SSH
  when it comes, and today's unauthenticated telnet is read-only.
  Read-only and admin are indistinguishable while no command writes;
  the distinction exists so the first admin command lands on a
  contract, not a retrofit.

MeshCore's over-the-air client table has two deliberately separate
views. **Sessions** are principals that have authenticated traffic in
the current process; a guest exists only there, expires on idle and is
never written to disk. The **ACL** contains durable non-guest roles:
`read-only`, `read-write` and `admin`. A successful admin-password
login creates or promotes its key to a durable admin entry, while a
guest-password or open login creates only an ephemeral guest session.
An operator who wants targeted access without sharing a password
grants the complete public key `read-only`; that key can then perform
the protocol's blank-password recheck. Demoting an ACL principal with
the guest credential removes the durable entry before the guest
session is admitted, so a restart cannot resurrect the old role.
Closing a session from the console removes a guest outright; for an
ACL principal it clears the live route and requires a fresh login while
leaving the durable role in place. Revoking an ACL entry takes back the
role and closes the live session atomically. The ACL drawer exports
replayable `grant` commands with complete keys and explicit roles; how
each role was originally earned remains audit history, not recreated
state.

The protocol engine owns session time as well as session mutation. Its
next idle deadline bounds the active radio receive window, without a
detached timer: when it expires, an ephemeral guest and its credential
are removed, while a durable ACL principal merely becomes inactive and
keeps its authorisation and learned route. Authenticated activity can
make that principal live again; an explicit close remains the stronger
boundary that clears the route and requires a login. After every login,
accepted activity, learned route, close, grant, revoke or expiry, the
engine publishes one immutable generation containing both views and a
credential-free `SessionsChanged` event. CLI reads and completion load
that atomic view directly, so painting client state never interrupts a
radio receive window and never filters an entry as a side effect.

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

The CAD scan is a **declared divergence**: the reference firmware
ships it disabled and only scans when a site turns it on, while this
daemon leaves it on. A Linux host with a healthy SPI bus can afford to
look before it speaks, and a repeater talking over its neighbours
costs the mesh more than it costs itself. `tx.cad: false` restores the
firmware's posture — the driver still refuses to key over a reception
actually in progress, which is a hardware guard rather than a
politeness — and the resolved choice is named in the startup line, so
a site measuring the difference can read which one it ran.

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
correlation it carries. Enforcement is a sliding one-hour window whose
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

**Traceability.** `FrameSent` carries the origin correlation, instant,
airtime and applied power; `TxDropped` carries its reason (queue,
duty, channel when drop is chosen). The journal links them, and `corr`
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
  Lotor consumes them and reimplements nothing at those layers. The
  test is mechanical, not a matter of judgement: a byte offset, a bit
  shift, a struct laid out for transmission or a quantity converted
  into its wire unit is library work, and goes upstream before it is
  used here. A lint gate holds the seam in the protocol package, and
  `AGENTS.md` states it for anyone about to cross it.
- Wire-protocol fidelity to the MeshCore reference is non-negotiable;
  behavioural defaults align with the reference firmware, deviations
  are opt-in and loudly logged.
- Errors recover automatically where possible and are always visible;
  silent degradation is a bug by definition.
