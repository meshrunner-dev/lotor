# Site interlock — collaborative TX contention across colocated daemons

Status: **proposal**. Nothing in this document is implemented. It
records a design conversation so the eventual work starts from the
reasoning, not from scratch. Its foundations — the authenticated
envelope, the session, the membership lifecycle — live in
[`peer-protocol.md`](peer-protocol.md), which this feature only rents.
Companion pieces once built: [`radio.md`](../architecture/radio.md)
for the plumbing this hooks into, [`DESIGN.md`](../../DESIGN.md) for
the ground rules it must honour.

## The problem

Two repeaters three metres apart desensitize each other whatever LBT
says. When one keys, the other's receiver is blinded for the whole
frame; when both key — and `tx.lbt_exhausted: transmit`, the default,
guarantees they eventually will — both frames are lost, spent from two
duty ledgers, and heard by nobody nearer than the next hill.

LBT cannot solve this. It is the mesh-facing courtesy: listen, back
off, and after the bounded wait (4 s, `tx.go:33`) apply the exhausted
policy. That policy is a *protocol-layer* decision (`tx.go:948`,
`:1008`) and must remain one — the distant mesh is the engine's
business. What the site needs is a different guarantee on a different
axis: **at most one transmitter keying per shared RF domain, at any
instant, enforced even against a forced TX.**

## The structural idea: a gate below policy

"Stronger than forced TX" cannot be won inside the protocol layer,
because forced TX *is* that layer deciding. It has to be won by
construction: place the gate at the one point every real keying
already passes — `controllerPort.transmit`, where the hand-over to
sibling bindings also lives. The controller accepts an optional
`TxGate` at construction, injected from `cmd/` like everything else it
does not need to understand. It asks "may I key for about this long"
and receives yes, or not-before-T. The protocol may skip LBT; it
cannot skip the controller.

That placement buys three things for free:

- relays **and** stations are covered, since both key through a port;
- `shadow` mode is naturally exempt — it reserves and commits airtime
  but never keys, and the gate guards keying, not accounting;
- CAD probes and noise-floor sampling are exempt for the same reason:
  they are receive-only.

The gate is the instantaneous sibling of the `AirtimeLedger`. The
ledger answers "may this radio spend, over the sliding hour"; the gate
answers "may this site key, right now". Different axes, both kept.

## The lease

One exclusive lease per **RF domain**, bounded by the announced
airtime plus a guard. Never unbounded: a daemon that dies holding it
is healed by expiry, the same philosophy as the `2×airtime+1s`
transmit deadline in `emit`.

```
Acquire(ctx, airtime, priority, corr) → Grant | ErrSiteBusy{Until}
Release(measured)
```

A refusal maps onto machinery that already exists: requeue with
`notBefore = Until`. No new queue semantics, no new waiting position —
the entry takes the same path a busy channel already sends it down.

Pipeline order, per emission:

```
duty reserve → lease → LBT ladder → key → TxDone → release + commit
```

The lease comes *before* LBT deliberately. A colocated peer's TX is
the loudest thing local LBT will ever hear; assessing a channel your
neighbour is about to occupy is wasted work, and assessing one he has
just released is stale the other way. Acquire first, then listen for
the *distant* mesh, which is what LBT is for. The LBT ladder is
already bounded, so lease hold time is bounded with it.

Domains are named, not global. Each radio declares its membership —
`rf_domain: siteA-868` — and grants are scoped per domain. Two
repeaters on one mast but disjoint bands owe each other nothing.

## The peer protocol: a tenant, not an owner

The interlock does not own a wire. It is the first **tenant** of the
shared session described in [`peer-protocol.md`](peer-protocol.md) —
the authenticated envelope, the JSON body discipline, the membership
lifecycle with its four kinds of absence (present / departed / lost /
never-seen), epochs, gossip and the evolution rules all live there,
because a second tenant will want every one of them unchanged.

What is the interlock's own, as a service riding that session:

- **Consent, not election.** For the two-to-four daemons a site
  holds, simplified Ricart–Agrawala: ask every present peer in the
  domain, each answers CLEAR or BUSY-until-T (a relative duration,
  as the session demands), crossed requests resolve by (priority,
  member id). No arbiter to elect, no arbiter to lose.
- **The message set.** REQUEST(domain, corr, airtime_ms, prio) →
  CLEAR | BUSY(busy_for_ms, holder, corr); RELEASE(domain, corr,
  measured_ms). `corr` is the emission's own correlation id — the
  journal joins both sides of a lease.
- **Quiescing order at shutdown.** When the session's LEAVE sequence
  runs, the interlock quiesces first: the gate closes to new
  acquires, any held lease is released, and only then does LEAVE go
  out. A daemon must be incapable of keying before it tells the site
  to stop waiting for it.
- **What the states mean here.** A *departed* peer blocks nobody —
  no timeout consulted. A *lost* peer is the case the
  `hard`/`advisory` decision below governs. The residual case from
  the session document — a two-member site, one clean departure, the
  survivor reinstalled from scratch — costs the interlock one TTL,
  and nothing can shorten it.
- **Cost.** One request per emission on top of the session's 1 Hz
  heartbeat. LAN round-trip (~1 ms) is noise against 100 ms–2 s of
  airtime.

## The product decision this forces: the lost peer

This is the heart of the word "immutable", and it is an operator
decision, not a code one. The mode is declared per domain, and it
governs **lost** peers only — a departed peer announced its absence
and blocks nobody, whatever the mode:

- **`hard`** — a *lost* peer freezes TX (fail-closed). The site
  guarantee survives everything, including the network; mesh service
  does not. A yanked Ethernet cable silences the whole site until
  someone notices.
- **`advisory`** — a *lost* peer is presumed absent and TX proceeds
  (fail-open with hysteresis). The guarantee becomes "except under
  partition".

In `hard` mode, immutability extends to administration: the interlock
attributes are writable from the local console only, never over OTA
and never by a companion. The precedent exists — the console already
refuses `region load`. The only lever is a mutation of the local
store, by someone who can reach the box.

## Configuration: a key beside the driver, a singleton for the session

The configuration model already holds both shapes this needs.

**The RF domain goes on the radio — beside the driver map, never
inside it.** The radio block is `driver:` plus a layered map, and that
structure says something precise: everything in the layered map
belongs to the *driver's* schema, crosses the layers opaque, and is
validated by sx126x. `rf_domain` must not go there — a site domain is
policy, not hardware, and the driver has no business knowing its
antenna shares air. But `driver:` itself is the precedent: a
daemon-owned key beside the opaque map. `rf_domain` joins it:

```yaml
radios:
  g3:
    driver: sx126x-spi
    rf_domain: chalet-868     # daemon-owned, like driver: — not an sx126x attr
    profile: lyra-zerow-station-g3
    overrides: { … }
```

Why the radio and not the relay or station: the **antenna** is what
shares air, not the protocol speaking through it. The enforcement
point is the controller port, per radio; declaring the domain where
the controller lives means relays *and* stations inherit the gate
with zero configuration of their own — their blocks do not change by
a line.

**The session is a singleton block, on the `cli`/`web`/`sentinel`
model**, with the same optionality rule — the block's absence
disables the whole thing:

```yaml
site:
  name: chalet
  member: wanadoo            # default: the system block's name — the
                             # installation already knows what it is called
  listen: :8790
  key: <hex secret>          # masked in the journal, show-secrets under
                             # Admin — the existing secrets discipline
  peers:
    radiocom: 10.64.0.12:8790
  interlock:
    chalet-868: { mode: hard }
    chalet-433: { mode: advisory }
```

Two choices inside. `member` defaults to the `system` block's name —
the daemon already knows what it is called, and the status surfaces
already report it. And the interlock's configuration is a **per-domain
map under `site:`**, not an attribute of the radio: the mode is a fact
of the *site* — one site can hold 868 hard and 433 advisory — while
the radio declares only its physical membership. The session/service
split of the protocol reads straight out of the YAML:
`site.{name,member,listen,key,peers}` is the session,
`site.interlock.*` is its first tenant, and a future service adds its
own key under `site:` without touching the rest.

**Absence rules — and v0 falls out of the shape:**

- Neither `rf_domain` nor `site:` → nothing exists, zero cost. The
  lone repeater's case.
- `rf_domain` without `site:` → the gate exists, purely local. Two
  radios of one daemon sharing a domain serialise **without a single
  network packet** — that is the v0 of this document, falling out of
  the configuration shape instead of being a mode apart. A domain
  with one member always grants, immediately.
- `site:` with peers but no `key` → the load fails. An
  unauthenticated site is not a degraded site; it is an error, and a
  typo must fail the load.
- `interlock:` naming a domain no local radio carries → tolerated:
  this daemon is a session member and a consent arbiter with no
  transmitter in that domain. Useful — a third box without a radio
  can be the witness that spares the two-member residual case.

**Wiring order in the manager**, as always from `cmd/`: the `site:`
block builds the session → the interlock service registers on it →
one gate per domain → `radio.NewController(…)` receives the gate its
radio's domain names. The controller knows only `TxGate`; peers,
session, all of it stays above. Each gate is a table in the
doctrine's sense — its state lives behind the service's goroutine,
orders arrive on an ask channel, not one more mutex per caller.

**Store mechanics.** A singleton kind `site` (empty name, like
`web`), its attrs in the config schema, one shape bump for the table,
and `key` marked secret — the journal mask and rescrub follow the
existing precedent. Console: `set site …` under local Admin. OTA: the
site attrs are **excluded from the OTA-writable surface**, the
`region load` precedent — this is the concrete implementation of the
word "immutable".

**Cross-daemon coherence is not a configuration problem.** Nothing is
replicated — each daemon declares its view, HELLO carries (domains,
modes, service versions), and disagreement is detected *at the join*,
loudly: in hard mode, a peer announcing a different mode on the same
domain receives no CLEAR and the status says so in words. confdb
stays per-daemon, as the design already assumes for everything else.

## Staging

- **v0, no network.** One daemon, several radios: the same `TxGate`
  serialises across its own controllers, and in configuration terms
  it is nothing but `rf_domain` without a `site:` block. The service
  binds to an **in-memory loopback bus** with the same API the
  session will offer — every pipeline mechanism (lease,
  requeue-on-refusal, release-on-TxDone, the typed error) gets built
  and race-tested in a single process where the "protocol" is a
  function call.
- **v1, the LAN.** The same gate and the same service, bound to the
  real session from [`peer-protocol.md`](peer-protocol.md). The seam
  between v0 and v1 is the session API; neither the gate nor anything
  above it moves.

Observability from v0: bus events (lease acquired / refused / peer
lost), a journal drop verdict `site-interlock-denied`, sentinel
counters, and a console `/site status` naming the peers, their
liveness, and who holds what.

## A collateral gain worth naming

Today, two simultaneous forced TX on one site lose **both** frames —
each transmits into the other's deafened receiver. Under the lease,
the silent peer is receiving, and receiving *well*: the interlock does
not merely prevent the collision, it converts it into a reception.
Colocated repeaters stop being the only nodes in the mesh that cannot
hear each other on the air.

## Out of scope, said plainly

- No TDMA, no schedule, no clock synchronisation: the mesh sees
  better behaviour, never a different protocol.
- Nothing in `meshcore-go`: this is not a mesh wire format.
- Hardware interlock (a PTT line daisy-chained between chassis) is
  the one design no daemon bug can defeat, and it is a different
  project. Software gets us collaborative; a wire gets us absolute.

## Open questions, to settle before the first line

- **Short chains.** An ACK owed milliseconds after a reception: does
  it ride a small extension of the current grant, or pay a second
  `Acquire`? Extension is faster and abusable; a second acquire is
  honest and may lose the timing the reference expects.
- **Starvation.** A high-priority chatterbox peer: cap on consecutive
  grants, or ageing on waiting requests? Either works; pick one and
  test it, because "fair enough" without a mechanism is how the quiet
  peer's beacons die.
- **Grant scope vs. emission trains.** Multipart forwards enqueue
  several emissions; per-emission acquire is simplest and probably
  right, but the interleaving under contention should be simulated
  before it is promised.
- **The `hard`-mode alarm.** Fail-closed without a loud signal is an
  outage that looks like silence. Which surface screams — status,
  MQTT, the advert itself? — needs an answer the day `hard` ships.
