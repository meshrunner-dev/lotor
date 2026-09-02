# Peer protocol — a shared session between colocated daemons

Status: **proposal**. Nothing here is implemented. It records the
foundations that [`site-interlock.md`](site-interlock.md) rests on but
does not own: the interlock is the first *tenant* of this session, not
the reason it exists. A second tenant — site dedup, advert
desynchronisation, a whole-site status view — should cost a handler
and a message type, not a new protocol. The ground rules it must
honour are in [`DESIGN.md`](../../DESIGN.md).

This is **not** MeshCore. Nothing on the air changes, no byte of this
touches `meshcore-go`. It is a lotor-to-lotor protocol on the site
LAN, owned by a new `internal/site` package (the session) with tenants
under it (`internal/site/interlock` first).

## The one idea: carve the graven from the mouldable

A protocol outlives its versions when the part everyone must
understand forever is tiny and stupid, and everything else is free to
move. Three layers, and the third is what makes future uses cheap.

## Layer 0 — the envelope, set in stone

A UDP datagram, fixed binary layout, constant offsets:

```
magic(4) │ ver(1) │ hmac(32) │ body…
```

The HMAC-SHA256 covers `magic│ver│body`, keyed by the site key with a
domain-separation tag (`"lotor-site-v1"`). The receiver verifies the
MAC **before** parsing anything: no unauthenticated byte ever reaches
a decoder, so the parser's attack surface for anyone without the key
is zero.

`ver` here does **not** version the protocol. It versions the
*cryptographic construction* — it bumps only if the MAC or the key
change nature, a key-rotation-grade event handled as a config flag-day.
All semantic versioning lives above. That is what lets the envelope
never move: a lotor of 2030 can always read the envelope of a lotor of
2026, even if it discards the body.

## Layer 0.5 — the body: JSON, and the MAC over the bytes as sent

JSON for the body, and by choice, not laziness:

- **additive evolution is free** — an unknown field is ignored, the
  default behaviour of every decoder;
- **precedent** — the MQTT observer and the web UI already speak JSON;
  no new dependency, no codegen;
- **greppable** — `tcpdump -A` on the site port reads by eye during
  an incident, and this protocol will be debugged at 2 a.m. on a mast;
- **size is a non-issue** — tens of bytes, 1 Hz heartbeat, one request
  per emission. CBOR would save bytes nobody counts; protobuf would
  add a compiler for nothing.

JSON's usual trap — no canonical form — does not apply: the MAC covers
**the bytes as emitted**, never a re-serialisation. Nothing is
canonicalised; what is sent is what is signed.

Every body carries the same logical header:

```json
{"type":"request", "site":"chalet", "member":"wanadoo",
 "epoch":17, "seq":4211, "svc":"interlock", "v":1,
 "body":{"corr":"ac0bc680", "domain":"chalet-868", "airtime_ms":1200, "prio":2}}
```

- `(epoch, seq)` is the replay defence: epoch monotonic per boot
  (persisted in the store), seq monotonic per life, watermarked per
  sender at each receiver — only what grows is accepted.
- Every duration is **relative** (`airtime_ms`, `busy_for_ms`); no
  wall clock ever crosses the wire. Two SBCs with drifting RTCs must
  not be able to disagree about who holds a lease.
- `corr` reuses `internal/correlation` — the id that follows an
  emission from the bus to the driver follows its lease across the
  peers, and the journal can join both sides.

## Layers 1 and 2 — the session, then tenant services

This is where "something other than the interlock" is won. The session
— JOIN / LEAVE / HEARTBEAT, the peer table, gossip, epochs — has
nothing interlock-specific in it. It is the floor every future use
wants unchanged. So it is split:

- **The session** owns identity, liveness and security. It exposes a
  small API — `Broadcast(svc, msg)`, `Send(svc, member, msg)`,
  `OnMessage(svc, handler)`, and peer-state callbacks.
- **Services** are named tenants above it. `interlock` is the first.
  Each HELLO/JOIN advertises `services: {"interlock":[1,1]}` — the
  version range spoken — and the site converges *per service* to the
  common minimum. A member below the minimum a local config requires
  refuses assembly, at load: the precedent is a profile name that
  fails the load rather than silently configuring nothing.

The pattern is the one the repository already likes: a **registry**,
as with `station.Register` and `radio.Register`. It also makes the
staging cleaner than the interlock document alone implies — the v0
"no network" mode is the interlock service bound to an **in-memory
loopback bus**: same API, same state machine, race-tested without a
single packet.

## Membership lifecycle: absence has kinds

This lives in the session, because every tenant needs it and none of
it is about the interlock. A timeout is the answer for a peer that
vanished without a word. Most absences are not that: an update
restarts the daemon, an operator stops it, a box reboots on schedule.
Making every one cost the full TTL punishes the common case for the
rare one, so absence gets states, and departures get announced.

A configured member is in one of four states:

- **present** — heartbeats arriving; its consent is required;
- **departed** — it said goodbye; excluded from the quorum
  immediately, no timeout consulted;
- **lost** — heartbeats stopped without a goodbye; the TTL ran out;
  this is the state a tenant's fail-closed/fail-open policy governs;
- **never-seen** — configured but silent since our own start; treated
  as *lost* once the join window closes, because a box off for
  maintenance and a box that crashed look identical from here.

**LEAVE.** The clean shutdown already runs an ordered, bounded
sequence (`shutdown()`, 15 s ceiling); the session joins it in its
proper place: tenants quiesce first (the interlock closes its gate and
releases any held lease), then LEAVE goes out — best-effort UDP,
repeated a few times, unacknowledged — and only then does the daemon
exit. Ordering matters: a daemon must be incapable of keying before it
tells the site to stop waiting for it. The update dance is this path
twice — `systemctl restart` delivers SIGTERM, LEAVE goes out, the new
binary JOINs seconds later; peers see departed → present and the TTL
is never consulted. A crash mid-update is precisely what the TTL
remains for.

**JOIN.** A starting daemon announces itself and holds a join window —
two heartbeat periods — to learn the room before any tenant acts.
Bounded, cheap, paid once per start.

**Epochs.** LEAVE and JOIN carry the member's epoch, monotonic per
boot (the store holds the counter). A replayed LEAVE from a past life
cannot kill a live member: a JOIN with a newer epoch supersedes
anything older, stale messages are dropped on comparison. This is also
why LEAVE must be signed like everything else — a forged departure is
not a denial of service but a defeat of the tenant that trusts it: for
the interlock, it tells the site to stop waiting for a peer that is
still transmitting.

**Gossip carries the news to the absent.** Heartbeats include each
member's last known state and epoch, so a restarting daemon learns "A
departed" from any third box instead of waiting out A's TTL itself.
Each daemon also persists the peer table in its state directory, so
its own restart does not erase its own memory.

The residual case, stated plainly: a **two-member site** where A
departs cleanly and B is then reinstalled from scratch. Nobody is left
to tell B, and B's own memory is gone — B pays the TTL once. Absence
without a note and without a witness costs the timeout; everything
else short-circuits it.

## The constitution — five rules to write into the code

1. **The envelope never changes meaning.** Its version byte bumps only
   for the cryptographic construction.
2. **Fields are added, never reassigned.** Unknown is ignored. An
   unknown `type` within a known service is ignored *and counted* —
   the observability of "I did not understand" is what makes mixed
   deployments diagnosable.
3. **A change of meaning is a service version**, negotiated to the
   common minimum. No field ever changes meaning by who is listening.
4. **Every message is idempotent or carries an id.** Retransmitting is
   always safe; all state is latest-wins by `(epoch, seq)`.
5. **Goldens pin every encoding.** The meshcore-go lesson on our own
   wire: encode *and* decode round-tripped, even for messages a daemon
   only ever emits.

Rule 3 earns emphasis in a **consent** protocol. "Ignore what you do
not understand" is not always enough: if interlock v2 changes what
CLEAR promises, a v1 answering CLEAR the old way breaks the guarantee.
Hence per-service negotiation — semantics extend only between peers
that explicitly share them, and an update's mixed-version window
speaks v1 end to end.

## Plausible tenants, to test the split

- **Site dedup hint** — "I just relayed hash H", so a peer drops its
  own re-flood without waiting to hear it on the air. Complements the
  interlock: fewer leases asked for at all. The verdict stays with the
  engine; this is a hint toward `seen`, not an order.
- **Advert desync** — site-aware spacing rather than local jitter
  alone.
- **Site status** — any member's web UI showing the whole site; the
  session already carries liveness, this only exposes it.

A deliberate non-tenant: **config convergence.** confdb is per-daemon
by design; gossiping configuration is the kind of convenience that
becomes an incident.

## Why not memberlist, raft, protobuf

- **memberlist** does gossip membership well, but imports tens of
  thousands of lines and its own wire to replace ~300 lines of ours on
  2–4 nodes.
- **raft** solves replicated consensus, a problem we do not have — a
  TTL lease needs no log.
- **protobuf** buys compactness and pays in codegen, on messages where
  compactness is worth nothing and legibility much.

## Open questions

- **HELLO amplification.** An unauthenticated first packet must not let
  a stranger make us reply to a spoofed source; the MAC gate covers
  content, but the rate of replies to unknown members still needs a
  bound.
- **Key rotation.** Two keys accepted during a window, one emitted,
  is the standard shape — but which surface drives the window, and
  does it ride the envelope's `ver` or a body field?
- **Multi-homing.** A daemon on two site LANs (a maintenance VLAN and
  the operational one) — one session per interface, or one session
  bound to a chosen address? The peer table assumes one identity.
