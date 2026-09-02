# Applications — mesh node roles beyond relaying and companions

Status: **in progress**. Stages 0 and 1 of the ladder below landed on
2026-09-02 — the library's server-side room codecs, and the seam with
a room that holds its identity and follows a radio in dry mode; the
rest is still proposal. It records the reasoning for a third role in
the daemon, the MeshCore room server being its first instance, and the
persistence question that role forces. Ground rules it must honour: [`DESIGN.md`](../../DESIGN.md);
plumbing it hooks into: [`radio.md`](../architecture/radio.md).

## The problem

A MeshCore room server is a mesh node that is neither of the two roles
this daemon knows. It does not forward — the reference ships it with
forwarding disabled — so it is not a relay. It has no local user and
no companion application on a TCP port — so it is not a station. What
it does is *serve the mesh*: clients log in over the air, post text,
and receive what others posted, under an access list the node's admin
governs. Sensor servers, bots, gateways that answer questions from the
mesh are the same shape: an identity on a radio, a protocol served to
peers, no local human.

Bending a station into that shape would cost the station its
definition — "exactly one application connection on a dedicated TCP
port" is what a station *is* — and bending a relay would put
origination policy into the one component whose job is not to have
any. Hence a role of its own, named for what it does and not for the
protocol it speaks: an **application**.

## Definition and invariants

An application is a mesh identity hosted by the daemon that **serves
peers over the air**. It originates and receives; it never forwards.
It has no local client socket of its own — its users are on the mesh,
its operator is on the console. It exists while detached from RF,
like a station, and may move between radios without losing its state.

What follows from that, and must stay true:

- an application binds to at most one radio, as a **non-authority
  consumer**: the relay's waveform wins when a relay is present, and
  on a radio without a relay the oldest attached non-relay binding —
  station or application alike — is the stable authority. The
  controller treats stations and applications as one class: round-
  robin fairness, a bounded lossy inbox, strict relay priority above
  them, one shared airtime ledger, `shadow` reserving what it would
  have spent. [`radio.md`](../architecture/radio.md) gains the word
  and no new rule;
- an application never earns a forwarding rung. Its transmit ladder
  is the station's: `dry`, `shadow`, `on-air`;
- an application's *behaviour* on the wire mirrors the reference
  program it stands in for — a room server presents itself as
  `ADV_TYPE_ROOM`, answers the 13-byte login reply the reference
  answers, pushes posts at the reference's cadence. Divergences are
  declared, opt-in where they change what peers see, and logged;
- an application is administered two ways, like a relay: locally on
  the console with the console's grammar, and over the air by an
  authenticated admin with the ecosystem's own CLI grammar, because
  that is what companion apps speak to a room server today;
- nothing an application stores may make the daemon depend on the
  sentinel existing. Observation stays optional.

## The seam: `internal/application`

The station package is the template, and deliberately so: the two
roles share the "hosted identity with optional RF" lifecycle exactly,
and differ in who they talk to. `internal/application` mirrors
`internal/station` shape for shape:

```go
// Registered per type, each type naming its protocol: "meshcore-room" first.
type Builder struct {
    Build   func(Spec) (Service, error)
    Check   func(map[string]any) error
    Asks    func(map[string]any) (RadioDemand, error)
    Presets map[string]map[string]any   // band presets, as stations
    Schema  []schema.Attr               // the type's contributed attrs
    Migrations []confdb.Migration       // the type's tables in the store — see Persistence
}

type Service interface {
    Run(ctx context.Context) error
    Info() Info
}
// RadioAttacher and RadioRequester as in station: the manager supplies
// or withdraws RF without stopping the service.
```

`Spec` carries what a station's does minus `Listen`, plus the door a
station does not have: the **store** — membership, cursors and history
all live in the configuration store, for the reasons the Persistence
section gives. `Info` reports lifecycle, RF state, the
identity, and a small `Summary map[string]string` the type fills for
its own status line (members, posts, pending pushes), so the console
and the web snapshot need no per-type knowledge to show it.

**Configuration.** A new kind, `applications:`, with instances by
name. Two structural attributes select the implementation:
`protocol` (the mesh it speaks) and `type` (what it does on it), so
config never conflates the two, as `DESIGN.md` insists. The type is the
schema's one choice, and each type declares the protocol it speaks —
the registry holds it to that word, so `protocol: lorawan` with a
MeshCore room is a configuration error, not a near miss. Type names
carry their protocol explicitly, `meshcore-room`, so the one word an
operator types can never be mistaken for another mesh's room when a
second protocol arrives.

```yaml
applications:
  lobby:
    protocol: meshcore
    type: meshcore-room
    radio: slot1
    profile: eu-868-narrow          # the band preset, as a station
    tx: { mode: shadow }
    identity: new                   # or a hex key; secret, as everywhere
    node_name: "Chalet lobby"
    admin_password: ...             # secret
    guest_password: ...             # secret; empty = no guest door
    allow_read_only: false
    advert_flood_interval: 47h
    advert_local_interval: 2m
    history: 32                     # posts kept; the reference's ring
    persist_history: true           # false = RAM only, reference parity
```

**Wiring in the manager** follows `startStation` / `attachStationRadio`
/ `rebindStation` line for line, with `radio.RoleApplication` on the
binding and `bus.SourceApplication` on everything it emits. A
`radio=` mutation rebinds without stopping the service; a
configuration error is a visible stillborn, never a retry loop.

**Console.** A `/application` context on the `/station` model:
`add`, `set`, `print` with provenance, `export`. Per instance,
`status`, and a `members` drawer that *is* the relay's ACL drawer —
same `grant`/`revoke`/`close`, same `SessionsChanged` generation view,
same `export` of replayable grants — because it is the same table
(below). Type-specific verbs are contributed by the type: a room adds
`posts [n]` and `post <text>` (the reference's `room.post`, from the
local console).

## Composition for MeshCore: two kernels, not a third copy

The reference itself is the argument. Its repeater and its room server
are two thin programs over one shared base: `ClientACL` for sessions
and permissions, `CommonCLI` for the admin grammar, the same 13-byte
login reply, the same reply-routing helpers, the same scoped-flood
selection. Lotor today holds that base once — inside the relay engine,
in `acl.go`, `session.go`, `anon.go`, `reply.go`, `command.go` — and a
second, independent origination pipeline inside the MeshCore station:
`queue.go`, the duty reservation, LBT and shadow logic of `rf.go`.

A room server needs both halves. Writing them a third time is the
mistake this document exists to prevent. The proposal is two
extractions, each landing with its current owner's tests green before
the room touches it:

1. **`internal/meshcorehost` — the server-side kernel**, lifted from
   the relay engine: the client table and its roles, login with its
   replay and skew guards, the anonymous-request limiter, the
   reply-routing decision (direct on a taught path, flood reply, or a
   PATH return carrying the answer), and the over-the-air admin
   grammar the reference calls `CommonCLI` — `set`/`get`/`password`/
   `advert`/`clock`/`region`. It keeps the engine's discipline: state
   owned by the caller's goroutine, no mutex, orders arriving on the
   caller's ask channel. The relay engine becomes its first consumer;
   the room its second. The name follows `internal/meshcorecfg`, the
   precedent for MeshCore code shared across roles.

   One thing this extraction surfaces: the permission role bits
   (`permRoleMask`, `permAdmin`, `permGuest`) live in the relay engine
   today. They are wire units — a room server *must* mask them the
   same way — and `AGENTS.md` says wire units belong in the library.
   They move to `meshcore-go` as part of the library work below, and
   the kernel imports them.

2. **`internal/origin` — the origination pipeline**, lifted from the
   station: the `(priority, not-before)` emission queue, atomic duty
   reservation before LBT and commit after, the LBT ladder with its
   bounded retries, the dry/shadow/on-air gate, `FrameSent` and
   `TxDropped` with their source. It is protocol-neutral by
   construction — an emission is bytes, a priority and an instant —
   and the station becomes its first consumer. (The relay keeps its
   own `tx.go`: its queue is the reference repeater's, with forwarding
   semantics origination has no business in. Unifying the two is a
   later question, not this one.)

The room server is then what is left: the post store, the push
scheduler, keep-alive handling, and the room's own admin verbs — a few
hundred lines that are *about rooms*, sitting on kernels that are not.

### Library work first

Per the seam rule, the wire code a room server needs and the library
lacks goes to `meshcore-go` before a line lands here, encode **and**
decode, pinned against the reference:

- the room login: `ts ‖ sync_since ‖ password` — the library builds it
  (`BuildRoomLoginReq`) and cannot parse it; `AnonPassword` reads the
  cursor's four bytes as the start of the password. A `ParseRoomLogin`
  and a room-aware discriminator (the typed-question test on byte 4
  misfires on a cursor byte below `0x20`);
- the outbound post: `TXT_TYPE_SIGNED_PLAIN` with the four-byte author
  prefix — the one subtype the text encoder cannot emit today;
- the keep-alive request (`forceSince`) and its answer, an ACK
  carrying the unsynced count as a fifth byte;
- the permission constants and role mask;
- the room's `ServerStats` (the repeater's 52-byte prefix plus two
  counters), and the access-list request/reply;
- reply-along-path composition: a RESPONSE carried as a PATH return's
  extra, with the path reversed — every server-side role composes it
  by hand today;
- `RoomFilter()` beside `RepeaterFilter()`, and a `ParseMultiAck`.

## The room server, as the first tenant

Behaviour follows the reference — `examples/simple_room_server` — and
is summarised here so the divergences below have something to diverge
from. Clients log in with an ANON_REQ sealed to the room's key carrying
their clock, a **sync cursor** (the room-clock timestamp of the last
post they hold) and a password; the admin word earns `admin`, the
guest word `read-write`, and `allow_read_only` admits an unknown word
as `guest`, which may read but never post. A post is a plain TXT_MSG;
the room stamps it with its own unique clock, stores it, and ACKs.
Delivery is a **push scheduler**: every 1.2 s one client, round-robin,
receives the oldest post newer than its cursor that it did not author
and that is at least 6 s old; the client's ACK advances its cursor;
three unanswered pushes stall that client until it speaks again. A
keep-alive REQ, direct only, may force the cursor and is answered with
the unsynced count. Admins administer over the air with `TXT_TYPE_CLI_
DATA` text: the shared grammar plus `setperm` and `room.post`.

**What the reference persists** is small: the identity, the
preferences, and the access list — *admin entries only*, lazily, five
seconds after a change. **Posts are RAM: a reboot empties the room.**
Non-admin members are forgotten too. That is the honest capability of
a microcontroller, and the place where a Linux daemon has something to
add — which is what makes persistence the real design question here.

**Declared divergences**, each deliberate, none visible in reply
bytes:

- posts survive a restart and the ring may be larger than 32
  (`history`, `persist_history`); the reference's behaviour is one
  setting away;
- every durable role persists — `read-only`, `read-write`, `admin` —
  not admins alone; guests stay in RAM, exactly as the relay's ACL
  already does. A member who logged in yesterday is still a member;
- anonymous requests and keep-alives are rate-limited (the repeater's
  `anon_limiter(4, 180)` shape; the reference room server carries a
  TODO where its limiter should be);
- a post an admin could not persist is **not acknowledged** — "a post
  acknowledged is a post kept" — and the refusal is counted and
  logged, where the reference stores in RAM and acknowledges;
- the reference's known sharp edges are fixed rather than reproduced
  when they are not wire-visible: the round-robin index read past a
  shrunken table, the late ACK that matches nothing, the blank-
  password short path that leaves the cursor untouched.

Client text longer than the stored maximum is one to decide: the
reference truncates silently at 151 characters while allowing clients
160. Refuse loudly, or truncate as the reference does? The proposal
leans to refusing — a post that is not what its author wrote should
not be acknowledged as if it were — but this is wire-adjacent
behaviour and is listed under open questions.

## Persistence

An application accumulates four kinds of state, and sorting them first
is what makes the store question answerable.

| State | Nature | Loss means |
|---|---|---|
| identity, preferences | small, rarely written, **is the node** | a different node |
| membership (ACL roles) | small, security-relevant, written on login/grant | members must re-login; admins re-grant |
| delivery cursors | one integer per member, advanced on every ACK | re-delivery of a few posts (clients resend their cursor at login and keep-alive) |
| history (posts) | grows with the room, bounded by `history` | an empty room — the reference's every reboot |

Three homes were weighed.

**The configuration store** — one file remains the whole node, history
included; one set of migrations, one backup command; the membership
table is the relay's ACL, already there. Against it stands the store's
own contract — *small, durable, the thing an operator saves; the
journal is a different database because it churns* — written for a
packet archive that grows by the thousand an hour.

**The sentinel journal** — prunes, has a RAM mode, the web UI browses
it. But the sentinel is optional by the design's strongest rule —
"nothing else may depend on a sentinel existing" — and a room's history
is operational state, not an observation. The right relation is the
reverse: the application publishes typed events, and a sentinel, if
present, archives them like everything else.

**A third store** — `applications.db`, WAL, its own pruning, a RAM mode
— the journal's contract for data of the journal's nature. The right
answer for data that churns; a third file, a third schema and a
two-sentence backup story for data that does not.

### Decision (2026-09-02): the configuration store

The deciding fact is the traffic, not the taxonomy. A room server
posts a handful of messages a day; its cursors advance a few times an
hour. The eMMC argument was sized for a packet journal, and a chat ring
of a few dozen entries is not that — its write traffic is, in the
operator's words, virtually nil. The store's contract bends less than
it looks: the file stays small (bounded by `history`), it stays the
one thing an operator saves, and a backup now restores a room **with
what was said in it** — which is what a room is for.

What it costs, stated so nobody rediscovers it:

- **Data tables beside the revision trail.** Posts and cursors are
  data, not configuration mutations; they land in their own tables
  (`room_posts`, `room_cursors`, keyed by application name) and are
  not revisioned. "Every mutation is recorded" keeps its meaning for
  configuration objects, and this is the one stated exception.
- **Migrations through the store's own registry.** Each application
  type contributes its tables through `confdb.Migration`, the shape
  bump discipline the store already enforces — no `Builder.Data`, no
  second migration system.
- **Cursors are debounced.** The reference's five-second lazy write is
  the right instinct: a cursor lost to a crash costs a re-delivery the
  client's own cursor repairs at its next keep-alive, and the store is
  not asked to fsync on every ACK.
- **`persist_history: false` stays.** It is the reference's behaviour
  — RAM ring, nothing written — and the escape for a host where even
  this traffic is unwelcome. Membership and identity persist
  regardless; they are configuration.

The split by class therefore lands entirely in `config.db`:

- **Identity and preferences** → attributes of the `application`
  object, secrets masked, mutations revisioned. Over-the-air
  `set`/`password` from an admin writes there, principal = the
  admin's key, as the relay's OTA CLI does.
- **Membership** → the existing `acl` table, whose `relay` column
  becomes an owner key (`relay:<name>`, `application:<name>`) — one
  shape bump with its migration. The relay's semantics carry over
  unchanged: durable roles persist with their replay guard and taught
  route; guests never touch disk.
- **Cursors and history** → `room_cursors` and `room_posts`, the
  in-memory ring the runtime authority and the tables its durability,
  loaded at start, posts written before they are acknowledged when
  `persist_history` is on.

The backup story is back to one sentence: *`config.db` is the node,
and now the room too.*

## Observability

- Logs: `application=<name>` beside `radio=` and `corr=`, the frame
  lineage unchanged. A post's correlation follows it from reception
  through storage to every push and ACK — the mailbox precedent.
- Bus: `FrameSent`/`TxDropped` with `SourceKind = application`;
  `ApplicationState` on lifecycle and RF changes; typed events per
  type — `RoomPost{App, Author, At, Corr}`, `RoomMember{App, Key,
  Role, Change}` — that a sentinel archives and the web snapshot
  shows, and that an MQTT observer may one day publish.
- Console `status`: lifecycle, RF state and its cause, the identity,
  and the type's summary — members, posts held, pushes pending,
  members stalled.
- Counters: logins by outcome, posts accepted/refused (by reason),
  pushes sent/acked/timed out, anonymous requests limited.

## Staging

0. **Library.** The codecs above land in `meshcore-go` with
   reference-pinned round-trip tests; tag, bump.
1. **The seam.** `internal/application`, the `applications:` kind,
   manager wiring, `RoleApplication`, `SourceApplication`, the console
   context. The room is the proving tenant from day one, in `dry`
   mode, RAM ring only — it logs clients in and pushes nothing on the
   air, which is already a whole protocol to test.
2. **`internal/meshcorehost`.** Extracted from the relay engine with
   the engine's tests green, then adopted by the room: login, roles,
   reply routing, admin grammar.
3. **`internal/origin`.** Extracted from the station with its tests
   green, then adopted by the room: the room pushes in `shadow`.
4. **Persistence.** `room_posts`, `room_cursors`, the ACL owner-key
   migration, `persist_history`.
5. **Observation.** Bus events, sentinel tables, the web snapshot.

**Validation** has a gift the architecture already made: a station and
a room on one controller *hear each other without RF* through the
hand-over of composed emissions. The whole login → post → push → ACK
cycle runs on the hardware-free bench, race-tested, before a chip is
keyed — and then on air against a stock companion app, whose behaviour
is the conformance oracle.

## Out of scope, said plainly

- Nothing in this document changes a byte on the air: room-server
  behaviour is the reference's, and the wire is `meshcore-go`'s.
- Observers (MQTT) are not applications: they consume the bus and own
  no mesh identity. The line is "does it have a key and answer peers".
- A room server that also relays is two objects on one radio — a relay
  and an application — never one object with both authorities.

## Open questions, to settle before the first line

- **`type` and `protocol`**: composite choice in the schema, or type
  declares protocol? Same information; the answer decides one helper.
- **Over-long posts**: refuse (proposed) or truncate at 151 as the
  reference does?
- **Stalled members**: the reference stops pushing after three
  failures until the client speaks. Keep, or add an eviction after an
  idle period the reference never implemented?
- **History retention**: by count alone (`history: N`, the ring), or
  by age as well? Count is the reference's shape and the simplest to
  reason about under pruning.
- **Names**: `origin` for the origination pipeline is a placeholder
  until a better word appears.
