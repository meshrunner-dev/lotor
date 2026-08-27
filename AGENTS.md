# Working on Lotor

Read this before writing code here. It exists to stop one specific
mistake, which is easy to make and expensive to undo.

## The wire is not ours to write

Lotor speaks MeshCore through `meshrunner.dev/pkg/meshcore`. That
module *is* the wire protocol; this repository is a daemon that uses
it. The seam is not a preference — the library is the published,
reference-pinned, fuzzed artifact, and any format that exists on both
sides of the seam will drift apart.

**Before you write a byte layout, stop.** If you are about to

- index into a payload (`payload[4]`, `plain[5:]`, `args[1]`),
- reach for `encoding/binary` outside a test,
- shift or mask a flag byte,
- lay out a struct field by field for transmission,
- convert a quantity into its wire unit (an SNR into quarter dB, a
  duration into seconds-since, a key into a path hash),
- or write a parser for something the library already parses,

then you are writing library code in the wrong repository. The lint
gate in `internal/protocol/meshcore` will refuse it, and that refusal
is the point.

**Do this instead, in order:**

1. **Look in the library first.** `requests.go`, `payloads.go`,
   `envelope.go`, `admin.go`, `control.go`, `packet.go`,
   `transport.go`, `advert.go`, `text.go`, `cayenne.go`. Most of what a
   repeater needs is already there, under a name you would not have
   guessed. Read the package before concluding it is missing.
2. **If it truly is missing, add it there.** A codec, a parser, a
   packet transform — it belongs beside the formats it is related to,
   with a test that pins the layout against the reference firmware.
   Encode *and* decode, even when this daemon only needs one direction:
   a format with one side implemented is a format nobody can test
   round-trip.
3. **Then tag the library, bump `go.mod` here, and consume it.**

Step 2 is not a detour around the task. A format written here costs a
second implementation, a second set of tests, and a refactor later;
written there it costs one function and one test.

## The hardware stops at the driver

The same seam exists one layer down, and it is currently intact: one
file in this repository touches chip, GPIO or ioctl code, and one names
a concrete driver. Keep it that way.

Above `internal/radio`, a radio is the `Device` interface and this
daemon's own types — `Waveform`, `Envelope`, `Frame`, `TxReport`,
`NoiseFloor`. Driver configuration crosses as an opaque
`map[string]any` on purpose: the layers in between carry it without
reading it. Lint denies `meshrunner.dev/pkg/lora`, GPIO and low-level
OS packages everywhere but `internal/radio/sx126x`, and denies naming
that driver package anywhere but the wiring in `cmd/`.

**What the lint cannot see, and you must.** It reads imports, so it
catches a chip type escaping upward. It does not catch the same leak
expressed as a *shape*: `Device` growing a `SetPin`, `Waveform`
acquiring an `SPISpeedHz`, `Envelope` learning what a chip select is.
Nothing forbidden is imported and the seam is gone anyway. If a method
or a field only makes sense for one chip on one bus, it belongs behind
the driver, whatever the linter says.

## Names off the air

A node name is attacker-chosen text arriving on a public band. Anything
that displays one goes through `meshName` in `internal/cli/table.go`,
which neutralises control and formatting runes — escape sequences, bidi
overrides — quotes the result so its bounds are unmistakable, and shows
a placeholder when there is no name at all. Format one yourself and you
have written the view that eventually forgets.

## Orders from outside, and why the mutex count stays flat

The console, a web UI and an API are three callers of the same
methods, not three kinds of state. Reaching for a new mutex each time
one arrives is the mistake to avoid; `ask.go` describes the shape that
replaces it.

Almost everything the protocol engine knows is owned by one goroutine
and needs no lock, because nothing else writes it. What arrives from
outside is an **order**, not a write: it goes on an ask channel, the
pipeline serves it on its own turn, and an `ack` carries back either
"taken" or the reason it was not. Policy that guards an order — a rate
limit, a one-at-a-time rule — belongs on the pipeline side of that
channel, next to the state it consults. Put it on the caller's side
and it needs a lock, and every later caller needs to remember it.

A lock is right for state that is genuinely shared and read
concurrently: the neighbourhood, the counters, the duty ledger. There
is one per table, and that count is a function of how many tables
there are, never of how many callers.

## What does belong here

Policy, not format; intent, not mechanism. Who may ask, how often, at
what priority, under what duty budget, in what order, with what jitter,
and what the journal records about it. The rule of thumb: code that
decides **whether** or **when** is a daemon concern; code that decides
**what the bytes are**, or **which register carries them**, is not.

## The rest

`DESIGN.md` carries the architecture and the ground rules. `task check`
must be green before a commit — fmt, tidy, strict lint, race, vuln.
