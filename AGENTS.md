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

## What does belong here

Policy, not format. Who may ask, how often, at what priority, under
what duty budget, in what order, with what jitter, and what the journal
records about it. The rule of thumb: code that decides **whether** or
**when** is a daemon concern; code that decides **what the bytes are**
is not.

## The rest

`DESIGN.md` carries the architecture and the ground rules. `task check`
must be green before a commit — fmt, tidy, strict lint, race, vuln.
