# Software updates — mechanisms and guarantees

Lotor's update system is a pull-based chain from a source commit to a running
binary. It is designed to answer four questions independently:

1. **Who authorized this release?**
2. **Are these exactly the bytes that were authorized?**
3. **Can an unprivileged daemon prepare an update without gaining installation
   privileges?**
4. **Can the machine recover when the replacement no longer keeps the service
   alive?**

This document explains those mechanisms and the guarantees they provide. It is
not an update-service deployment guide.

## The complete flow

```text
source commit
    │
    ├── deterministic build + embedded VCS provenance
    │
    ├── immutable per-platform artifacts
    │       └── optional deterministic gzip
    │
    └── strict manifest
            ├── product, channel, version, publication time
            ├── artifact URL, size and transport SHA-256
            ├── unpacked binary size and SHA-256
            └── Minisign-compatible signature
                         │
                         ▼
unprivileged Lotor daemon
    ├── verifies signature, product, channel pin and platform
    ├── decides whether the offered version is newer
    ├── downloads and verifies the transport bytes
    ├── unpacks within the signed size bound
    ├── verifies the resulting binary
    ├── executes its self-check against a copy of the configuration
    └── writes a complete-stage marker last
                         │
                         ▼
privileged installer
    ├── re-verifies the stage from its own trust store
    ├── preserves the previous binary
    ├── atomically replaces the executable
    ├── marks the new version as being on probation
    └── restarts the unprivileged service
                         │
              ┌──────────┴──────────┐
              ▼                     ▼
       survives 90 seconds     enters a crash loop
              │                     │
         commit update          restore previous binary
```

Upstream discovery and staging happen only when an administrator asks Lotor to
check or install. There is no background polling or automatic download policy.
The system watcher automates only the privileged half after a complete stage
exists.

## Channels and release trains

A channel is a mutable pointer expressed as a signed manifest. The artifacts it
points to are immutable and version-addressed.

| Channel | Source | Ordering rule | Trust train |
|---|---|---|---|
| `release` | stable version tag | semantic version | stable |
| `rc` | release-candidate tags and stable releases | semantic version | stable |
| `beta` | beta tags and stable releases | semantic version | stable |
| `dev` | every commit accepted on the main branch | signed publication time, with the persistence limit below | fast |
| `try-<slug>` | an explicitly selected branch or pull request | signed publication time, with the persistence limit below | fast |

A stable release also refreshes `rc` and `beta`. The semantic-version guard
prevents that refresh from stepping a client down when a prerelease channel is
legitimately ahead. An administrator may explicitly force an equal or older
version when intentionally changing course.

The stable and fast trains use different signing keys. Each public key carries
a locally trusted channel scope: the stable key may vouch for `release`, `rc`
and `beta`; the fast key may vouch for `dev` and `try-*`. A manifest signed
by the fast key but presented as `release` is refused, both during the initial
check and during privileged installation.

The trains also have separate publication credentials and artifact storage.
This is an availability boundary in addition to the cryptographic one: routine
fast-channel automation is not given the authority to replace or delete stable
artifacts.

Fast artifacts are retained for a bounded period, except for the version named
by a live manifest. Stable artifacts are retained as release history.

## The signed statement

One manifest describes one product, one channel and one version. It includes a
signed publication time and a map of artifacts keyed by platform, such as
`linux/arm64`.

For every artifact it records:

- the download URL;
- the exact byte size and SHA-256 of what is downloaded;
- the compression method, currently either none or gzip;
- when compressed, the exact byte size and SHA-256 of the unpacked binary.

The parser is deliberately strict. Missing required fields, unknown JSON
fields, unsupported compression, malformed hashes, empty artifact sets and
unsupported URL schemes are errors. The network fetch is bounded to one MiB
before parsing. Strictness makes evolution explicit: an older binary refuses a
statement it does not understand instead of silently interpreting a new
contract with old assumptions.

The manifest also binds two names that signatures alone cannot:

- `product` must equal Lotor's product slug, preventing a valid signature for
  another product from becoming an update;
- `channel` must equal the channel requested by the client.

Finally, the selected signing key must locally vouch for that channel.

## Trust model

Manifests use Minisign-compatible Ed25519 signatures, prehashed with
BLAKE2b-512. Verification also checks Minisign's global signature, which binds
the trusted comment to the content signature.

The verifier accepts a set of public keys:

- official keys embedded in the running binary;
- additional public keys placed in a root-owned trust store.

The channel scope belongs to the trusted public-key material, not to a claim
made by the remote publisher. An unscoped additional key may vouch for every
channel; a scoped one follows the same exact-name and wildcard rules as the
official keys.

The official manifest service is reached over HTTPS. Artifact URLs may use
HTTPS or plain HTTP because their contents are pinned by the signed hashes.
This separation is intentional:

- HTTPS authenticates the official channel endpoint and prevents on-path
  substitution in transit;
- the signature authenticates the channel statement;
- SHA-256 authenticates the artifact bytes named by that statement.

Plain artifact transport does not provide confidentiality or availability. An
intermediary may block a download or feed incorrect bytes, but incorrect bytes
are rejected.

### Key rotation

Verification against a set of keys permits a non-disruptive rotation:

1. a release embeds both the outgoing and incoming public keys;
2. manifests may temporarily carry signatures for both keys;
3. publication switches to the incoming key after the fleet trusts it;
4. a later release removes the outgoing key.

Per-key signature files let a client that trusts only the outgoing key verify
during the overlap even when the primary signature already uses the incoming
key.

This overlap is essential. If a signing key is compromised before clients have
received another trusted key, those clients cannot authenticate an in-band
revocation; recovery then requires an out-of-band trust update. Removing a
compromised key protects only clients that have already installed a release
which no longer trusts it.

### Independent distributions

The same mechanism supports independent builds without changing the updater:
a different manifest origin, an optional bearer token and an additional trusted
public key are enough. The independent publisher may scope its key to selected
channels. Trust remains local to the machine; using a custom source does not
turn its key into an official one.

## Checking and choosing a version

An interactive check fetches the manifest unconditionally, so Lotor does not
retain a stale local manifest cache between commands. The update library
supports HTTP validators for other callers, but the console's check and install
paths intentionally ask the source again. A newly published manifest is visible
as soon as the source and any intervening CDN serve it; there is no additional
client-side cache to expire.

Before reporting an update, Lotor verifies in this order:

1. at least one trusted public key exists;
2. a signature matches one of those keys;
3. the signed JSON satisfies the strict manifest schema;
4. the product and requested channel match;
5. the signing key vouches for that channel;
6. the manifest contains an artifact for the running OS and architecture;
7. the channel's ordering rule considers the offered version newer.

A check stops there: it downloads no executable and changes no state. It can
show the offered version, publication time, artifact size, signing-key ID and
signed notes.

Stable channels compare semantic versions, tolerating the `v` prefix used by
Git tags and applying prerelease ordering. Build metadata does not affect
ordering.

Fast channels are designed to compare the signed `published` time with the
last accepted time for that channel. The library implements this comparison,
but the current interactive check and install path does not persist or supply
that last timestamp. Therefore authenticity and integrity hold for `dev` and
`try-*`, but rollback resistance across separate invocations does **not** yet:
an older, still-valid signed fast manifest can be offered again and considered
a candidate when its version differs from the running one.

## Download and staging

The running daemon performs staging without elevated privileges.

For an uncompressed artifact, it streams at most the signed size into the stage
while calculating SHA-256. A size or hash mismatch removes the candidate.

For a compressed artifact, verification has two boundaries:

1. the downloaded archive must match its signed size and SHA-256 before a
   decompressor sees it;
2. decompression is streamed through the signed binary-size limit, and the
   resulting binary must match its own signed SHA-256.

The archive does not survive successful unpacking. Failed or partial candidates
are removed rather than left looking installable. The size limits also prevent
a validly structured but incorrectly described archive from filling the
filesystem during unpacking.

Before completing the stage, the daemon executes the new binary as its own
unprivileged user. The candidate runs a self-check which:

- proves the executable can run on the host architecture;
- copies the live configuration database;
- applies and validates configuration migrations only on that copy;
- proves the candidate can read the resulting configuration.

The self-check does not touch radios, listeners or the live database. Passing it
means “this binary starts and understands this configuration,” not “every
runtime subsystem is healthy.”

The exact verified manifest and signature are stored beside the binary. A
`ready` marker containing the version, channel, platform and binary hash is
written last, after the other files have been flushed. The privileged side
ignores every partial stage lacking that marker.

## The privilege boundary

The daemon is allowed to write its stage but not the installed executable. A
root-owned watcher notices the final marker and starts a short-lived installer.
The installer is the already-installed Lotor binary, not the candidate.

Before changing the executable, it reconstructs trust independently:

- it reloads the embedded and root-owned public keys;
- it re-verifies the exact staged manifest and signature;
- it re-parses the strict manifest;
- it re-checks the key's channel scope;
- it selects the signed artifact named by the stage platform marker;
- it hashes the staged binary again against that artifact's signed binary hash.

Only then does it copy the candidate into the executable's filesystem, preserve
the previous executable as a hard link, and rename the candidate over the
installed path. The final rename is atomic: there is no interval in which the
service path names no binary.

The replacement is then marked as pending, the completed stage is cleared, and
the service is restarted under its normal unprivileged account. Root never
executes the candidate binary.

This boundary prevents a compromised daemon from turning arbitrary staged bytes
into code executed by root. It does not make the staging daemon irrelevant to
policy: the privileged verifier accepts any manifest authorized by the local
trust set and its channel pin; it does not independently enforce a root-owned
“this machine follows release only” policy. It also does not compare the stage
platform marker with the platform on which the installer is running. The normal
staging path chooses both values before the privilege boundary, but a compromised
stager could therefore choose any still-valid signed artifact accepted by the
local trust set. It still cannot turn unsigned bytes into root execution. The
resulting service runs with the same unprivileged account and sandbox as before.

## Probation and rollback

Installation preserves the previous executable and arms a pending-update
marker. The new daemon detects that marker at startup.

If the process remains alive for 90 seconds, it clears the marker and commits
the update. If it repeatedly fails and the service manager reaches its start
limit, the rollback service restores the previous executable and restarts it.

Rollback is guarded by the pending marker. A crash loop at any other time does
not replace a known-good binary merely because a backup exists.

Probation is deliberately a **liveness** check. It catches wrong architectures,
startup failures, configuration incompatibilities that escaped self-check and
immediate crash loops. It does not judge radio health, network reachability,
mesh behavior or application-level correctness. A defective release that keeps
the process alive for 90 seconds is committed and requires an explicit
subsequent update or downgrade.

## Build provenance and publication

All official channels use one build recipe:

- CGO is disabled;
- paths are trimmed;
- the functional version is the single explicit linker value;
- the Go toolchain embeds the VCS revision, source timestamp, tree state,
  toolchain and target;
- a build made from a Git checkout is refused if VCS provenance is absent.

Development and try versions derive their identity from the source commit's
timestamp and revision, so rebuilding the same source produces the same version
name. The manifest's `published` value is a separate, signed statement of when
that channel was updated.

The stable publication path rebuilds every target independently and compares
the bytes with the artifacts about to ship. Compression is deterministic and
is repeated as a second byte-for-byte proof. Artifact hashes are measured by
the same process that creates the manifest.

Artifacts are uploaded before any manifest points to them. Uploads are
immutable by version and path: an existing object is accepted only when its
bytes are identical, otherwise publication fails. The channel statement is
published last. Any transient skew between a manifest and its signature fails
closed at verification rather than installing a mixture of releases.

Stable compressed artifacts also receive a build-provenance attestation binding
them to the repository, commit and workflow. That attestation is useful for
external auditing, but the updater does not depend on it: its automatic trust
root remains the Minisign key embedded in or added to the running installation.

## Guarantees and limits

| Property | What Lotor guarantees |
|---|---|
| Manifest authenticity | The statement verifies under a locally trusted key |
| Channel separation | The verifying key must locally vouch for the named channel |
| Product binding | A signed manifest for another product is refused |
| Artifact integrity | Downloaded size and SHA-256 are checked before decompression |
| Binary integrity | Unpacked size and SHA-256 are checked during staging; root checks the SHA-256 again |
| Partial-stage safety | The installer acts only after the final ready marker exists |
| Privilege separation | The candidate is never executed as root |
| Replacement atomicity | The installed path changes by same-filesystem rename |
| Crash-loop recovery | The previous binary is restored only during update probation |
| Normal stable selection | Equal and older semantic versions require explicit force |
| Reproducibility | Stable publication proves independent builds and gzip output byte-for-byte |

The system intentionally does **not** claim:

- availability or confidentiality of artifact transport;
- that a validly signed release is bug-free or non-malicious;
- threshold signatures, mandatory transparency-log verification or manifest
  expiry;
- full freeze-attack protection when an update source withholds newer
  manifests;
- end-to-end fast-channel anti-replay until the accepted publication timestamp
  is persisted;
- application-level health assessment during probation;
- automatic recovery from a signing-key compromise before a replacement key is
  already trusted.
