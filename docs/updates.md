# Release channels — infrastructure runbook

The relays only ever talk to `https://updates.meshrunner.dev/lotor/…`,
which serves nothing but static files: a signed `manifest.json` per
channel, and `channels.json` for discovery. The binaries live in two
S3 buckets, one per train, as gzipped objects keyed by version —
`lotor/<version>/lotor_<version>_<os>_<arch>.gz` — written once and
never overwritten; the manifests point at them. Artifact downloads
ride plain http on purpose: the manifest arrived signed over TLS and
its sha256 pins the artifact's bytes, so the artifact transport is
just a pipe — a MITM there produces a failed hash and nothing else.
TLS is owed where the *decision* travels, and that is the manifest
host. Leaving any host one day is an rsync of the static tree, a copy
of the objects, and fresh manifests — no deployed relay changes.

Two hashes per artifact, because there are two boundaries: `sha256`
and `size` describe the fetched bytes, verified before anything
parses them — an attacker on the plain-http leg never reaches the
decompressor; `binary_sha256` and `binary_size` describe what
unpacking must produce, and are what the daemon stages and the
privileged installer re-verifies. `cmd/relsign manifest -gzip`
compresses and hashes in one process, so the `.gz` the workflow
uploads is byte-for-byte what the signature vouches for.

## One-time setup

1. **The `updates` repository** (same org). Contents become the site:
   ```
   CNAME                      ← "updates.meshrunner.dev"
   lotor/channels.json
   lotor/<channel>/manifest.json
   lotor/<channel>/manifest.json.minisig
   ```
   Enable Pages on the default branch, root. Add the DNS record:
   `updates.meshrunner.dev CNAME <org>.github.io`.

2. **The signing keys — one per train.** On a trusted machine:
   ```
   go run ./cmd/relsign keygen /private/stable release rc beta
   go run ./cmd/relsign keygen /private/fast   dev try-*
   ```
   The channel pin rides each `.pub` file's comment line, and every
   verifier reads it there: a key vouches for its train and no other,
   so the hot fast key can never sign anything a release-following
   relay accepts.
   - Each **secret** (`relsign.key`) becomes the `RELSIGN_KEY` secret
     of its **Actions environment** (below) — file content, verbatim,
     never printed and never committed.
   - Both **public** halves go into `officialKeys` in
     `internal/update/trust.go`, exactly as the files read. The
     workflows refuse to publish while that list is empty.

3. **The buckets — one per train.** The signature protects integrity;
   the split protects *availability*: the fast train signs every push
   with no reviewer, so its credentials are the hot ones, and they
   must not be able to delete what the stable train published. Each
   bucket is public-read over plain http (the manifests carry the
   trust), written only by its train's CI credentials, and those
   credentials hold `PutObject` and `ListBucket` — no delete on the
   stable bucket for anyone automated, delete on the fast bucket for
   the sweeper. The upload script refuses an existing key: artifacts
   are immutable, and a collision is a bug to hear about, not bytes
   to replace.

4. **The Actions environments.** Two, matching the trains, and each
   holds its own copies beside its signing key — environment secrets
   are not shared. Per environment:
   - secrets: `RELSIGN_KEY`, `UPDATES_DEPLOY_KEY`, and the bucket's
     `S3_ACCESS_KEY` + `S3_SECRET_KEY`;
   - variables: `S3_ENDPOINT` (the https API endpoint uploads talk
     to), `S3_BUCKET`, `DL_BASE` (the public http base the artifact
     URLs are minted under, no trailing slash), and optionally
     `S3_REGION` (defaults to `us-east-1`, which most S3-compatibles
     accept).
   - `stable` — protection rules: deployment from tags `v*` only, and
     a required reviewer, so pushing a tag is not enough to mint a
     signed stable release — someone approves the signing job.
     (Protection rules need a public repository, or Team/Enterprise
     for a private fork.)
   - `fast` — deployment from `main` only, no reviewer: dev signs
     every push by design, and the try sweeper runs here too.
   The signing jobs declare `environment:`; keep them minimal and
   free of third-party actions — the environment bounds who and when,
   not what a compromised step inside the approved job could read.

5. **The deploy key.** `ssh-keygen -t ed25519 -f updates-deploy`;
   the public half becomes a *deploy key with write access* on the
   `updates` repository, the private half the org secret
   `UPDATES_DEPLOY_KEY`.

## Channels

| channel      | fed by                           | binaries                    |
|--------------|----------------------------------|-----------------------------|
| `release`    | tag `vX.Y.Z`                     | stable bucket, that version |
| `rc`         | tag `vX.Y.Z-rc.N` (and stables)  | idem                        |
| `beta`       | tag `vX.Y.Z-beta.N` (and stables)| idem                        |
| `dev`        | every push to main               | fast bucket, its own version|
| `try-<slug>` | manual run of *try channel*      | fast bucket, its own version|

Every publication is its own immutable version — nothing rolls, and
nothing is ever replaced in place. A stable tag still gets a GitHub
release page for its notes; no binaries ride on it. A stable release
also refreshes `rc` and `beta`: a less stable channel is never behind
a more stable one. When one is legitimately ahead (a running beta
cycle), the relays' own semver guard keeps them where they are. And
because dev versions accumulate instead of overwriting each other,
bisecting a fleet regression is one hand-written manifest away —
every build of the last two weeks is still addressable.

`try` builds any branch (`ref: my-branch`) or PR (`ref: pr/123`) as
an ordinary version whose name embeds the subject — no releases, no
tags, no notifications. The weekly sweep deletes try manifests older
than two weeks and the version each pointed at, then dev versions on
the same clock — all but the one the live manifest names. The stable
bucket is never swept; a few megabytes per release is what history
costs.

## Key rollover

Verification always runs against a *set*: the keys embedded in the
binary plus whatever root deposited in `/etc/lotor/trusted-keys/`.
An official rotation is three ordinary releases:

1. Ship a release whose `officialKeys` lists old **and** new.
2. Once the fleet is past it, switch `RELSIGN_KEY` to the new key.
   During the overlap the publisher may lay one signature file per
   active key beside a manifest.
3. Releases later, drop the old key from the list.

No operator does anything at any step. A **compromised** key is the
degenerate case of step 3 done immediately: ship a release signed by
the new key that no longer trusts the old — relays that have not yet
taken that release remain exposed until they do, which is the honest
limit of the scheme.

## Forks

A fork runs the same workflows with its own `RELSIGN_KEY`, its own
manifest host (its Pages, or anywhere static) and its own bucket —
or any static host at all for the artifacts, plain http included:
the manifest's hashes carry the trust, not the artifact transport.
On the relay: `/update set url=… channel=…`, and the fork's
`relsign.pub` dropped in `/etc/lotor/trusted-keys/` by root. For
artifacts served as a private GitHub repository's release assets,
`/update set token=…` — downloads then ride the GitHub API with
`Accept: application/octet-stream`.

## The move off release assets (2026-08)

The manifests are the hinge: a relay only ever acts on the current
manifest, so the flip is atomic per channel — but a relay must *read*
the new statement before its channel speaks it. Manifests carrying
`compression` or a plain-http URL are refused by clients from before
the bridge (strict parsing refuses unknown fields — by design), so
the order was:

1. **Bridge** — ship the client that reads both forms (gzip, http
   URLs) through the *old* pipeline; let each channel's fleet take it.
2. **Flip** — switch the workflows to bucket uploads and compressed
   manifests. A relay still running a pre-bridge build after the flip
   is stuck on its last version and says so in `update check`; the
   cure is one manual install of any bridge-or-later build.
3. **Sweep the past** — delete the rolling `dev` release, the `try`
   release and the `dev` tag by hand; the tag's honesty role is
   carried by the version string, which embeds the commit. Old stable
   release assets may stay: nothing references them.

## The install dance

Installing never grants the daemon a privilege. The pieces, in order:

1. `/update install` (console, admin): the daemon downloads into
   `/var/lib/lotor/updates/`, verifies hash and signature, runs the
   new binary's own `update selfcheck` — catching the wrong
   architecture and the corrupt download before anything privileged
   is asked — and writes the `ready` marker last.
2. **`lotor-install.path`** sees the marker and starts
   **`lotor-install.service`** (root, oneshot): `lotor update apply`
   re-verifies the whole stage against *root's* trust store, links
   the old binary aside as `.prev`, renames the new one in — no
   instant without a binary at the path — arms the probation marker,
   and restarts the service.
3. The new daemon runs **on probation**: 90 healthy seconds commit
   the update. If instead it crash-loops, the service's start limit
   trips **`OnFailure=lotor-rollback.service`**, which puts `.prev`
   back — and touches nothing when no update is on probation, so an
   unrelated fault never rolls a good binary back.

Enable the machinery once:

```
cp contrib/systemd/*.{service,path} /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now lotor-install.path
systemctl enable lotor.service
```

The signature is the privilege boundary: a compromised daemon can
stage whatever it likes and gain nothing, because the installer
installs what verifies under `/etc/lotor/trusted-keys` and the
embedded keys — or nothing.

## Product identity and build provenance

Three contracts, three homes, never confused:

- **the product** (`internal/product`) — slug, name, description,
  homepage, update base, and the install ABI. The slug is the
  manifest's `product` field, the artifact prefix and the S3 key
  prefix; a client refuses any manifest naming another product,
  however valid its signature. Scripts and workflows read the same Go
  source through `go run ./internal/product/cmd/meta`;
- **the build** (`internal/version`) — the functional version is the
  ONE ldflag the official recipe stamps
  (`-X …/internal/version.release=1.2.3`); revision, source time,
  tree state, toolchain and target come from the toolchain's native
  VCS stamping (`-buildvcs=true`). `lotor version` prints the block,
  `lotor version --json` feeds inventories, and every surface —
  banner, status, MQTT, OTA `ver` — repeats the same value;
- **the install ABI** — `lotor.service`, `/var/lib/lotor`,
  `/usr/local/bin/lotor` — spelled out in `internal/product`, checked
  by `task identity:check`, and never derived from the slug: a
  rebrand must not relocate a running fleet.

Official builds are reproducible: `scripts/build` is the one recipe
(Task and CI both), it injects no wall clock, and `task build:repro`
proves two clean builds byte-identical. The three times that matter
stay distinct — `source time` is the commit's (in the binary),
`published` is the manifest's (signed), and the job's own clock lives
only in the CI provenance. Versions are functions of the source: dev
and try derive their timestamp from the commit, so a rerun mints the
same identity, and the immutable upload accepts a byte-identical
replay while refusing a different object under a known name. Git tags
keep their `v` prefix; product versions drop it, and the update
comparator tolerates both so old manifests stay valid.

For stable releases, a GitHub artifact attestation can bind the
distributed bytes to repository, commit and workflow. It complements
Minisign, never replaces it: the signed manifest and the embedded
keys remain the auto-update's root of trust.
