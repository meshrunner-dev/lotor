# Release channels — infrastructure runbook

The relays only ever talk to `https://updates.meshrunner.dev/lotor/…`,
which serves nothing but static files: a signed `manifest.json` per
channel, and `channels.json` for discovery. The binaries live in
release assets; the manifests point at them. Leaving GitHub one day is
an rsync of the static tree to another host, and fresh manifests with
new artifact URLs — no deployed relay changes.

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

3. **The Actions environments.** Two, matching the trains:
   - `stable` — holds the stable train's `RELSIGN_KEY`. Protection
     rules: deployment allowed from tags `v*` only, and a required
     reviewer, so pushing a tag is not enough to mint a signed stable
     release — someone approves the signing job. (Protection rules
     need a public repository, or Team/Enterprise for a private
     fork.)
   - `fast` — holds the fast train's key, no gates: dev signs every
     push by design.
   The signing jobs declare `environment:`; keep them minimal and
   free of third-party actions — the environment bounds who and when,
   not what a compromised step inside the approved job could read.

4. **The deploy key.** `ssh-keygen -t ed25519 -f updates-deploy`;
   the public half becomes a *deploy key with write access* on the
   `updates` repository, the private half the org secret
   `UPDATES_DEPLOY_KEY`.

## Channels

| channel      | fed by                          | binaries                     |
|--------------|---------------------------------|------------------------------|
| `release`    | tag `vX.Y.Z`                    | the tag's release assets     |
| `rc`         | tag `vX.Y.Z-rc.N` (and stables) | idem                         |
| `beta`       | tag `vX.Y.Z-beta.N` (and stables)| idem                        |
| `dev`        | every push to main              | rolling pre-release `dev`    |
| `try-<slug>` | manual run of *try channel*     | fixed pre-release `try`      |

A stable release also refreshes `rc` and `beta`: a less stable channel
is never behind a more stable one. When one is legitimately ahead (a
running beta cycle), the relays' own semver guard keeps them where
they are.

`try` builds any branch (`ref: my-branch`) or PR (`ref: pr/123`) into
the shared `try` bucket — one pre-release, assets named by subject,
no tags, no notifications. The weekly sweep deletes manifests older
than two weeks and the assets they pointed at.

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

A fork runs the same workflows with its own `RELSIGN_KEY` and its own
manifest host (its Pages, or anywhere static). On the relay:
`/update set url=… channel=…`, and the fork's `relsign.pub` dropped in
`/etc/lotor/trusted-keys/` by root. For a private repository's assets,
`/update set token=…` — asset downloads then ride the GitHub API with
`Accept: application/octet-stream`.

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
