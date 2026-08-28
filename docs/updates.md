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

2. **The signing key.** On a trusted machine:
   ```
   go run ./cmd/relsign keygen /some/private/place
   ```
   - The **secret** (`relsign.key`) becomes the org Actions secret
     `RELSIGN_KEY` — file content, verbatim. It is never printed and
     never committed.
   - The **public** half goes into `officialKeys` in
     `internal/update/trust.go`, as the file reads. The workflows
     refuse to publish while that list is empty.

3. **The deploy key.** `ssh-keygen -t ed25519 -f updates-deploy`;
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
