# Lotor

<https://meshrunner.dev/lotor>

Lotor is a mesh relay daemon: one machine, one daemon, one or more
**relays** — each an instance of a mesh protocol bound to its radio
hardware. MeshCore is the first supported protocol.

This is an independent implementation. It is not affiliated with or
endorsed by the MeshCore project.

## What it does today

- **Relays** on real hardware (SX126x over SPI): receive, judge,
  deduplicate and forward mesh traffic, with listen-before-talk, a
  duty-cycle budget, and a transmit gate that steps from dry run to
  shadow to on-air.
- **Configuration lives in SQLite** — one file is the whole relay,
  identity included: back it up, restore it, done. Every change is a
  revision with who/when/what, and `undo` inverts the newest.
- **An admin console** in the style of a network operating system:
  contexts (`/relay`, `/radio`, `/mqtt`, `/update`…), `Tab`
  completion, colour by symbol class, `print` with provenance,
  `export` as replayable lines. Telnet is read-only; the local unix
  socket is admin, authenticated by file permissions.
- **Self-update** from signed release channels (`release`, `rc`,
  `beta`, `dev`), verified against keys pinned per release train, and
  installed across a privilege boundary made of systemd units — with
  probation and automatic rollback.
- **MQTT observing**: publish what the relay hears and sends to
  brokers, in the observer ecosystem's own JSON, so analyzers and
  maps consume lotor like any observer node.

See [DESIGN.md](DESIGN.md) for the architecture,
[the update mechanism and its guarantees](docs/updates.md), and
[the MQTT observer wire contract](docs/mqtt-observer.md).

## Deploying

Manual for now; the pieces are the same ones the update machinery
maintains afterwards. As root, on the target (example: linux/arm64 —
a Raspberry Pi):

```sh
# The binary, from the channel of your choice (dev shown): the signed
# manifest names the current version's artifact, gzipped.
url=$(curl -fsSL https://updates.meshrunner.dev/lotor/dev/manifest.json \
  | jq -r '.artifacts["linux/arm64"].url')
curl -fL "$url" | gunzip > /usr/local/bin/lotor
chmod 755 /usr/local/bin/lotor

# A dedicated user: the radio needs nothing beyond spi and gpio.
# (The unit grants those as supplementary groups; Raspberry Pi OS has
# them out of the box — on plain Debian, create them and give them
# the /dev/spidev* and /dev/gpiochip* nodes via udev first.)
useradd --system --no-create-home --shell /usr/sbin/nologin lotor

# The systemd units: the daemon (sandboxed, OnFailure rollback), and
# the update installer's path unit and rollback.
for u in lotor.service lotor-install.service lotor-install.path lotor-rollback.service; do
  curl -fL -o "/etc/systemd/system/$u" \
    "https://raw.githubusercontent.com/meshrunner-dev/lotor/main/contrib/systemd/$u"
done
systemctl daemon-reload
systemctl enable --now lotor-install.path
systemctl enable --now lotor.service
```

The state directory (`/var/lib/lotor` — configuration database and
journal) and the runtime directory (console socket) are created and
owned by systemd; nothing else on the filesystem belongs to the
daemon.

First configuration happens on the console, not in a file:

```
lotor console
[admin@host] > /radio add slot1 driver=sx126x-spi profile=… spi=/dev/spidev0.0
[admin@host] > /relay add main protocol=meshcore radio=slot1 profile=… identity=new
[admin@host] > /update set channel=release
```

`?` and `Tab` explain and complete every step, profiles carry the
band and board presets, and `export` at the root prints the lines
that recreate the whole configuration.

## Developing

```
go build -o bin/ ./cmd/lotor
task check     # the gate: formatting, tidy, strict lint, race tests, vuln scan
```

Bare `lotor` prints the help — running the daemon is an explicit
`lotor run`. `lotor console` opens a running daemon's console
(127.0.0.1:2323 by default) and ends cleanly on `quit` and Ctrl+D.

## License

[MIT](LICENSE).
