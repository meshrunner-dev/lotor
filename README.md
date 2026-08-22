# Lotor

Lotor is a mesh relay daemon: one machine, one daemon, one or more
**relays** — each an instance of a mesh protocol bound to its radio
hardware. MeshCore is the first supported protocol.

This is an independent implementation. It is not affiliated with or
endorsed by the MeshCore project.

## Status

Early, and receive-only **by construction**: the radio seam exposes no
transmit yet. The MeshCore engine runs a dry run — it hears, parses,
deduplicates and judges frames, logging what it *would* relay — so the
judgement earns trust on a live mesh before it is allowed to key a
transmitter.

See [DESIGN.md](DESIGN.md) for the architecture, the configuration
paradigm and the roadmap; `config.example.yaml` for a working
single-relay setup.

## Running

```
go build -o bin/ ./cmd/lotor
bin/lotor -config config.example.yaml -log-level debug
```

Open the console of a running daemon with the built-in client — it
ends cleanly on `quit` and on Ctrl+D, which netcat variants do not
agree on:

```
lotor console           # 127.0.0.1:2323 by default
```

`task check` is the development gate: formatting, tidy, strict lint,
race tests, vulnerability scan.

## License

[MIT](LICENSE).
