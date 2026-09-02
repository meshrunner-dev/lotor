# MQTT observer — architecture and wire contract

Publishing what a relay hears and sends to MQTT turns it into an observer of
the mesh. Analyzers, maps and dashboards consume a de-facto JSON contract
established by the observer firmware ecosystem. Lotor preserves that contract
where interoperability requires it and documents its extensions explicitly.

## Wire contract

### Topics and identifiers

The default topic template is:

```text
meshcore/{iata}/{device}/{type}
```

It expands to these message classes:

```text
meshcore/{iata}/{device}/status
meshcore/{iata}/{device}/packets
meshcore/{iata}/{device}/raw
meshcore/{iata}/{device}/neighbors
```

`iata` is the observation site's three-character ASCII alphanumeric code. It is
operator-chosen, normalized to uppercase and required for every observer, even
when a custom topic template does not interpolate `{iata}`. It identifies the
site; it is unrelated to MeshCore regions.

`device` is the watched relay's complete public key in uppercase hexadecimal.
The same convention applies to payload `origin_id` values, neighbour public
keys, JWT public keys and pubkey-derived usernames. Whole wire frames and path
tokens remain lowercase hexadecimal.

Custom topic templates may use `{iata}`, `{device}`, `{token}` and `{type}`.
Lotor refuses templates that leave a placeholder unresolved, expand to an
empty topic level, begin or end with `/`, or contain MQTT wildcards. The
Meshrank layout is the same mechanism:
`meshrank/uplink/{token}/{device}/{type}`. Its preset leaves the high-volume
RAW feed disabled.

### PACKET — QoS 0, never retained

One message is published per selected frame. Received frames are enabled by
default; transmitted frames follow the `tx` policy described below. Several
numbers deliberately travel as JSON strings because that is the ecosystem
contract.

```json
{
  "timestamp": "2026-08-28T02:32:16.123456+00:00",
  "hash": "16 lowercase hex characters — the 8-byte packet hash",
  "origin": "the configured display name",
  "origin_id": "64 uppercase hex characters — the relay public key",
  "type": "PACKET",
  "direction": "rx | tx",
  "time": "HH:MM:SS (UTC)",
  "date": "DD/MM/YYYY (UTC)",
  "len": "wire length, decimal string",
  "packet_type": "payload type, decimal string",
  "route": "F | D",
  "payload_len": "decimal string",
  "raw": "the whole wire frame, lowercase hex",
  "SNR": "RX only — one-decimal string",
  "RSSI": "RX only — integer string",
  "path": ["per-hop lowercase hex tokens — direct packets with hops only"]
}
```

`route` is two-valued: transport variants collapse into their direct or flood
direction. The reference may publish a rebroadcast `score`; Lotor has no
equivalent and omits this optional field.

### STATUS — QoS 1, optionally retained

STATUS is published immediately after every established or re-established
broker session, then every `status_interval`. The default interval is five
minutes. An explicit `status_interval=0` disables STATUS completely, including
the connection-time publication. There is no last will; consumers infer an
offline observer from silence.

```json
{
  "status": "online",
  "timestamp": "2026-08-28T02:32:16.123456+00:00",
  "origin": "name",
  "origin_id": "UPPERCASE_PUBKEY",
  "model": "radio driver",
  "firmware_version": "Lotor version",
  "radio": "869.618000,62.5,8,8",
  "client_version": "lotor/version",
  "repeat": "on | off",
  "stats": {
    "uptime_secs": 1,
    "packets_sent": 1,
    "packets_received": 1,
    "errors": 1,
    "noise_floor": -104,
    "tx_air_secs": 1,
    "rx_air_secs": 1,
    "recv_errors": 1,
    "journal_degraded": false,
    "journal_failures": 0,
    "journal_last_error": "optional last error",
    "journal_last_fail_at": "optional UTC timestamp"
  }
}
```

The radio string is `MHz to six decimals,kHz to one decimal,SF,CR`. Every stat
is optional and omitted when unknown. Journal fields are Lotor extensions: they
appear after a journal failure and preserve the latest failure even after
recovery. The reference also publishes fields such as `battery_mv`,
`queue_len` and `internal_heap`; Lotor currently omits them.

The retain flag is sent only when the observer's `retain` setting or selected
preset enables it.

### RAW — QoS 0, never retained, disabled by default

```json
{
  "origin": "name",
  "origin_id": "UPPERCASE_PUBKEY",
  "timestamp": "2026-08-28T02:32:16.123456+00:00",
  "type": "RAW",
  "data": "lowercase wire-frame hex"
}
```

RAW is the same selected frame carried by `PACKET.raw`, without the analysis.
The observer still requires the frame to parse successfully before publishing
either representation.

### NEIGHBORS — QoS 0, optionally retained, disabled by default

```json
{
  "timestamp": "2026-08-28T02:32:16.123456+00:00",
  "origin": "name",
  "origin_id": "UPPERCASE_PUBKEY",
  "total_neighbors": 1,
  "queried_neighbors": 1,
  "truncated": false,
  "self": {
    "regions": "a,b,c",
    "default_region": "a",
    "scopes": "a,b,c",
    "default_scope": "a"
  },
  "neighbors": [{
    "pubkey": "UPPERCASE_PUBKEY",
    "snr": 7.5,
    "heard_secs_ago": 42,
    "regions": "a,b",
    "scopes": "a,b",
    "status": "responded | timeout | send_failed"
  }]
}
```

`regions` and `default_region` are the current vocabulary. `scopes` and
`default_scope` are deprecated compatibility aliases carrying the same values;
consumers should migrate to the region names. An unknown neighbour age is JSON
`null`, never zero.

Setting `neighbours_interval` is explicit consent to real RF emissions and has
a 30-minute floor. A round first runs a zero-hop discovery and waits for its
one-minute collection window, then asks each neighbour for its regions, one at
a time. A round also runs when the first broker session is established; later
reconnections do not start an extra round.

The transmit gate must be `on-air` or `on-air-zero-hop`. If it cannot key the
radio, the round does not run and no stale or all-failed snapshot is published.
If discovery itself is unavailable after admission, the observer continues
with the current neighbour table. Per-neighbour results mean:

- `responded`: the region reply arrived;
- `timeout`: the question was sent but no reply arrived;
- `send_failed`: that particular question could not be sent.

The round runs outside the frame-consumer loop, so waiting for neighbours does
not stop packet observation. A tick that lands while a round is already active
is skipped rather than queued.

### Device authentication

Most community brokers authenticate the device with a JWT-shaped password and
the fixed username `v1_{UPPERCASE_PUBKEY}`. The first two token segments are
base64url JSON; the third is the Ed25519 signature over `header.payload` in
uppercase hexadecimal rather than base64url. Claims are `publicKey`, `aud`,
`iat`, `exp`, and optional `owner`.

Setting `audience` selects this mode. Lotor mints credentials from the watched
relay's identity for every connection attempt, including reconnects. The
default token lifetime is 24 hours; broker presets may shorten it. Without an
audience, a configured username and password are used as-is, with `{pubkey}` in
the username expanding to the uppercase relay key. Empty credentials connect
anonymously.

## Lotor configuration

Each `/mqtt` instance is one broker connection. Like radios and relays, it is
layered: a community-broker `profile` supplies defaults and per-profile
overrides patch them without destroying settings saved for another profile.

| Attribute | Type | Default | Meaning |
|---|---|---|---|
| `profile` | string | `custom` | Community preset; `custom` starts empty |
| `disabled` | bool | false | Keep the configuration but run no connection |
| `url` | string | required | `tcp://`, `ssl://`, `ws://` or `wss://` broker |
| `username` | string | empty | Static credential; `{pubkey}` expands to the relay key |
| `password` | secret string | empty | Static credential |
| `audience` | string | empty | Non-empty selects device-token authentication |
| `token_lifetime` | duration | 24h | Lifetime claimed by a minted token |
| `keepalive` | duration | 2m | Presets behind balancers normally use 55s |
| `ca` | string | system roots | PEM file replacing the broker trust roots |
| `retain` | bool | false | Retain STATUS and NEIGHBORS only |
| `iata` | string | required | Three-character site code, independent of the topic |
| `token` | string | empty | Per-connection token used by some topic layouts |
| `topic` | string | `meshcore/{iata}/{device}/{type}` | Topic template |
| `relay` | string | the only relay | Relay whose identity and traffic are observed |
| `packets` | bool | true | Publish analyzed PACKET messages |
| `raw` | bool | false | Publish RAW messages too |
| `rx` | bool | true | Publish received frames |
| `tx` | `off`, `self-adverts`, `all` | `self-adverts` | Which relay emissions are published |
| `types` | words | all | Payload-type names admitted by the filter |
| `status_interval` | duration | 5m | STATUS cadence; zero disables it completely |
| `neighbours_interval` | duration | off | Neighbour round cadence and RF consent; floor 30m |
| `origin` | string | relay node name | Override the published display name |
| `owner` | 64 hex characters | empty | Optional operator key in device tokens |

IATA remains mandatory for disabled observers so that parking and unparking
cannot turn a previously accepted configuration into an invalid one.

## Data path and visibility

The internal bus is the observer seam. Each observer subscribes with a bounded
256-event buffer and selects events belonging to its configured relay:

- `FrameHeard` provides frames delivered to the relay, including explicit
  provenance; only events with no emitting binding are RF receptions with SNR
  and RSSI and are eligible for RX publication;
- `FrameSent` provides actual relay emissions;
- station-originated emissions and their controller-local hand-over to the
  relay are not relay RX or TX and are excluded;
- shadow emissions are excluded because they never went on the air;
- dropped or refused transmissions are not presented as sent frames.

Consequently `tx=all` means all actual emissions from the watched relay, not
all producers sharing its radio. `tx=self-adverts` publishes only adverts whose
embedded identity is the watched relay's own key.

This distinction matters on a shared radio. A hand-over contains real MeshCore
traffic but no demodulator measurement; publishing it as RX would duplicate the
station's emission under the relay identity and fabricate `0` RSSI/SNR. Its
binding and `caused_by` correlation remain available to logs and the journal
instead.

The observer parses selected frames with `meshrunner.dev/pkg/meshcore`, applies
the payload-type filter, builds JSON and publishes synchronously with a bounded
wait. A slow or disconnected broker cannot block the relay indefinitely: once
the subscription buffer fills, later bus events are dropped and counted.
There is no unbounded offline MQTT queue.

Traffic selection and filtering are logged at debug level. Payload encoding,
broker completion and lifecycle plumbing are trace logs. The
frame correlation identifier is carried through these logs but is deliberately
absent from the ecosystem JSON contract.

Observer counters — published messages, publish errors, bus drops, configured
filter drops and last publication time — are available through
`/mqtt/<name> status`. They are operational counters, not fields in the MQTT
STATUS payload, whose `stats` describe the watched relay and journal.

## Broker and lifecycle

The client uses `eclipse/paho.mqtt.golang` for TCP, TLS, WebSocket transport,
keepalive and reconnection. Initial connection happens in the background so an
unavailable broker cannot block daemon assembly. Reconnect attempts back off to
two minutes. Individual publishes wait at most five seconds and fail immediately
when no broker session is open. Custom `ca` files create a dedicated root pool
with TLS 1.2 as the minimum version.

The manager starts observers from configuration, reconnects an observer when
its effective configuration changes, and rebinds it when the watched relay is
rebuilt or topology changes alter the meaning of an omitted `relay`. An observer
never restarts the relay it watches.

`/mqtt/<name> status` reports `connected`, `connecting`, `disabled` or `down`,
including a startup refusal cause and the counters above. `disable` and `enable`
park and unpark a connection without discarding its configuration; collection
listings mark parked observers with `X`. `export` emits the complete layered
configuration, including inactive profile overrides and masked secrets.

The JSON payloads and topic behavior are pinned by golden tests in
`internal/mqtt`; connection behavior is covered with an in-process MQTT peer and
observer-loop tests.
