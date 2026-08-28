# MQTT observer — plan and wire contract

Publishing what the relay hears (and sends) to MQTT brokers turns it
into an observer of the mesh: analyzers, maps and dashboards consume a
de-facto JSON contract established by the observer firmware ecosystem.
This document pins that contract as measured from the reference
implementation, then the design for lotor. Interop is the point: a
consumer that already ingests observer nodes must ingest lotor without
noticing a difference.

## The wire contract (measured, not guessed)

### Topics

```
meshcore/{iata}/{device_id}/status
meshcore/{iata}/{device_id}/packets
meshcore/{iata}/{device_id}/raw
```

`iata` is the site's three-letter region code, operator-chosen.
`device_id` is the node's public key, lowercase hex, in full. A second
layout exists (`meshrank/uplink/{token}/{device_id}/{type}`, no raw)
and custom templates with `{iata} {device} {token} {type}`
placeholders; lotor's topic attribute is such a template, so all three
layouts are one mechanism.

### PACKET (QoS 0, not retained)

One message per frame heard (and optionally per frame sent). The
schema's quirks are part of the contract — several numbers travel as
JSON strings, and field presence varies by direction:

```json
{
  "timestamp":  "2026-08-28T02:32:16.123456+00:00",
  "hash":       "16 hex chars — the packet hash, 8 bytes",
  "origin":     "the node's display name",
  "origin_id":  "64 hex chars — the node public key",
  "type":       "PACKET",
  "direction":  "rx | tx",
  "time":       "HH:MM:SS (UTC)",
  "date":       "DD/MM/YYYY (UTC)",
  "len":        "wire length, decimal AS STRING",
  "packet_type": "payload type, decimal AS STRING",
  "route":      "F | D",
  "payload_len": "decimal AS STRING",
  "raw":        "the whole wire frame, lowercase hex",
  "SNR":        "rx only — %.1f AS STRING",
  "RSSI":       "rx only — integer AS STRING",
  "score":      "rx only, optional — int(score*1000) AS STRING",
  "path":       ["per-hop hex tokens — direct packets with hops only"]
}
```

`route` is two-valued in the reference (`isRouteDirect()` collapses
transport variants). `score` is the firmware's rebroadcast score;
lotor has no equivalent and omits it, which the schema allows.

### STATUS (QoS 1, retain where the broker allows)

Published every `status_interval` (reference default 5 min), value
`"online"`; there is no LWT in the reference — consumers infer offline
from silence.

```json
{
  "status": "online", "timestamp": "…",
  "origin": "name", "origin_id": "pubkey hex",
  "model": "hardware string", "firmware_version": "…",
  "radio": "869.618000,62.5,8,8   — %.6f MHz,%.1f kHz,sf,cr",
  "client_version": "…",
  "repeat": "on | off   — whether the node forwards",
  "stats": { "uptime_secs": 1, "packets_sent": 1, "packets_received": 1,
             "errors": 1, "noise_floor": -104, "tx_air_secs": 1,
             "rx_air_secs": 1, "recv_errors": 1 }
}
```

Every stat is optional (omitted when unknown); the reference also
sends `battery_mv`, `queue_len`, `internal_heap`, which lotor omits.

### RAW (QoS 0, default off)

`{"origin", "origin_id", "timestamp", "type": "RAW", "data": "hex"}` —
the same frame as `packets.raw` without the analysis; highest volume,
off unless a consumer asks.

### NEIGHBORS — deferred

The reference can publish its neighbour table (with scopes — a shape
lotor's neighbourhood already holds). Deferred to a follow-up: it is
periodic and additive, nothing in v1 forecloses it.

## Design in lotor

### A collection kind: `/mqtt`

Each instance is one broker connection, first-class like a radio:

| attr | type | default | note |
|---|---|---|---|
| `url` | string | — | `tcp://host:1883`, `ssl://`, `ws(s)://` |
| `username` | string | "" | |
| `password` | string, Secret | "" | |
| `iata` | string | — | feeds the topic template |
| `token` | string | "" | for meshrank-style layouts |
| `topic` | string | `meshcore/{iata}/{device}/{type}` | template |
| `relay` | string | the only relay | whose identity and frames |
| `status` | bool | true | publish STATUS |
| `packets` | bool | true | publish PACKET |
| `raw` | bool | false | publish RAW |
| `rx` | bool | true | received frames |
| `tx` | enum off/self-adverts/all | self-adverts | sent frames (reference default) |
| `types` | words | all | payload-type filter, by name |
| `status_interval` | duration | 5m | |

### Data path

The bus is the seam, as it is for the sentinel: `FrameHeard` and
`FrameSent` gain the raw wire bytes, and the observer is one more
subscriber — the engine changes by two struct fields and nothing else.
The observer parses frames with the wire library, builds the JSON
above, and publishes; a slow broker loses messages (bounded queue,
drops counted and reported by status) rather than slowing the relay.

Client: eclipse/paho.mqtt.golang — reconnect, keepalive and TLS are
exactly the wheels not worth reinventing. Auth v1: none or
username/password. The reference's per-community JWT presets are out
of scope; a fork of the ecosystem's broker presets belongs in
configuration, not in code.

### Lifecycle

The manager owns observers the way it owns relays: assembled from
config at start, bounced when their instance mutates — never bouncing
the relay, whose frames they merely watch. The console gets `/mqtt`
through the existing kind machinery (add/set/print/export), plus
`status` on an instance: connected or not, what was published,
dropped, filtered.

## Order of work

1. this document;
2. bus: `Raw` on both frame events;
3. `internal/mqtt`: payloads and topics, pure and golden-tested
   against the contract above;
4. client and observer lifecycle (paho);
5. kind `mqtt` end to end (schema → store → manager → console);
6. lab: a local mosquitto, real frames observed, then a real
   community broker when the operator provides one.
