package main

// The wire vocabulary of over-the-air administration. The words are
// the ecosystem's — the companion apps send what the reference's
// CommonCLI accepts, and a repeater that answered a private dialect
// would simply not be administrable from the field. What each word
// does, though, goes through this daemon's own mutation door: the
// same validation, the same journal, the same bounce, with the
// principal saying the change came from the air.

import (
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"

	"meshrunner.dev/lotor/internal/confdb"
	"meshrunner.dev/lotor/internal/config"
	"meshrunner.dev/lotor/internal/protocol"
	enginemc "meshrunner.dev/lotor/internal/protocol/meshcore"
)

// otaSetting maps one CommonCLI variable onto the attribute it means
// here. A word absent from this table is not administrable over the
// air, and says so. Deliberately absent whole commands, acted with
// the operator: tempradio, reboot, clkreboot, poweroff — a daemon
// under systemd is not a firmware to power-cycle, and temporary
// radio params await a design of their own; they answer Unknown
// command like anything else this node does not speak.
var otaSetting = map[string]string{
	"name":                  "node_name",
	"lat":                   "node_lat",
	"lon":                   "node_lon",
	"owner.info":            "owner_info",
	"repeat":                "tx.mode",
	"advert.interval":       "advert_local_interval",
	"flood.advert.interval": "advert_flood_interval",
	"flood.max":             "flood_max_hops",
	"flood.max.unscoped":    "flood_max_unscoped_hops",
	"flood.max.advert":      "flood_max_advert_hops",
	"dutycycle":             "duty_cycle_pct",
	"tx":                    "tx_power_dbm",
	"freq":                  "frequency_hz",
	"guest.password":        "guest_password",
	"allow.read.only":       "guest_access",
}

// otaRender translates a stored value back into the wire's own
// vocabulary for get — the inverse of what set accepts. A setting
// absent here reads back as stored.
var otaRender = map[string]func(string) string{
	// The gate ladder collapses to the reference's two words: only
	// on-air repeats routable traffic; every lesser gate is off.
	"repeat": func(v string) string {
		if v == "on-air" {
			return repeatOn
		}
		return repeatOff
	},
	// The wire speaks megahertz where the store keeps hertz.
	"freq": func(v string) string {
		hz, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return v
		}
		return strconv.FormatFloat(hz/1e6, 'f', 3, 64)
	},
}

// otaReadOnly are the words a companion may read but never write from
// the air: the credentials that grant a role, and the identity that
// is the node. Changing those is the console's alone — an admin who
// could rewrite the admin word could lock the owner out of their own
// repeater with one packet.
var otaReadOnly = map[string]bool{
	"admin_password": true,
	"identity":       true,
	// The reference gates set freq to the serial port alone — a
	// frequency changed over the air strands the admin who changed
	// it — and our serial port is the console.
	"frequency_hz": true,
}

// The reference's two words for the repeat gate, spelled once.
const (
	repeatOn  = "on"
	repeatOff = "off"
)

// otaUnknown is the reference's exact answer for a word it does not
// speak — and no echo: a reply is airtime, and the sender knows what
// it typed.
const otaUnknown = "Unknown command"

// otaCommands runs one administration line from the air and returns
// what to answer. It is the engine's commands hook.
func (m *manager) otaCommands(relay string) func(line string, admin []byte) string {
	return func(line string, admin []byte) string {
		principal := "air:" + hex.EncodeToString(admin[:6])
		return m.runOTA(relay, principal, line)
	}
}

// runOTA dispatches one line. Every answer is a single short string:
// the reference's clients show it as a message.
func (m *manager) runOTA(relay, principal, line string) string {
	verb, rest, _ := strings.Cut(line, " ")
	rest = strings.TrimSpace(rest)
	switch verb {
	case "get":
		return m.otaGet(relay, rest)
	case "set":
		return m.otaSet(relay, principal, rest)
	case "advert":
		return m.otaAdvert(relay, true)
	case "advert.zerohop":
		return m.otaAdvert(relay, false)
	case "setperm":
		return m.otaSetperm(relay, principal, rest)
	case "discover.neighbors":
		if rest != "" {
			return "Err - discover.neighbors has no options"
		}
		return m.otaDiscover(relay)
	case "neighbor.remove":
		return m.otaRemoveNeighbour(relay, rest)
	case "ver":
		return "lotor " + version
	case "clock":
		if rest == "sync" {
			// The host keeps its own clock, so what sync asks for is
			// already true — an OK, in the reference's OK-with-detail
			// shape, because answering an app's sync with a failure
			// would report broken what is merely unnecessary.
			return "OK - clock already synced (system time)"
		}
		// The reference's own clock shape: minutes and a datestamp.
		return time.Now().UTC().Format("15:04 - 2/1/2006") + " UTC"
	case "time":
		// The host's clock is not the mesh's to set; the refusal
		// wears the reference's ERR shape.
		return "ERR: clock not settable (system time)"
	case "":
		return ""
	default:
		return otaUnknown
	}
}

// otaGet reads one setting back, or the node's own summary.
func (m *manager) otaGet(relay, name string) string {
	if name == "" {
		return "ERR: get what?"
	}
	attr, known := otaSetting[name]
	if !known {
		return otaUnknown
	}
	// "> value", the reference's get shape — through the setting's
	// own renderer when the wire speaks another vocabulary than the
	// store.
	if v, ok := m.relayValue(relay, attr); ok {
		if render, translated := otaRender[name]; translated {
			v = render(v)
		}
		return "> " + v
	}
	return "> (unset)"
}

// otaSet applies one setting through the mutation door.
func (m *manager) otaSet(relay, principal, rest string) string {
	name, value, ok := strings.Cut(rest, " ")
	if !ok || strings.TrimSpace(value) == "" {
		return "ERR: set what?"
	}
	attr, known := otaSetting[name]
	if !known {
		return otaUnknown
	}
	if otaReadOnly[attr] {
		return "ERR: console only"
	}
	value = strings.TrimSpace(value)
	if attr == "tx.mode" {
		// repeat on/off is the reference's word for the transmit gate.
		switch value {
		case repeatOn:
			value = "on-air"
		case repeatOff:
			value = "on-air-zero-hop"
		default:
			return "ERR: on or off"
		}
	}
	// Validated here, synchronously and lock-free — the schema is
	// immutable, the choice and the resolved configuration read from
	// the view — so a bad value earns its honest refusal now instead
	// of a false ok and a line in a journal the admin cannot see.
	choice, _ := m.relayValue(relay, attrProtocol)
	typed, err := m.parseAgainst(confdb.KindRelay, choice,
		map[string]string{attr: value}, nil)
	if err != nil {
		return otaErr(err)
	}
	if msg := m.otaDeepCheck(choice, relay, attr, typed[attr]); msg != "" {
		return msg
	}
	// Never Mutate from here: this runs in the engine's goroutine, and
	// a relay bounce joins that very goroutine. The order goes to a
	// goroutine that can safely bounce, the reply optimistic past this
	// point — the deep cross-field checks run there, journalled.
	return m.orderAir(airOrder{
		relay: relay, principal: principal, set: map[string]string{attr: value},
	}, "OK")
}

// otaDeepCheck runs the engine's own validation on a copy of the
// relay's resolved configuration with one value changed — the same
// judgement assembly passes, which is what makes "applied" honest.
// The types the schema cannot pin (tx_power_dbm accepts "auto") only
// this catches. Empty means sound; a relay with no snapshot — one
// that failed assembly — skips the check, since the change may be
// its cure.
func (m *manager) otaDeepCheck(choice, relay, attr string, value any) string {
	if strings.HasPrefix(attr, "tx.") {
		return "" // the transmit block is the daemon's, not the engine's
	}
	cfg := m.relayCfgCopy(relay)
	if cfg == nil {
		return ""
	}
	builder, err := protocol.Lookup(choice)
	if err != nil {
		return ""
	}
	cfg[attr] = value
	if err := builder.Check(cfg); err != nil {
		return otaErr(err)
	}
	return ""
}

// otaErr shortens a refusal for the air: the reference's ERR shape,
// the wrapper prefixes dropped, the tail cut — a reply is airtime,
// and the journal keeps the whole story for whoever needs it.
func otaErr(err error) string {
	msg := err.Error()
	// Wrapped chains read inward: the deepest segment names the
	// actual complaint, the wrappers name where it passed through.
	if i := strings.LastIndex(msg, ": "); i >= 0 && i+2 < len(msg) {
		if tail := msg[i+2:]; len(tail) > 12 {
			msg = tail
		}
	}
	msg = strings.TrimPrefix(msg, "meshcore params: ")
	const keep = 60
	if len(msg) > keep {
		msg = msg[:keep-1] + "…"
	}
	return "ERR: " + msg
}

// otaSetperm grants or revokes an admin permission — the reference's
// "setperm {pubkey-hex} {perms-int}", where a guest role (zero) means
// removal. An admin may hand another admin the role over the air,
// but never touches the credentials that grant it, which stay the
// console's: otaReadOnly already keeps admin_password out of set.
func (m *manager) otaSetperm(relay, principal, rest string) string {
	keyHex, permStr, ok := strings.Cut(rest, " ")
	if !ok {
		return "Err - bad params" // the reference's own words, all three
	}
	pub, err := hex.DecodeString(strings.TrimSpace(keyHex))
	if err != nil || len(pub) == 0 || len(pub) > 32 {
		return "Err - bad pubkey"
	}
	perms, err := strconv.Atoi(strings.TrimSpace(permStr))
	if err != nil || perms < 0 || perms > 255 {
		return "Err - bad params"
	}
	// The byte travels whole — a guest role meaning removal, which
	// alone may name its entry by prefix; a grant needs the whole
	// key, exactly the reference's applyPermissions split.
	if byte(perms)&enginemc.PermRoleMask != enginemc.PermGuest && len(pub) != 32 {
		return "Err - invalid params"
	}
	return m.orderAir(airOrder{
		relay: relay, principal: principal, grant: true, pubKey: pub, perms: byte(perms),
	}, "OK")
}

// otaDiscover keys a neighbourhood scan, refusing first what would
// be refused anyway. The reference never refuses its own discovers —
// it overwrites the pending tag and sends again — while this engine
// runs one window at a time, since two overlapping scans make every
// responder answer both into the same jitter. That deviation owes
// the admin a reason: the queued order's own refusal reaches the
// journal alone, and an OK the scan never honoured is worse than a
// plain no. Both reads below are lock-free, which is what lets them
// run in the engine's own goroutine.
func (m *manager) otaDiscover(relay string) string {
	m.viewMu.RLock()
	info, ok := m.infos[relay]
	m.viewMu.RUnlock()
	if !ok || info.Discover == nil {
		return "Err - this relay cannot scan"
	}
	if info.TXMode != config.TXOnAir && info.TXMode != config.TXOnAirZeroHop {
		return "Err - transmit gate is " + txModeWord(info.TXMode)
	}
	if info.ScanWindow != nil {
		if until, listening := info.ScanWindow(); listening {
			// The answers of the window already open join the same
			// neighbourhood, so the honest word is wait, not retry.
			return fmt.Sprintf("Err - scanning, %ds left",
				int(time.Until(until).Round(time.Second).Seconds()))
		}
	}
	return m.orderAir(airOrder{relay: relay, discover: true}, "OK - Discover sent")
}

// txModeWord names a gate for a refusal; empty reads as the dry
// posture it means.
func txModeWord(mode string) string {
	if mode == "" {
		return config.TXDry
	}
	return mode
}

// otaRemoveNeighbour drops neighbours by key prefix — and with no
// argument at all, drops them all: the wire's purge, which on the
// reference falls out of a zero-length prefix matching every entry.
// The table carries its own lock, so this needs no detour through
// the air channel.
func (m *manager) otaRemoveNeighbour(relay, rest string) string {
	prefix, err := hex.DecodeString(strings.TrimSpace(rest))
	if err != nil || len(prefix) > 32 {
		return "ERR: bad pubkey"
	}
	m.viewMu.RLock()
	info, ok := m.infos[relay]
	m.viewMu.RUnlock()
	if !ok || info.RemoveNeighbours == nil {
		return "ERR: no neighbour table"
	}
	info.RemoveNeighbours(prefix)
	return "OK"
}

// otaAdvert announces this node, flooded or to the neighbourhood.
func (m *manager) otaAdvert(relay string, flood bool) string {
	// Advert waits on the engine's own loop, so it cannot run in the
	// engine goroutine either: queued like a mutation, triggered off
	// it. The reply leaves first.
	ok := "OK - zerohop advert sent"
	if flood {
		ok = "OK - Advert sent"
	}
	return m.orderAir(airOrder{relay: relay, advert: true, flood: flood}, ok)
}
