package main

// The wire vocabulary of over-the-air administration. The words are
// the ecosystem's — the companion apps send what the reference's
// CommonCLI accepts, and a repeater that answered a private dialect
// would simply not be administrable from the field. What each word
// does, though, goes through this daemon's own mutation door: the
// same validation, the same journal, the same bounce, with the
// principal saying the change came from the air.

import (
	"context"
	"encoding/hex"
	"fmt"
	"strings"
	"time"
)

// otaSetting maps one CommonCLI variable onto the attribute it means
// here. A word absent from this table is not administrable over the
// air, and says so.
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

// otaReadOnly are the words a companion may read but never write from
// the air: the credentials that grant a role, and the identity that
// is the node. Changing those is the console's alone — an admin who
// could rewrite the admin word could lock the owner out of their own
// repeater with one packet.
var otaReadOnly = map[string]bool{
	"admin_password": true,
	"identity":       true,
}

// otaCommands runs one administration line from the air and returns
// what to answer. It is the engine's commands hook.
func (m *manager) otaCommands(ctx context.Context, relay string) func(line string, admin []byte) string {
	return func(line string, admin []byte) string {
		principal := "air:" + hex.EncodeToString(admin[:6])
		return m.runOTA(ctx, relay, principal, line)
	}
}

// runOTA dispatches one line. Every answer is a single short string:
// the reference's clients show it as a message.
func (m *manager) runOTA(ctx context.Context, relay, principal, line string) string {
	verb, rest, _ := strings.Cut(line, " ")
	rest = strings.TrimSpace(rest)
	switch verb {
	case "get":
		return m.otaGet(relay, rest)
	case "set":
		return m.otaSet(ctx, relay, principal, rest)
	case "advert":
		return m.otaAdvert(relay, true)
	case "advert.zerohop":
		return m.otaAdvert(relay, false)
	case "ver":
		return "lotor " + version
	case "clock":
		return time.Now().UTC().Format("2006-01-02 15:04:05") + " UTC"
	case "":
		return ""
	default:
		return "unknown command: " + verb
	}
}

// otaGet reads one setting back, or the node's own summary.
func (m *manager) otaGet(relay, name string) string {
	if name == "" {
		return "get what? try: get name"
	}
	attr, known := otaSetting[name]
	if !known {
		return "unknown setting: " + name
	}
	m.mu.Lock()
	traces := m.traces["relay "+relay]
	m.mu.Unlock()
	for _, t := range traces {
		if t.Key == attr {
			return fmt.Sprintf("%s: %v", name, t.Value)
		}
	}
	return name + ": unset"
}

// otaSet applies one setting through the mutation door.
func (m *manager) otaSet(ctx context.Context, relay, principal, rest string) string {
	name, value, ok := strings.Cut(rest, " ")
	if !ok || strings.TrimSpace(value) == "" {
		return "set what to what? try: set name Raccoon City"
	}
	attr, known := otaSetting[name]
	if !known {
		return "unknown setting: " + name
	}
	if otaReadOnly[attr] {
		return name + " is set from the console only"
	}
	value = strings.TrimSpace(value)
	if attr == "tx.mode" {
		// repeat on/off is the reference's word for the transmit gate.
		switch value {
		case "on":
			value = "on-air"
		case "off":
			value = "on-air-zero-hop"
		default:
			return "repeat wants on or off"
		}
	}
	msg, err := m.Mutate(ctx, "relay", relay,
		map[string]string{attr: value}, nil, principal)
	if err != nil {
		return "refused: " + err.Error()
	}
	return msg
}

// otaAdvert announces this node, flooded or to the neighbourhood.
func (m *manager) otaAdvert(relay string, flood bool) string {
	m.mu.Lock()
	info, ok := m.infos[relay]
	m.mu.Unlock()
	if !ok || info.TriggerAdvert == nil {
		return "this relay cannot advertise"
	}
	if err := info.TriggerAdvert(flood); err != nil {
		return "refused: " + err.Error()
	}
	if flood {
		return "advert flooded"
	}
	return "advert sent to the neighbourhood"
}
