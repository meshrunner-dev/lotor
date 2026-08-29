// Package mqtt publishes what a relay hears and sends to MQTT
// brokers, in the observer ecosystem's own JSON — the de-facto
// contract its analyzers and maps already consume. The schemas here
// are measured from the reference implementation, quirks included:
// several numbers travel as strings, some fields ride one direction
// only, and a consumer must not be able to tell lotor from the
// firmwares it already ingests. docs/mqtt-observer.md is the pinned
// contract this package answers to.
package mqtt

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"meshrunner.dev/pkg/meshcore"
)

// packetMessage is one frame, analysed — the packets topic.
type packetMessage struct {
	Timestamp  string   `json:"timestamp"`
	Hash       string   `json:"hash"`
	Origin     string   `json:"origin"`
	Type       string   `json:"type"`
	Direction  string   `json:"direction"`
	Time       string   `json:"time"`
	Date       string   `json:"date"`
	Len        string   `json:"len"`
	PacketType string   `json:"packet_type"`
	Route      string   `json:"route"`
	PayloadLen string   `json:"payload_len"`
	Raw        string   `json:"raw"`
	OriginID   string   `json:"origin_id"`
	SNR        string   `json:"SNR,omitempty"`
	RSSI       string   `json:"RSSI,omitempty"`
	Path       []string `json:"path,omitempty"`
}

// statusMessage is the periodic heartbeat — the status topic.
type statusMessage struct {
	Status          string       `json:"status"`
	Timestamp       string       `json:"timestamp"`
	Origin          string       `json:"origin"`
	OriginID        string       `json:"origin_id"`
	Model           string       `json:"model"`
	FirmwareVersion string       `json:"firmware_version"`
	Radio           string       `json:"radio"`
	ClientVersion   string       `json:"client_version"`
	Repeat          string       `json:"repeat,omitempty"`
	Stats           *statusStats `json:"stats,omitempty"`
}

type statusStats struct {
	UptimeSecs      *int  `json:"uptime_secs,omitempty"`
	PacketsSent     *int  `json:"packets_sent,omitempty"`
	PacketsReceived *int  `json:"packets_received,omitempty"`
	Errors          *int  `json:"errors,omitempty"`
	NoiseFloor      *int  `json:"noise_floor,omitempty"`
	TxAirSecs       *int  `json:"tx_air_secs,omitempty"`
	RxAirSecs       *int  `json:"rx_air_secs,omitempty"`
	RecvErrors      *int  `json:"recv_errors,omitempty"`
	JournalDegraded *bool `json:"journal_degraded,omitempty"`
}

// rawMessage is the frame without the analysis — the raw topic.
type rawMessage struct {
	Origin    string `json:"origin"`
	OriginID  string `json:"origin_id"`
	Timestamp string `json:"timestamp"`
	Type      string `json:"type"`
	Data      string `json:"data"`
}

// isoTimestamp renders the instant the way every observer message
// stamps itself: UTC, microseconds, an explicit +00:00.
func isoTimestamp(at time.Time) string {
	return at.UTC().Format("2006-01-02T15:04:05.000000+00:00")
}

// PacketJSON builds the packets-topic message for one frame. rx says
// which direction it travelled; SNR and RSSI ride only received
// frames, per the contract.
func PacketJSON(raw []byte, pkt *meshcore.Packet, at time.Time,
	rx bool, origin, originID string, snr, rssi float64,
) ([]byte, error) {
	utc := at.UTC()
	m := packetMessage{
		Timestamp:  isoTimestamp(at),
		Hash:       hashHex(pkt),
		Origin:     origin,
		Type:       "PACKET",
		Direction:  "tx",
		Time:       utc.Format("15:04:05"),
		Date:       utc.Format("02/01/2006"),
		Len:        strconv.Itoa(len(raw)),
		PacketType: strconv.Itoa(int(pkt.PayloadType())),
		Route:      routeLetter(pkt),
		PayloadLen: strconv.Itoa(len(pkt.Payload)),
		Raw:        hex.EncodeToString(raw),
		OriginID:   originID,
	}
	if rx {
		m.Direction = "rx"
		m.SNR = fmt.Sprintf("%.1f", snr)
		m.RSSI = strconv.Itoa(int(rssi))
	}
	if pkt.IsRouteDirect() && pkt.PathHashCount() > 0 {
		m.Path = pathTokens(pkt)
	}
	return json.Marshal(m)
}

// routeLetter is the contract's two-valued route: the reference
// collapses the transport variants into their plain direction.
func routeLetter(pkt *meshcore.Packet) string {
	if pkt.IsRouteDirect() {
		return "D"
	}
	return "F"
}

// hashHex is the packet hash the ecosystem correlates frames by.
func hashHex(pkt *meshcore.Packet) string {
	h := pkt.Hash()
	return hex.EncodeToString(h[:])
}

// pathTokens renders a direct packet's remaining hops, one hex token
// per hop at the packet's own hash width.
func pathTokens(pkt *meshcore.Packet) []string {
	size, count := pkt.PathHashSize(), pkt.PathHashCount()
	tokens := make([]string, 0, count)
	for i := range count {
		lo := i * size
		hi := min(lo+size, len(pkt.Path))
		if lo >= hi {
			break
		}
		tokens = append(tokens, hex.EncodeToString(pkt.Path[lo:hi]))
	}
	return tokens
}

// Health is what the status message may say about the node; nil
// pointers are omitted, matching the reference's "-1 means unknown".
type Health struct {
	Repeat          string // "on" | "off" | "" to omit
	UptimeSecs      *int
	PacketsSent     *int
	PacketsReceived *int
	Errors          *int
	NoiseFloor      *int
	TxAirSecs       *int
	RxAirSecs       *int
	RecvErrors      *int
	// JournalDegraded says the archive is failing its writes — an
	// outage the heartbeat must carry, because after a log rotation
	// nothing else durable says it happened.
	JournalDegraded *bool
}

// StatusJSON builds the heartbeat.
func StatusJSON(at time.Time, origin, originID, model, firmware, radio,
	client string, h Health,
) ([]byte, error) {
	m := statusMessage{
		Status:          "online",
		Timestamp:       isoTimestamp(at),
		Origin:          origin,
		OriginID:        originID,
		Model:           model,
		FirmwareVersion: firmware,
		Radio:           radio,
		ClientVersion:   client,
		Repeat:          h.Repeat,
	}
	stats := statusStats{
		UptimeSecs: h.UptimeSecs, PacketsSent: h.PacketsSent,
		PacketsReceived: h.PacketsReceived, Errors: h.Errors,
		NoiseFloor: h.NoiseFloor, TxAirSecs: h.TxAirSecs,
		RxAirSecs: h.RxAirSecs, RecvErrors: h.RecvErrors,
		JournalDegraded: h.JournalDegraded,
	}
	if stats != (statusStats{}) {
		m.Stats = &stats
	}
	return json.Marshal(m)
}

// RadioString renders the status message's radio field the way the
// contract spells it: MHz to six decimals, kHz to one, then the
// spreading factor and coding rate.
func RadioString(freqHz uint32, bandwidthHz uint32, sf, cr int) string {
	return fmt.Sprintf("%.6f,%.1f,%d,%d",
		float64(freqHz)/1e6, float64(bandwidthHz)/1e3, sf, cr)
}

// RawJSON builds the raw-topic message.
func RawJSON(raw []byte, at time.Time, origin, originID string) ([]byte, error) {
	return json.Marshal(rawMessage{
		Origin:    origin,
		OriginID:  originID,
		Timestamp: isoTimestamp(at),
		Type:      "RAW",
		Data:      hex.EncodeToString(raw),
	})
}

// NeighborEntry is one neighbour as the neighbourhood message shows
// it: the passive facts, and the outcome of asking it for its scopes.
type NeighborEntry struct {
	PubKey       string
	SNR          float64
	HeardSecsAgo int
	HeardUnknown bool
	Regions      string
	// Status is the regions question's outcome: responded, timeout, or
	// send_failed — the ecosystem's own vocabulary.
	Status string
}

type neighborsMessage struct {
	Timestamp        string          `json:"timestamp"`
	Origin           string          `json:"origin"`
	OriginID         string          `json:"origin_id"`
	TotalNeighbors   int             `json:"total_neighbors"`
	QueriedNeighbors int             `json:"queried_neighbors"`
	Truncated        bool            `json:"truncated"`
	Self             neighborsSelf   `json:"self"`
	Neighbors        []neighborEntry `json:"neighbors"`
}

type neighborsSelf struct {
	Regions       string `json:"regions"`
	DefaultRegion string `json:"default_region"`
	// The scope spellings are deprecated duplicates, kept one release
	// for consumers mid-migration; same values.
	Scopes       string `json:"scopes"`
	DefaultScope string `json:"default_scope"`
}

type neighborEntry struct {
	PubKey       string  `json:"pubkey"`
	SNR          float64 `json:"snr"`
	HeardSecsAgo *int    `json:"heard_secs_ago"`
	Regions      string  `json:"regions"`
	// Scopes is the deprecated duplicate of Regions, kept one release.
	Scopes string `json:"scopes"`
	Status string `json:"status"`
}

// NeighborsJSON builds the neighbourhood message. queried counts the
// entries actually asked this round; an unknown age travels as null,
// not as zero.
func NeighborsJSON(at time.Time, origin, originID, selfRegions, defaultRegion string,
	entries []NeighborEntry, queried int,
) ([]byte, error) {
	m := neighborsMessage{
		Timestamp:        isoTimestamp(at),
		Origin:           origin,
		OriginID:         originID,
		TotalNeighbors:   len(entries),
		QueriedNeighbors: queried,
		Self: neighborsSelf{
			Regions: selfRegions, DefaultRegion: defaultRegion,
			Scopes: selfRegions, DefaultScope: defaultRegion,
		},
		Neighbors: make([]neighborEntry, 0, len(entries)),
	}
	for _, e := range entries {
		out := neighborEntry{
			PubKey: e.PubKey, SNR: e.SNR,
			Regions: e.Regions, Scopes: e.Regions, Status: e.Status,
		}
		if !e.HeardUnknown {
			age := e.HeardSecsAgo
			out.HeardSecsAgo = &age
		}
		m.Neighbors = append(m.Neighbors, out)
	}
	return json.Marshal(m)
}
