//go:build !lean

package web

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"meshrunner.dev/lotor/internal/cli"
	"meshrunner.dev/lotor/internal/product"
)

// status is the one snapshot both transports speak: GET /api/status
// answers it whole, and the event stream pushes the same shape — the
// polling fallback degrades the cadence, never the content.
type status struct {
	Product     string           `json:"product"`
	Version     string           `json:"version"`
	Revision    string           `json:"revision,omitempty"`
	System      string           `json:"system"`
	UptimeSecs  int64            `json:"uptimeSecs"`
	FramesHeard uint64           `json:"framesHeard"`
	FramesSent  uint64           `json:"framesSent"`
	Relays      []relayStatus    `json:"relays"`
	Observers   []observerStatus `json:"observers"`
	Journal     *journalStatus   `json:"journal,omitempty"`
}

type relayStatus struct {
	Name     string `json:"name"`
	Protocol string `json:"protocol"`
	Radio    string `json:"radio"`
	State    string `json:"state"`
	Cause    string `json:"cause,omitempty"`
	Waveform string `json:"waveform,omitempty"`
}

type observerStatus struct {
	Name  string `json:"name"`
	URL   string `json:"url"`
	State string `json:"state"`
	Cause string `json:"cause,omitempty"`
}

type journalStatus struct {
	Healthy  bool   `json:"healthy"`
	Writes   uint64 `json:"writes"`
	Failures uint64 `json:"failures"`
	LastErr  string `json:"lastError,omitempty"`
}

// snapshot reads the live views once and renders them whole.
func snapshot(deps Deps, counters *tally) status {
	s := status{
		Product:     product.Name,
		Version:     deps.Version,
		Revision:    deps.Revision,
		System:      systemName(deps),
		UptimeSecs:  int64(time.Since(deps.Started).Seconds()),
		FramesHeard: counters.heard.Load(),
		FramesSent:  counters.sent.Load(),
		Relays:      []relayStatus{},
		Observers:   []observerStatus{},
	}
	if deps.LiveRelays != nil {
		for _, r := range deps.LiveRelays() {
			row := relayStatus{Name: r.Name, Protocol: r.Protocol, Radio: r.Radio}
			if r.State != nil {
				row.State = r.State()
			}
			if r.Err != nil {
				row.Cause = r.Err()
			}
			if wf := r.Waveform; wf.FrequencyHz > 0 {
				row.Waveform = fmt.Sprintf("%.3f MHz sf%d",
					float64(wf.FrequencyHz)/1e6, wf.SpreadingFactor)
			}
			s.Relays = append(s.Relays, row)
		}
	}
	if deps.LiveMQTTs != nil {
		for _, mq := range deps.LiveMQTTs() {
			s.Observers = append(s.Observers, observerStatus{
				Name: mq.Name, URL: mq.URL,
				State: observerWord(mq), Cause: mq.Down,
			})
		}
	}
	if deps.Health != nil {
		h := deps.Health()
		s.Journal = &journalStatus{
			Healthy: h.Healthy, Writes: h.Writes,
			Failures: h.Failures, LastErr: h.LastErr,
		}
	}
	return s
}

// observerWord names one observer's condition the way the operator
// reads it: parked on purpose, down with a cause, or running with the
// broker session's own word.
func observerWord(mq cli.MQTTInfo) string {
	switch {
	case mq.Disabled:
		return "disabled"
	case mq.Down != "":
		return "down"
	case mq.Connected != nil && mq.Connected():
		return "connected"
	default:
		return "connecting"
	}
}

// systemName is what the page header says, with the same fallback the
// console prompt uses.
func systemName(deps Deps) string {
	if deps.SystemName != nil {
		if name := deps.SystemName(); name != "" {
			return name
		}
	}
	return product.Slug
}

// writeJSON answers one request with one snapshot. An encode error
// here is the client hanging up mid-body: there is no one left to
// tell, and the snapshot itself is plain data that always marshals.
func writeJSON(w http.ResponseWriter, s status) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	//nolint:errchkjson // plain data; a failure is the peer's hangup
	_ = json.NewEncoder(w).Encode(s)
}
