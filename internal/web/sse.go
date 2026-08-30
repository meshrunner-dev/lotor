//go:build !lean

package web

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"meshrunner.dev/lotor/internal/bus"
)

// The event stream's cadence: a steady tick so counters and uptime
// move, an immediate wake on state changes so a relay's death shows
// when it happens — coalesced, because the browser may sit on the far
// end of the very radio link the relay serves.
const (
	sseTick     = 2 * time.Second
	sseCoalesce = 500 * time.Millisecond
)

// eventsHandler streams status snapshots as server-sent events. Each
// client costs one bus subscription, closed when the client leaves;
// the counters stay the server's, shared.
func eventsHandler(deps Deps, counters *tally) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		fl, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming unsupported", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("Connection", "keep-alive")

		var wake <-chan bus.Event
		if deps.Bus != nil {
			sub := deps.Bus.Subscribe(64)
			defer sub.Close()
			wake = sub.C
		}
		send := func() {
			data, err := json.Marshal(snapshot(deps, counters))
			if err != nil {
				return
			}
			fmt.Fprintf(w, "event: status\ndata: %s\n\n", data)
			fl.Flush()
		}
		send()
		last := time.Now()
		tick := time.NewTicker(sseTick)
		defer tick.Stop()
		// pending holds a coalesced send: armed when a state change
		// lands inside the quiet window, so a flap burst becomes one
		// refresh instead of a frame per transition.
		pending := time.NewTimer(time.Hour)
		pending.Stop()
		defer pending.Stop()
		for {
			select {
			case <-r.Context().Done():
				return
			case ev, ok := <-wake:
				if !ok {
					return
				}
				if !stateChange(ev) {
					continue
				}
				if since := time.Since(last); since >= sseCoalesce {
					send()
					last = time.Now()
				} else {
					pending.Reset(sseCoalesce - since)
				}
			case <-pending.C:
				send()
				last = time.Now()
			case <-tick.C:
				send()
				last = time.Now()
			}
		}
	}
}

// stateChange says whether an event is worth an immediate refresh —
// lifecycle transitions, not the frame traffic the tick already
// summarises through the counters.
func stateChange(ev bus.Event) bool {
	switch ev.(type) {
	case bus.RelayState, bus.ObserverState:
		return true
	}
	return false
}
