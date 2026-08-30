//go:build !lean

package web

import (
	"context"
	"embed"
	"errors"
	"net"
	"net/http"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"meshrunner.dev/lotor/internal/bus"
)

// assets is the UI, whole: served from the binary so a relay carries
// its own interface and a deployment is one file.
//
//go:embed assets
var assets embed.FS

// shutdownGrace bounds how long open connections — the event streams,
// mostly — may hold the daemon's shutdown.
const shutdownGrace = 3 * time.Second

// ListenAndServe runs the web UI until the context ends. It holds the
// same discipline as the console listeners: the context closes the
// listener, open handlers get a bounded grace, and the caller's
// WaitGroup sees the whole thing out.
func ListenAndServe(ctx context.Context, addr string, deps Deps) error {
	log := deps.Log
	if log == nil {
		log = zap.NewNop()
	}
	ln, err := (&net.ListenConfig{}).Listen(ctx, "tcp", addr)
	if err != nil {
		return err
	}
	log.Info("web ui listening", zap.String("addr", addr))

	counters := newTally(ctx, deps.Bus)
	srv := &http.Server{
		Handler: newMux(deps, counters),
		// Requests inherit the daemon's context, so an event stream
		// ends when the daemon does — Shutdown's grace is real, not a
		// wait on clients that would never leave.
		BaseContext:       func(net.Listener) context.Context { return ctx },
		ReadHeaderTimeout: 10 * time.Second,
		// No WriteTimeout: the event stream writes for the life of
		// its client. The header timeout and the context bound what
		// can actually wedge.
		IdleTimeout: 2 * time.Minute,
	}
	//nolint:contextcheck // the parent ctx is DONE when this runs — the
	// grace deliberately lives on a fresh context, or it would be void.
	stop := context.AfterFunc(ctx, func() {
		sctx, cancel := context.WithTimeout(context.Background(), shutdownGrace)
		defer cancel()
		_ = srv.Shutdown(sctx)
	})
	defer stop()
	if err := srv.Serve(ln); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// newMux wires the four routes the scaffold serves. Everything is a
// GET; there is nothing to post to — the UI is read-only by design,
// not by omission.
func newMux(deps Deps, counters *tally) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFileFS(w, r, assets, "assets/index.html")
	})
	mux.Handle("GET /assets/", http.FileServerFS(assets))
	mux.HandleFunc("GET /api/status", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, snapshot(deps, counters))
	})
	mux.HandleFunc("GET /events", eventsHandler(deps, counters))
	return mux
}

// tally is the server's own count of what the mesh did while the UI
// watched: one bus subscription for the whole server, whatever the
// number of browsers.
type tally struct {
	heard, sent atomic.Uint64
}

// newTally subscribes and counts until the context ends. A nil bus —
// a bare test rig — counts nothing and stays at zero.
func newTally(ctx context.Context, b *bus.Bus) *tally {
	t := &tally{}
	if b == nil {
		return t
	}
	sub := b.Subscribe(256)
	go func() {
		defer sub.Close()
		for {
			select {
			case <-ctx.Done():
				return
			case ev, ok := <-sub.C:
				if !ok {
					return
				}
				switch ev.(type) {
				case bus.FrameHeard:
					t.heard.Add(1)
				case bus.FrameSent:
					t.sent.Add(1)
				}
			}
		}
	}()
	return t
}
