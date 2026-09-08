package cli

import (
	"context"
	"fmt"
	"io"
	"regexp"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"meshrunner.dev/lotor/internal/bus"
)

type displayTest struct {
	session *session
	input   *io.PipeWriter
	out     *probeConn
	done    <-chan struct{}
}

func startDisplayTest(t *testing.T, deps Deps) displayTest {
	t.Helper()
	r, w := io.Pipe()
	out := &probeConn{wrote: make(chan struct{}, 1)}
	s := &session{deps: deps, out: syncOut(out), colors: true}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.serveEdited(ctx, r, 80)
	}()
	t.Cleanup(func() {
		cancel()
		_ = r.Close()
		_ = w.Close()
		<-done
	})
	synctest.Wait()
	return displayTest{session: s, input: w, out: out, done: done}
}

func (d displayTest) typeKeys(t *testing.T, keys string) {
	t.Helper()
	if _, err := io.WriteString(d.input, keys); err != nil {
		t.Fatal(err)
	}
	synctest.Wait()
}

// Display transcripts contain cursor movement as well as SGR; erase-line
// sequences must not consume the next prompt while removing decorations.
func displayText(text string) string {
	return regexp.MustCompile(`\x1b\[[0-?]*[ -/]*[@-~]`).ReplaceAllString(text, "")
}

func displayDeps() Deps {
	return Deps{Version: "test", Started: time.Now(), Kinds: testKinds(),
		Bus: bus.New(), Sessions: NewSessions(),
		SystemName: func() string { return "test" },
		Relays:     []RelayInfo{{Name: "r", Protocol: "meshcore", State: func() string { return "running" }}},
	}
}

func TestDisplayBuffersTypeaheadUntilCommandOutputEnds(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		resume := sync.OnceFunc(func() { close(release) })
		defer resume()
		deps := displayDeps()
		deps.LiveRelays = func() []RelayInfo { <-release; return nil }
		d := startDisplayTest(t, deps)
		d.typeKeys(t, "status\r")
		before := d.out.transcript()
		d.typeKeys(t, "q")
		if got := d.out.transcript(); got != before {
			t.Fatalf("typeahead painted over the running command: %q", got[len(before):])
		}
		resume()
		synctest.Wait()
		got := displayText(d.out.transcript())
		if !strings.Contains(got, "daemon") || !strings.HasSuffix(got, "> q") {
			t.Fatalf("command output did not precede the retained draft: %q", got)
		}
		d.typeKeys(t, "uit\r")
		<-d.done
	})
}

func TestDisplayEchoesQueuedNavigationInItsOriginalContext(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		resume := sync.OnceFunc(func() { close(release) })
		defer resume()
		deps := displayDeps()
		deps.LiveRelays = func() []RelayInfo { <-release; return deps.Relays }
		d := startDisplayTest(t, deps)
		d.typeKeys(t, "status\r")
		d.typeKeys(t, "/relay/r\rprint\rquit\r")
		resume()
		synctest.Wait()
		<-d.done
		got := displayText(d.out.transcript())
		if !strings.Contains(got, "[read-only@test] > /relay/r\r\n") ||
			!strings.Contains(got, "[read-only@test] /relay/r> print\r\n") {
			t.Fatalf("queued commands used the wrong prompt context: %q", got)
		}
	})
}

func TestDisplayWatchStillAcceptsControlC(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := startDisplayTest(t, displayDeps())
		d.typeKeys(t, "frames watch\r")
		if !d.session.isWatching() {
			t.Fatal("watch never began")
		}
		d.typeKeys(t, "abandoned\x03")
		if d.session.isWatching() {
			t.Fatal("Ctrl+C could not end the watch")
		}
		if strings.Contains(d.out.transcript(), "unknown command") {
			t.Fatal("cancelled typeahead was executed")
		}
		d.typeKeys(t, "quit\r")
		<-d.done
	})
}

func TestDisplayDrainsQueuedAndUnterminatedLinesAtEOF(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		resume := sync.OnceFunc(func() { close(release) })
		defer resume()
		deps := displayDeps()
		deps.LiveRelays = func() []RelayInfo { <-release; return nil }
		d := startDisplayTest(t, deps)
		d.typeKeys(t, strings.Join([]string{"status", "status"}, "\r"))
		if err := d.input.Close(); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		resume()
		synctest.Wait()
		<-d.done
		if got := strings.Count(d.out.transcript(), "\r\ndaemon"); got != 2 {
			t.Fatalf("executed %d statuses at EOF, want 2", got)
		}
	})
}

func TestDisplayFarewellBypassesAnActiveCapture(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := startDisplayTest(t, displayDeps())
		d.typeKeys(t, strings.Repeat("x", 100))
		release := make(chan struct{})
		resume := sync.OnceFunc(func() { close(release) })
		defer resume()
		captured := make(chan string, 1)
		go func() {
			body, err := d.session.capture(func() error {
				fmt.Fprint(d.session.out, "frame ")
				<-release
				fmt.Fprint(d.session.out, "tail")
				return nil
			})
			if err != nil {
				body = err.Error()
			}
			captured <- body
		}()
		synctest.Wait()
		d.session.deps.Sessions.Farewell("leaving now")
		synctest.Wait()
		resume()
		if got := <-captured; got != "frame tail" {
			t.Fatalf("farewell leaked into command capture: %q", got)
		}
		<-d.done
		if got := d.out.transcript(); !strings.HasSuffix(got, "leaving now\r\n") {
			t.Fatalf("farewell did not reach the terminal: %q", got)
		}
	})
}

func TestAutoDisplayRepaintsOnMetadataOnlyResize(t *testing.T) {
	d := startTerminalSession(t)
	if err := SendTerminalSize(d.peer, 80, 24); err != nil {
		t.Fatal(err)
	}
	d.waitFor(t, "> ")
	d.send(t, strings.Repeat("x", 90))
	if err := SendTerminalSize(d.peer, 30, 4); err != nil {
		t.Fatal(err)
	}
	// A full 30-cell row cannot come from the prior 80-column draft.
	d.waitFor(t, strings.Repeat("x", 30)+"\r\n")
	d.send(t, "\x03\x04")
	d.finish(t)
}

func TestDisplayEchoesTheCommandThatEndsAWatch(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := startDisplayTest(t, displayDeps())
		d.typeKeys(t, "frames watch\r")
		d.typeKeys(t, "status\r")
		got := displayText(d.out.transcript())
		echo := strings.Index(got, "[read-only@test] > status\r\n")
		result := strings.Index(got, "\r\ndaemon")
		if echo < 0 || result < echo {
			t.Fatalf("watch lost its terminating command echo: %q", got)
		}
		d.typeKeys(t, "quit\r")
		<-d.done
	})
}
