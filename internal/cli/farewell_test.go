package cli

import (
	"bytes"
	"strings"
	"sync"
	"testing"
)

func TestFarewellReachesEverySessionAndEndsItsLine(t *testing.T) {
	// The daemon says it while the sockets are still open. A terminal
	// gets its prompt taken back; a transcript gets a clean break.
	// Both end at column zero, so the shell that gets the terminal
	// back starts where it should.
	table := NewSessions()
	var term, plain bytes.Buffer
	termSession := &session{deps: Deps{Sessions: table}, out: syncOut(&term), colors: true}
	plainSession := &session{deps: Deps{Sessions: table}, out: syncOut(&plain)}
	defer termSession.register()()
	defer plainSession.register()()

	table.Farewell("Lotor is shutting down — this console is closing")

	if got := term.String(); got != "\r\x1b[KLotor is shutting down — this console is closing\r\n" {
		t.Errorf("terminal farewell = %q", got)
	}
	if got := plain.String(); got != "\r\nLotor is shutting down — this console is closing\r\n" {
		t.Errorf("plain farewell = %q", got)
	}
	for what, got := range map[string]string{"terminal": term.String(), "plain": plain.String()} {
		if !strings.HasSuffix(got, "\r\n") {
			t.Errorf("the %s session was left mid-line: %q", what, got)
		}
	}
}

func TestFarewellWithoutATableSaysNothing(t *testing.T) {
	// A daemon that keeps no session table has nobody to tell, and
	// must not die trying on its way out.
	var nilTable *Sessions
	nilTable.Farewell("going down")
}

func TestASessionsWritesAreSerialised(t *testing.T) {
	// The farewell arrives from the shutdown path while the REPL may
	// still be writing. Under -race this is the assertion; the
	// interleaving is what it guards against.
	var out bytes.Buffer
	w := syncOut(&out)
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 32 {
				_, _ = w.Write([]byte("0123456789abcdef"))
			}
		})
	}
	wg.Wait()
	if out.Len() != 8*32*16 {
		t.Errorf("wrote %d bytes, want %d", out.Len(), 8*32*16)
	}
}
