package cli

import (
	"bytes"
	"context"
	"io"
	"net"
	"strings"
	"testing"
	"testing/iotest"
	"time"
)

type terminalSession struct {
	peer  net.Conn
	out   *probeConn
	done  <-chan struct{}
	table *Sessions
}

func startTerminalSession(t *testing.T) terminalSession {
	t.Helper()
	server, peer := net.Pipe()
	ctx, cancel := context.WithCancel(context.Background())
	out := &probeConn{wrote: make(chan struct{}, 1)}
	done := make(chan struct{})
	deps := testDeps(t)
	deps.Sessions = NewSessions()
	go func() {
		defer close(done)
		ServeAuto(ctx, &sessionConn{Reader: &iacStripper{r: server}, Writer: out, conn: server}, deps)
	}()
	t.Cleanup(func() {
		cancel()
		_ = server.Close()
		_ = peer.Close()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("terminal session did not stop")
		}
	})
	if err := peer.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	return terminalSession{peer: peer, out: out, done: done, table: deps.Sessions}
}

func (s terminalSession) waitFor(t *testing.T, text string) {
	t.Helper()
	deadline := time.After(3 * time.Second)
	for !strings.Contains(s.out.transcript(), text) {
		select {
		case <-s.out.wrote:
		case <-deadline:
			t.Fatalf("waiting for %q in %q", text, s.out.transcript())
		}
	}
}

func (s terminalSession) send(t *testing.T, input string) {
	t.Helper()
	// One byte per Write pins fragmentation at every possible boundary.
	for i := range len(input) {
		if _, err := s.peer.Write([]byte{input[i]}); err != nil {
			t.Fatal(err)
		}
	}
}

func (s terminalSession) finish(t *testing.T) string {
	t.Helper()
	select {
	case <-s.done:
	case <-time.After(3 * time.Second):
		t.Fatal("terminal session did not finish")
	}
	out := s.out.transcript()
	if strings.Contains(out, "unknown command") {
		t.Fatalf("terminal input leaked into commands: %q", out)
	}
	if got := strings.Count(out, " console\r\n"); got != 1 {
		t.Fatalf("banner count = %d, want one: %q", got, out)
	}
	return out
}

func TestTerminalReportAfterGraceStillGetsEditor(t *testing.T) {
	s := startTerminalSession(t)
	// The greeting proves the grace expired; no scheduler-sensitive sleep.
	s.waitFor(t, "> ")
	s.send(t, "\x1b[30;1R")
	s.waitFor(t, "\x1b[9999C")
	s.send(t, "\x1b[30;100Rstatus\rjunk\x03\x04")
	out := s.finish(t)
	if !strings.Contains(out, "daemon") || !strings.Contains(out, "^C") {
		t.Fatalf("late terminal did not get character editing: %q", out)
	}
}

func TestTerminalTypingBeforeReportKeepsEveryKey(t *testing.T) {
	s := startTerminalSession(t)
	s.send(t, "sta\x1b[30;1R")
	s.waitFor(t, "\x1b[9999C")
	// Typing also races the width query; its late report must be consumed
	// by the editor after the measurement returned the first typed byte.
	s.send(t, "t\x1b[30;100Rus\rexit\r")
	out := s.finish(t)
	if !strings.Contains(out, "daemon") || !strings.Contains(out, "bye.") {
		t.Fatalf("early keystrokes were lost: %q", out)
	}
}

func TestTerminalFragmentedReportCrossesGrace(t *testing.T) {
	s := startTerminalSession(t)
	s.send(t, "\x1b[30;")
	s.waitFor(t, "> ")
	s.send(t, "1R")
	s.waitFor(t, "\x1b[9999C")
	s.send(t, "\x1b[30;100Rexit\r")
	s.finish(t)
}

func TestTerminalNAWSAfterGraceNeedsNoCursorAnswer(t *testing.T) {
	for _, width := range []int{0, 255, 120} {
		t.Run(stringWidthName(width), func(t *testing.T) {
			s := startTerminalSession(t)
			s.waitFor(t, "> ")
			var prelude bytes.Buffer
			if err := SendTerminalSize(&prelude, width, 30); err != nil {
				t.Fatal(err)
			}
			s.send(t, prelude.String())
			// A CPR already in flight is harmless after explicit metadata.
			s.send(t, "\x1b[30;1Rstatus\rjunk\x03\x04")
			out := s.finish(t)
			if strings.Contains(out, "\x1b[9999C") || !strings.Contains(out, "daemon") {
				t.Fatalf("NAWS terminal was probed or lost input: %q", out)
			}
		})
	}
}

func stringWidthName(width int) string {
	if width == 0 {
		return "unknown width"
	}
	if width == 255 {
		return "escaped IAC"
	}
	return "normal width"
}

func TestTerminalPlainScriptStartsOnItsFirstLine(t *testing.T) {
	s := startTerminalSession(t)
	s.send(t, "status\n")
	// The pipe remains open: selection cannot wait for EOF or a report.
	s.waitFor(t, "daemon")
	s.send(t, "exit\n")
	out := s.finish(t)
	if strings.Contains(out, "\x1b[K") || strings.Contains(out, cPath) {
		t.Fatalf("a script received the editor: %q", out)
	}
}

func TestTerminalSelectionPreservesFinalLineAndNonReports(t *testing.T) {
	for _, input := range []string{"status", "\x1b[Astatus\n", "\x1b[Rstatus\n", "\x1b[1;;2Rstatus\n", "ok\x1b"} {
		terminal, _, got := awaitTerminal(context.Background(), iotest.OneByteReader(strings.NewReader(input)), nil)
		if terminal || string(got) != input {
			t.Errorf("input %q: terminal=%v, replay=%q", input, terminal, got)
		}
	}
}

func TestTerminalSelectionCancellationWakesIdleRead(t *testing.T) {
	server, peer := net.Pipe()
	defer func() { _ = server.Close() }()
	defer func() { _ = peer.Close() }()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out := &probeConn{wrote: make(chan struct{}, 1)}
	done := make(chan struct{})
	deps := testDeps(t)
	go func() {
		defer close(done)
		ServeAuto(ctx, &sessionConn{Reader: &iacStripper{r: server}, Writer: out, conn: server}, deps)
	}()
	s := terminalSession{peer: peer, out: out, done: done}
	s.waitFor(t, "> ")
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("cancellation left the mode-selection read blocked")
	}
}

func TestTerminalNAWSCodecBoundsAndStripsMetadata(t *testing.T) {
	for _, width := range []int{-1, 0, 80, 255, 65535, 100000} {
		var wire bytes.Buffer
		if err := SendTerminalSize(&wire, width, 255); err != nil {
			t.Fatal(err)
		}
		wire.WriteString("status\n")
		strip := &iacStripper{r: iotest.OneByteReader(&wire)}
		got, err := io.ReadAll(strip)
		if err != nil || string(got) != "status\n" || !strip.hasSize || strip.width != max(0, min(width, 65535)) {
			t.Fatalf("width %d: got %q, size %d/%v, error %v", width, got, strip.width, strip.hasSize, err)
		}
	}
	// Unterminated or overlong metadata is never a terminal declaration.
	for _, body := range [][]byte{{optNAWS, 0, 80, 0}, {optNAWS, 0, 80, 0, 24, 0}} {
		wire := append([]byte{iacByte, iacSubBg}, body...)
		wire = append(wire, iacByte, iacSubEn, 'x')
		strip := &iacStripper{r: bytes.NewReader(wire)}
		got, err := io.ReadAll(strip)
		if err != nil || string(got) != "x" || strip.hasSize {
			t.Fatalf("invalid NAWS accepted: %v, %q, %v", strip.hasSize, got, err)
		}
	}
}

func TestTerminalNegotiationIsRegisteredBeforeInputAndGetsFarewell(t *testing.T) {
	s := startTerminalSession(t)
	s.waitFor(t, "> ")
	before := s.table.snapshot()
	peer, ok := before["1"]
	if len(before) != 1 || !ok || peer.id != "1" || peer.began.IsZero() {
		t.Fatal("idle negotiation was not fully registered")
	}
	began := peer.began
	s.table.Farewell("test farewell")
	s.waitFor(t, "test farewell")
	var prelude bytes.Buffer
	if err := SendTerminalSize(&prelude, 100, 30); err != nil {
		t.Fatal(err)
	}
	s.send(t, prelude.String()+"status\r")
	s.waitFor(t, "daemon")
	after := s.table.snapshot()
	if len(after) != 1 || after["1"] != peer || peer.began != began || !peer.hasTerminal() {
		t.Fatal("negotiation replaced or registered the session twice")
	}
	s.send(t, "exit\r")
	s.finish(t)
	if len(s.table.snapshot()) != 0 {
		t.Fatal("negotiated session remained registered after exit")
	}
}

func TestTerminalNAWSUsesTheRFC1073ByteOrder(t *testing.T) {
	var wire bytes.Buffer
	if err := SendTerminalSize(&wire, 300, 255); err != nil {
		t.Fatal(err)
	}
	want := []byte{255, 251, 31, 255, 250, 31, 1, 44, 0, 255, 255, 255, 240}
	if !bytes.Equal(wire.Bytes(), want) {
		t.Fatalf("NAWS bytes = %v, want %v", wire.Bytes(), want)
	}
}

func TestTerminalWidthReplyAfterTimeoutStaysOutOfCommands(t *testing.T) {
	for _, partial := range []bool{false, true} {
		name := "whole reply late"
		if partial {
			name = "reply split by timeout"
		}
		t.Run(name, func(t *testing.T) {
			s := startTerminalSession(t)
			s.send(t, "\x1b[30;1R")
			s.waitFor(t, "\x1b[9999C")
			report := "\x1b[30;100R"
			if partial {
				s.send(t, "\x1b[30;")
				report = "100R"
			}
			// The CR proves the failed width query restored the cursor.
			s.waitFor(t, "\x1b[9999C\x1b[6n\r")
			s.send(t, report+"status\rexit\r")
			out := s.finish(t)
			if !strings.Contains(out, "daemon") {
				t.Fatalf("late width reply consumed the command: %q", out)
			}
		})
	}
}

func TestTerminalPlainEOFDeliversTheLastCommand(t *testing.T) {
	s := startTerminalSession(t)
	s.send(t, "status")
	if err := s.peer.Close(); err != nil {
		t.Fatal(err)
	}
	out := s.finish(t)
	if !strings.Contains(out, "daemon") || strings.Contains(out, "\x1b[K") {
		t.Fatalf("plain EOF lost or edited the last command: %q", out)
	}
}
