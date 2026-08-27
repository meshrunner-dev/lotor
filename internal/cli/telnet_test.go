package cli

import (
	"bytes"
	"context"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

func dialTestServer(t *testing.T) *net.TCPConn {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	deps := testDeps(t)
	go func() { _ = ServeListener(ctx, ln, deps) }()

	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	tc, ok := conn.(*net.TCPConn)
	if !ok {
		t.Fatalf("conn type %T", conn)
	}
	t.Cleanup(func() { _ = tc.Close() })
	_ = tc.SetDeadline(time.Now().Add(5 * time.Second))
	return tc
}

// The daemon must close the connection right after quit: a lingering
// socket is what left netcat waiting for one more keystroke.
func TestQuitClosesTheConnection(t *testing.T) {
	conn := dialTestServer(t)
	if _, err := conn.Write([]byte("quit\r\n")); err != nil {
		t.Fatal(err)
	}
	all, err := io.ReadAll(conn) // returns only when the server closes
	if err != nil {
		t.Fatalf("connection not closed cleanly: %v", err)
	}
	if !strings.Contains(string(all), "bye.") {
		t.Errorf("no goodbye before close:\n%s", all)
	}
}

func TestEscapeIACDoublesAndStripRestores(t *testing.T) {
	var wire bytes.Buffer
	payload := []byte{'a', 255, 'b', 255, 255, 'c'}
	if _, err := EscapeIAC(&wire).Write(payload); err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(StripIAC(&wire))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("round trip = %v, want %v", got, payload)
	}
}

// A client half-close (Ctrl+D through a real client) must end the
// session the same way.
func TestClientEOFEndsTheSession(t *testing.T) {
	conn := dialTestServer(t)
	if _, err := conn.Write([]byte("status\r\n")); err != nil {
		t.Fatal(err)
	}
	if err := conn.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	all, err := io.ReadAll(conn)
	if err != nil {
		t.Fatalf("connection not closed cleanly: %v", err)
	}
	if !strings.Contains(string(all), "daemon") {
		t.Errorf("status output missing before close:\n%s", all)
	}
}

// probeConn is a transport that can be told whether to answer the
// terminal probe, and that honours a read deadline like a socket.
type probeConn struct {
	in      chan []byte
	out     bytes.Buffer
	answer  bool
	pending []byte
	dead    time.Time
	mu      sync.Mutex
}

func (p *probeConn) Write(b []byte) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.out.Write(b)
	if p.answer && bytes.Contains(b, []byte("\x1b[6n")) {
		p.pending = append(p.pending, []byte("\x1b[24;1R")...)
	}
	return len(b), nil
}

func (p *probeConn) Read(b []byte) (int, error) {
	p.mu.Lock()
	if len(p.pending) > 0 {
		n := copy(b, p.pending)
		p.pending = p.pending[n:]
		p.mu.Unlock()
		return n, nil
	}
	deadline := p.dead
	p.mu.Unlock()
	var timer <-chan time.Time
	if !deadline.IsZero() {
		timer = time.After(time.Until(deadline))
	}
	select {
	case chunk, ok := <-p.in:
		if !ok {
			return 0, io.EOF
		}
		return copy(b, chunk), nil
	case <-timer:
		return 0, os.ErrDeadlineExceeded
	}
}

func (p *probeConn) SetReadDeadline(t time.Time) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.dead = t
	return nil
}

func (p *probeConn) transcript() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.out.String()
}

func TestASessionDegradesInsteadOfWaiting(t *testing.T) {
	for _, c := range []struct {
		name    string
		answers bool
		edited  bool
	}{
		{"a terminal that answers gets the editor", true, true},
		{"a pipe that cannot answer gets plain lines", false, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			p := &probeConn{in: make(chan []byte, 4), answer: c.answers}
			done := make(chan struct{})
			go func() { defer close(done); ServeAuto(context.Background(), p, testDeps(t)) }()
			p.in <- []byte("status\n")
			p.in <- []byte("quit\n")
			close(p.in)
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("the session never came back — a probe must not be able to hang it")
			}
			out := p.transcript()
			if !strings.Contains(out, "daemon") {
				t.Fatalf("the command never ran:\n%q", out)
			}
			// The editor repaints and colours; the plain path does
			// neither, which is what a transcript wants.
			painted := strings.Contains(out, "\x1b[K") || strings.Contains(out, cPath)
			if painted != c.edited {
				t.Errorf("edited=%v, want %v — transcript %q", painted, c.edited, out[:min(200, len(out))])
			}
		})
	}
}

func TestTheProbeGivesBackWhatItSwallowed(t *testing.T) {
	// A peer that starts talking instead of answering must not lose
	// its first letters to the question — and must not wait for the
	// grace either, since what it sent can never become an answer.
	p := &probeConn{in: make(chan []byte, 2)}
	p.in <- []byte("status\nquit\n")
	close(p.in)
	done := make(chan struct{})
	start := time.Now()
	go func() { defer close(done); ServeAuto(context.Background(), p, testDeps(t)) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the session hung")
	}
	if !strings.Contains(p.transcript(), "daemon") {
		t.Fatalf("the first command was eaten:\n%q", p.transcript())
	}
	if took := time.Since(start); took > terminalGrace {
		t.Errorf("waited %s for an answer that could never come", took)
	}
}

func TestCursorReportShapes(t *testing.T) {
	for _, c := range []struct {
		in   string
		want bool
	}{
		{"\x1b", true},       // the introducer alone may still grow
		{"\x1b[", true},      //
		{"\x1b[24", true},    //
		{"\x1b[24;1R", true}, // the whole answer
		{"s", false},         // a command's first letter
		{"\x1bO", false},     // a function key, not a report
		{"\x1b[A", false},    // an arrow
	} {
		if got := couldBeCursorReport([]byte(c.in)); got != c.want {
			t.Errorf("couldBeCursorReport(%q) = %v", c.in, got)
		}
	}
}
