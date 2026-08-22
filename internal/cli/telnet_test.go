package cli

import (
	"context"
	"io"
	"net"
	"strings"
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
