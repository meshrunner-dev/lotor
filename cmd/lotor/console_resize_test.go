package main

import (
	"bytes"
	"io"
	"net"
	"os"
	"sync"
	"syscall"
	"testing"
	"time"

	"meshrunner.dev/lotor/internal/cli"
)

func readConsoleSize(t *testing.T, peer net.Conn, width, height int) {
	t.Helper()
	var want bytes.Buffer
	if err := cli.SendTerminalSize(&want, width, height); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, want.Len())
	if _, err := io.ReadFull(peer, got); err != nil || !bytes.Equal(got, want.Bytes()) {
		t.Fatalf("NAWS=%v error=%v; want %v", got, err, want.Bytes())
	}
}

func TestConsoleResizeSendsDimensionsWithoutAKey(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	_ = server.SetDeadline(time.Now().Add(5 * time.Second))
	stdin, input := io.Pipe()
	defer stdin.Close()
	defer input.Close()
	changed := make(chan os.Signal, 1)
	measured := make(chan struct{}, 1)
	sizes := make(chan consoleSize, 2)
	sizes <- consoleSize{width: 80, height: 24}
	sizes <- consoleSize{width: 20, height: 10}
	resize := &consoleResize{changed: changed, measure: func() (consoleSize, error) {
		size := <-sizes
		measured <- struct{}{}
		return size, nil
	}}
	done := make(chan error, 1)
	go func() {
		done <- copyConsole(client, stdin, io.Discard, &consoleSize{width: 80, height: 24}, resize)
	}()
	readConsoleSize(t, server, 80, 24)
	changed <- syscall.SIGWINCH // unchanged dimensions must not emit another frame
	select {
	case <-measured:
	case <-time.After(time.Second):
		t.Fatal("resize signal was not measured")
	}
	changed <- syscall.SIGWINCH
	readConsoleSize(t, server, 20, 10)
	_ = server.Close()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("resize worker prevented console exit")
	}
}

func TestConsoleCloseJoinsABlockedResizeWrite(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	_ = server.SetDeadline(time.Now().Add(5 * time.Second))
	stdin, input := io.Pipe()
	defer stdin.Close()
	defer input.Close()
	changed := make(chan os.Signal, 1)
	measured := make(chan struct{})
	resize := &consoleResize{changed: changed, measure: func() (consoleSize, error) {
		close(measured)
		return consoleSize{width: 20, height: 10}, nil
	}}
	done := make(chan error, 1)
	go func() {
		done <- copyConsole(client, stdin, io.Discard, &consoleSize{width: 80, height: 24}, resize)
	}()
	readConsoleSize(t, server, 80, 24)
	changed <- syscall.SIGWINCH
	select {
	case <-measured:
	case <-time.After(time.Second):
		t.Fatal("resize was not attempted")
	}
	// Do not read the resize frame: net.Pipe cannot complete its write.
	if _, err := server.Write([]byte("bye.\r\n")); err != nil {
		t.Fatal(err)
	}
	_ = server.Close()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("closing output did not release the blocked metadata writer")
	}
}

type pausedConsoleWire struct {
	started, release chan struct{}
	once             sync.Once
	mu               sync.Mutex
	out              bytes.Buffer
}

func (w *pausedConsoleWire) Write(p []byte) (int, error) {
	w.once.Do(func() { close(w.started); <-w.release })
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.out.Write(p)
}

func TestConsoleResizeCannotSplitEscapedInput(t *testing.T) {
	wire := &pausedConsoleWire{started: make(chan struct{}), release: make(chan struct{})}
	writer := &consoleWriter{wire: wire}
	keysDone, sizeDone := make(chan error, 1), make(chan error, 1)
	go func() { _, err := writer.Write([]byte{'a', 255, 'b'}); keysDone <- err }()
	<-wire.started
	go func() { sizeDone <- writer.terminalSize(consoleSize{width: 255, height: 24}) }()
	close(wire.release)
	if err := <-keysDone; err != nil {
		t.Fatal(err)
	}
	if err := <-sizeDone; err != nil {
		t.Fatal(err)
	}
	var want bytes.Buffer
	want.Write([]byte{'a', 255, 255, 'b'})
	if err := cli.SendTerminalSize(&want, 255, 24); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(wire.out.Bytes(), want.Bytes()) {
		t.Fatalf("interleaved input and NAWS: %v; want %v", wire.out.Bytes(), want.Bytes())
	}
}
