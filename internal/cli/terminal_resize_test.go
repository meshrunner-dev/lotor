package cli

import (
	"bytes"
	"io"
	"net"
	"slices"
	"testing"
	"testing/iotest"
	"time"
)

func TestTerminalDimensionsPreserveWidthAndHeight(t *testing.T) {
	var wire bytes.Buffer
	if err := SendTerminalSize(&wire, 255, 511); err != nil {
		t.Fatal(err)
	}
	strip := &iacStripper{r: iotest.OneByteReader(&wire)}
	session := &sessionConn{Reader: strip}
	var sizes []terminalDimensions
	session.onTerminalResize(func(size terminalDimensions) { sizes = append(sizes, size) })
	if _, err := io.ReadAll(strip); err != nil {
		t.Fatal(err)
	}
	want := terminalDimensions{width: 255, height: 511}
	if got := session.terminalDimensions(); got != want || !slices.Equal(sizes, []terminalDimensions{want}) {
		t.Fatalf("size=%v callbacks=%v; want %v", got, sizes, want)
	}
}

func TestTerminalResizeIsDeliveredBeforeAnotherKey(t *testing.T) {
	server, peer := net.Pipe()
	defer server.Close()
	defer peer.Close()
	_ = peer.SetWriteDeadline(time.Now().Add(5 * time.Second))
	strip := &iacStripper{r: server}
	session := &sessionConn{Reader: strip}
	sizes := make(chan terminalDimensions, 2)
	session.onTerminalResize(func(size terminalDimensions) { sizes <- size })
	readDone := make(chan struct{})
	go func() { _, _ = io.Copy(io.Discard, strip); close(readDone) }()
	for _, want := range []terminalDimensions{{width: 80, height: 24}, {width: 20, height: 10}} {
		if err := SendTerminalSize(peer, want.width, want.height); err != nil {
			t.Fatal(err)
		}
		select {
		case got := <-sizes:
			if got != want {
				t.Fatalf("resize=%v; want %v", got, want)
			}
		case <-time.After(time.Second):
			t.Fatal("metadata-only resize waited for a key")
		}
	}
	_ = peer.Close()
	<-readDone
}

func TestMalformedNAWSDoesNotNotifyResize(t *testing.T) {
	for _, body := range [][]byte{{optNAWS, 0, 80, 0}, {optNAWS, 0, 80, 0, 24, 0}} {
		wire := append([]byte{iacByte, iacSubBg}, body...)
		wire = append(wire, iacByte, iacSubEn)
		strip := &iacStripper{r: bytes.NewReader(wire)}
		strip.resized = func(size terminalDimensions) { t.Errorf("malformed metadata notified %v", size) }
		if _, err := io.ReadAll(strip); err != nil {
			t.Fatal(err)
		}
	}
}
