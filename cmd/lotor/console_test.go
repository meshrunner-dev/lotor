package main

import (
	"bufio"
	"bytes"
	"net"
	"strings"
	"testing"
	"time"
)

func TestConsoleAnnouncesOnlyRawTerminalsBeforeInput(t *testing.T) {
	for _, tc := range []struct {
		name string
		size *consoleSize
		want string
	}{
		{"terminal", &consoleSize{width: 80, height: 24}, "\xff\xfb\x1f\xff\xfa\x1f\x00\x50\x00\x18\xff\xf0quit\n"},
		{"pipe", nil, "quit\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, server := net.Pipe()
			defer client.Close()
			defer server.Close()
			_ = client.SetDeadline(time.Now().Add(5 * time.Second))
			_ = server.SetDeadline(time.Now().Add(5 * time.Second))
			var screen bytes.Buffer
			done := make(chan error, 1)
			go func() { done <- copyConsole(client, strings.NewReader("quit\n"), &screen, tc.size, nil) }()
			wire, err := bufio.NewReader(server).ReadString('\n')
			if err != nil || wire != tc.want {
				t.Fatalf("first input = %q, %v; want %q", wire, err, tc.want)
			}
			if _, err := server.Write([]byte("\xff\xfb\x01bye.\r\n")); err != nil {
				t.Fatal(err)
			}
			_ = server.Close()
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("server close did not release the console")
			}
			if screen.String() != "bye.\r\n" {
				t.Fatalf("terminal output contains negotiation: %q", screen.String())
			}
		})
	}
}
