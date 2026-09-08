package main

import (
	"context"
	"io"
	"net"
	"os"
	"strings"

	"golang.org/x/term"

	"meshrunner.dev/lotor/internal/cli"
	"meshrunner.dev/lotor/internal/config"
)

type consoleSize struct{ width, height int }

// console connects the terminal to the daemon's CLI. Keep local signal
// handling while connecting, then put the TTY in raw mode before sending
// terminal metadata or forwarding keys to the daemon's line editor.
// An explicit terminal announcement avoids a round trip through an SSH
// terminal emulator just to distinguish a human from piped input.
func console(addr string) error {
	network := "tcp"
	switch {
	case addr == "":
		if _, err := os.Stat(config.DefaultConsoleSocket); err == nil {
			network, addr = "unix", config.DefaultConsoleSocket
		} else {
			addr = config.DefaultCLIListen
		}
	case strings.Contains(addr, "/"):
		network = "unix"
	}

	var d net.Dialer
	conn, err := d.DialContext(context.Background(), network, addr)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	var size *consoleSize
	if fd := int(os.Stdin.Fd()); term.IsTerminal(fd) {
		state, err := term.MakeRaw(fd)
		if err != nil {
			return err
		}
		defer func() { _ = term.Restore(fd, state) }()
		width, height, err := term.GetSize(fd)
		if err != nil {
			width, height = 0, 0
		}
		size = &consoleSize{width: width, height: height}
	}
	return copyConsole(conn, os.Stdin, os.Stdout, size)
}

// copyConsole announces terminal metadata before forwarding any user
// bytes. The server closing ends the command without waiting for stdin,
// so an update restart or quit cannot require one more key to leave.
func copyConsole(conn net.Conn, in io.Reader, out io.Writer, size *consoleSize) error {
	if size != nil {
		if err := cli.SendTerminalSize(conn, size.width, size.height); err != nil {
			return err
		}
	}
	go func() {
		_, _ = io.Copy(cli.EscapeIAC(conn), in)
		if t, ok := conn.(interface{ CloseWrite() error }); ok {
			_ = t.CloseWrite()
		}
	}()
	_, _ = io.Copy(out, cli.StripIAC(conn))
	return nil
}
