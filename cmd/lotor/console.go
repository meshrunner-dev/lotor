package main

import (
	"context"
	"io"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"golang.org/x/term"

	"meshrunner.dev/lotor/internal/cli"
	"meshrunner.dev/lotor/internal/config"
)

type consoleSize struct{ width, height int }

type consoleResize struct {
	changed <-chan os.Signal
	measure func() (consoleSize, error)
}

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
	var resize *consoleResize
	if fd := int(os.Stdin.Fd()); term.IsTerminal(fd) {
		state, err := term.MakeRaw(fd)
		if err != nil {
			return err
		}
		defer func() { _ = term.Restore(fd, state) }()
		changed := make(chan os.Signal, 1)
		signal.Notify(changed, syscall.SIGWINCH)
		defer signal.Stop(changed)
		resize = &consoleResize{
			changed: changed,
			measure: func() (consoleSize, error) {
				width, height, err := term.GetSize(fd)
				return consoleSize{width: width, height: height}, err
			},
		}
		initial, err := resize.measure()
		if err != nil {
			initial = consoleSize{}
		}
		size = &initial
	}
	return copyConsole(conn, os.Stdin, os.Stdout, size, resize)
}

// copyConsole announces terminal metadata before forwarding any user
// bytes. The server closing ends the command without waiting for stdin,
// so an update restart or quit cannot require one more key to leave.
func copyConsole(conn net.Conn, in io.Reader, out io.Writer, size *consoleSize, resize *consoleResize) error {
	input := &consoleWriter{wire: conn}
	done := make(chan struct{})
	var workers sync.WaitGroup
	defer func() {
		close(done)
		// Release a resizer whose metadata write is blocked by a peer
		// that closed its output or stopped reading. Stdin may remain
		// blocked until the console process exits; never wait for it.
		_ = conn.Close()
		workers.Wait()
	}()
	if size != nil {
		if err := input.terminalSize(*size); err != nil {
			return err
		}
		if resize != nil {
			workers.Go(func() { resize.run(done, input, *size) })
		}
	}
	go func() {
		_, _ = io.Copy(input, in)
		if t, ok := conn.(interface{ CloseWrite() error }); ok {
			_ = t.CloseWrite()
		}
	}()
	_, _ = io.Copy(out, cli.StripIAC(conn))
	return nil
}

// consoleWriter locks one complete application write, including IAC
// escaping. A resize frame can never appear between an IAC and its doubled
// byte, or halfway through a pasted UTF-8 sequence.
type consoleWriter struct {
	wire io.Writer
	mu   sync.Mutex
}

func (w *consoleWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return cli.EscapeIAC(w.wire).Write(p)
}

func (w *consoleWriter) terminalSize(size consoleSize) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return cli.SendTerminalSize(w.wire, size.width, size.height)
}

func (r *consoleResize) run(done <-chan struct{}, writer *consoleWriter, previous consoleSize) {
	for {
		select {
		case <-done:
			return
		case _, ok := <-r.changed:
			if !ok {
				return
			}
			size, err := r.measure()
			if err == nil && size != previous {
				if err := writer.terminalSize(size); err != nil {
					return
				}
				previous = size
			}
		}
	}
}
