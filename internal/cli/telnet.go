package cli

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"time"

	"go.uber.org/zap"
)

// ServeTelnet accepts operator sessions until the context ends. The
// wire is plaintext and v1 is read-only; the default bind is loopback.
func ServeTelnet(ctx context.Context, addr string, deps Deps, log *zap.Logger) error {
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", addr)
	if err != nil {
		return err
	}
	log.Info("cli listening", zap.String("addr", addr))
	return ServeListener(ctx, ln, deps)
}

// ServeListener accepts sessions on an existing listener — the
// telnet entry point above, and the tests' doorway. Transient accept
// errors (fd exhaustion and kin) are ridden out, never fatal: the
// daemon must not lose its console for the rest of its life over one
// bad moment. Sessions are waited for on the way out, so their last
// output is flushed, not truncated.
func ServeListener(ctx context.Context, ln net.Listener, deps Deps) error {
	log := deps.Log
	if log == nil {
		log = zap.NewNop()
	}
	unlisten := context.AfterFunc(ctx, func() { _ = ln.Close() })
	defer unlisten()
	defer func() { _ = ln.Close() }()

	var sessions sync.WaitGroup
	defer sessions.Wait()
	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return nil
			}
			log.Debug("cli accept failed — riding it out", zap.Error(err))
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(250 * time.Millisecond):
			}
			continue
		}
		sessions.Go(func() {
			hangup := context.AfterFunc(ctx, func() { _ = conn.Close() })
			defer hangup()
			defer func() { _ = conn.Close() }()
			transport, peer := "telnet", conn.RemoteAddr().String()
			if _, unix := conn.(*net.UnixConn); unix {
				transport, peer = "console", ""
			}
			slog := log.With(zap.String("transport", transport), zap.String("peer", peer))
			slog.Info("cli session opened")
			opened := time.Now()
			defer func() {
				slog.Info("cli session closed", zap.Duration("after", time.Since(opened)))
			}()
			// Character-at-a-time with the daemon echoing: real telnet
			// clients honour it, the console client raw-modes its
			// terminal, and scripts through pipes simply see their
			// commands echoed into the transcript. The local socket
			// carries no telnet peer — nothing to negotiate with.
			if _, unix := conn.(*net.UnixConn); !unix {
				_, _ = conn.Write([]byte{iacByte, iacWill, optEcho, iacByte, iacWill, optSGA})
			}
			// Local console and telnet both carry IAC. NAWS lets a
			// raw client declare its mode before forwarding keystrokes.
			_, _ = conn.Write([]byte{iacByte, iacDo, optNAWS})
			ServeAuto(ctx, &sessionConn{
				Reader: &iacStripper{r: conn},
				// A client that stops reading while a watch floods
				// must wedge its own session, not the daemon: every
				// write gets a deadline.
				Writer: deadlineWriter{conn: conn},
				conn:   conn,
			}, deps)
		})
	}
}

// sessionConn is one session's byte stream, carrying the read
// deadline the terminal probe needs — the reader above it strips
// telnet commands, so the connection's own clock has to be reachable
// through it.
type sessionConn struct {
	io.Reader
	io.Writer

	conn net.Conn
}

type terminalDimensions struct{ width, height int }

func (s *sessionConn) SetReadDeadline(t time.Time) error { return s.conn.SetReadDeadline(t) }

func (s *sessionConn) terminalProbe(enabled bool) {
	if reader, ok := s.Reader.(*iacStripper); ok {
		reader.probing = enabled
	}
}

func (s *sessionConn) terminalSize() (int, bool) {
	if reader, ok := s.Reader.(*iacStripper); ok {
		return reader.width, reader.hasSize
	}
	return 0, false
}

func (s *sessionConn) terminalDimensions() terminalDimensions {
	if reader, ok := s.Reader.(*iacStripper); ok {
		return terminalDimensions{width: reader.width, height: reader.height}
	}
	return terminalDimensions{}
}

// Install the handler before starting the session reader. Valid metadata
// invokes it on that reader, even when no following key arrives.
func (s *sessionConn) onTerminalResize(handler func(terminalDimensions)) {
	if reader, ok := s.Reader.(*iacStripper); ok {
		reader.resized = handler
	}
}

// RemoteAddr names the far end, for the session table.
func (s *sessionConn) RemoteAddr() net.Addr { return s.conn.RemoteAddr() }

// Telnet protocol bytes (RFC 854) and the options we negotiate.
const (
	iacByte  = 255 // IAC — interpret as command
	iacWill  = 251 // WILL..DONT carry one option byte
	iacDo    = 253
	iacDont  = 254
	iacSubBg = 250 // SB — subnegotiation until IAC SE
	iacSubEn = 240 // SE
	optEcho  = 1   // the daemon echoes
	optSGA   = 3   // suppress go-ahead: character-at-a-time
	optNAWS  = 31  // RFC 1073: terminal width and height
)

// writeTimeout bounds one session write; a peer deaf for that long
// has left.
const writeTimeout = 30 * time.Second

// deadlineWriter arms a deadline before every write so a stalled
// client errors its session out instead of parking it forever.
type deadlineWriter struct {
	conn net.Conn
}

func (w deadlineWriter) Write(p []byte) (int, error) {
	_ = w.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
	return w.conn.Write(p)
}

// StripIAC filters telnet negotiation out of a byte stream — the
// console client reads the daemon through it, so our own WILL bytes
// never reach an operator's screen.
func StripIAC(r io.Reader) io.Reader { return &iacStripper{r: r} }

// EscapeIAC doubles 0xFF data bytes on the way to a telnet peer, as
// the protocol demands — the console client writes through it.
func EscapeIAC(w io.Writer) io.Writer { return &iacEscaper{w: w} }

type iacEscaper struct {
	w io.Writer
}

func (e *iacEscaper) Write(p []byte) (int, error) {
	written := 0
	for len(p) > 0 {
		i := bytes.IndexByte(p, iacByte)
		if i < 0 {
			n, err := e.w.Write(p)
			return written + n, err
		}
		if _, err := e.w.Write(p[:i+1]); err != nil {
			return written, err
		}
		if _, err := e.w.Write([]byte{iacByte}); err != nil {
			return written, err
		}
		written += i + 1
		p = p[i+1:]
	}
	return written, nil
}

// SendTerminalSize announces a raw terminal and its dimensions before
// user input is forwarded. This optimistic WILL + NAWS prelude uses the
// RFC 1073 layout; older Lotor servers ignore it through their IAC filter.
// The server also requests NAWS, but this small console client does not
// wait for a full Telnet option exchange. Zero means an unknown dimension.
func SendTerminalSize(w io.Writer, width, height int) error {
	var dimensions [4]byte
	binary.BigEndian.PutUint16(dimensions[:2], uint16(max(0, min(width, 65535))))
	binary.BigEndian.PutUint16(dimensions[2:], uint16(max(0, min(height, 65535))))
	var prelude bytes.Buffer
	prelude.Write([]byte{iacByte, iacWill, optNAWS, iacByte, iacSubBg, optNAWS})
	_, _ = EscapeIAC(&prelude).Write(dimensions[:])
	prelude.Write([]byte{iacByte, iacSubEn})
	n, err := w.Write(prelude.Bytes())
	if err == nil && n != prelude.Len() {
		return io.ErrShortWrite
	}
	return err
}

// iacStripper state machine.
const (
	stNormal    = iota
	stIAC       // just saw IAC
	stOption    // inside WILL/WONT/DO/DONT: one option byte to drop
	stSubneg    // inside a subnegotiation block
	stSubnegIAC // saw IAC inside the block: SE ends it
)

// iacStripper drops telnet's in-band negotiation so the REPL sees
// clean lines from real telnet clients; nc and scripts pass through
// untouched.
type iacStripper struct {
	r     io.Reader
	state int
	// A NAWS body is the option and four dimension bytes. Any other
	// subnegotiation is drained with the same constant memory bound.
	subneg  [5]byte
	subLen  int
	width   int
	height  int
	hasSize bool
	probing bool
	resized func(terminalDimensions)
}

// errTerminalSize yields control to mode selection after metadata alone,
// so a raw terminal need not send a keystroke to receive its first prompt.
var errTerminalSize = errors.New("terminal size received")

func (f *iacStripper) Read(p []byte) (int, error) {
	buf := make([]byte, len(p))
	for {
		n, err := f.r.Read(buf)
		out := 0
		hadSize := f.hasSize
		for _, b := range buf[:n] {
			if keep, c := f.step(b); keep {
				p[out] = c
				out++
			}
		}
		if f.probing && !hadSize && f.hasSize {
			return out, errTerminalSize
		}
		if out > 0 || err != nil {
			return out, err
		}
	}
}

// step advances the state machine by one byte and reports whether the
// byte reaches the application.
func (f *iacStripper) step(b byte) (keep bool, c byte) {
	switch f.state {
	case stNormal:
		if b == iacByte {
			f.state = stIAC
			return false, 0
		}
		return true, b
	case stIAC:
		switch {
		case b == iacByte: // escaped literal 0xFF
			f.state = stNormal
			return true, b
		case b == iacSubBg:
			f.state = stSubneg
			f.subLen = 0
		case b >= iacWill && b <= iacDont:
			f.state = stOption
		default: // two-byte command
			f.state = stNormal
		}
	case stOption:
		f.state = stNormal
	case stSubneg:
		if b == iacByte {
			f.state = stSubnegIAC
		} else {
			f.subByte(b)
		}
	case stSubnegIAC:
		switch b {
		case iacSubEn:
			f.state = stNormal
			f.applySize()
		case iacByte:
			f.subByte(b)
			f.state = stSubneg
		default:
			// An unexpected command cannot turn a malformed body
			// into a valid NAWS by silently losing its bytes.
			f.subLen = len(f.subneg) + 1
			f.state = stSubneg
		}
	}
	return false, 0
}

func (f *iacStripper) applySize() {
	if f.subLen != len(f.subneg) || f.subneg[0] != optNAWS {
		return
	}
	f.width = int(binary.BigEndian.Uint16(f.subneg[1:3]))
	f.height = int(binary.BigEndian.Uint16(f.subneg[3:5]))
	f.hasSize = true
	if f.resized != nil {
		f.resized(terminalDimensions{width: f.width, height: f.height})
	}
}

func (f *iacStripper) subByte(b byte) {
	if f.subLen < len(f.subneg) {
		f.subneg[f.subLen] = b
	}
	if f.subLen <= len(f.subneg) {
		f.subLen++
	}
}
