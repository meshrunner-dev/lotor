package cli

import (
	"context"
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
			Serve(ctx, struct {
				io.Reader
				io.Writer
			}{Reader: &iacStripper{r: conn}, Writer: conn}, deps)
		})
	}
}

// Telnet protocol bytes (RFC 854).
const (
	iacByte  = 255 // IAC — interpret as command
	iacWill  = 251 // WILL..DONT carry one option byte
	iacDont  = 254
	iacSubBg = 250 // SB — subnegotiation until IAC SE
	iacSubEn = 240 // SE
)

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
}

func (f *iacStripper) Read(p []byte) (int, error) {
	buf := make([]byte, len(p))
	for {
		n, err := f.r.Read(buf)
		out := 0
		for _, b := range buf[:n] {
			if keep, c := f.step(b); keep {
				p[out] = c
				out++
			}
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
		}
	case stSubnegIAC:
		if b == iacSubEn {
			f.state = stNormal
		} else {
			f.state = stSubneg
		}
	}
	return false, 0
}
