// Package cli is the line-based operator interface: a REPL that knows
// nothing about its transport (telnet today, SSH someday, a pipe in
// tests). Commands speak the design's vocabulary — relay, radio,
// sentinel, config — and v1 is read-only. Prefixes are keys: the short
// displayed form of a transaction or public key addresses its rows.
package cli

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"sort"
	"strings"
	"time"

	"meshrunner.dev/lotor/internal/bus"
	"meshrunner.dev/lotor/internal/config"
	"meshrunner.dev/lotor/internal/radio"
	"meshrunner.dev/lotor/internal/sentinel"
)

// Command vocabulary reused across parsers.
const (
	scopeRelay = "relay"
	scopeRadio = "radio"
	verbShow   = "show"
	optOn      = "true"
	optJSON    = "json"
	optLast    = "last"
	cmdFrames  = "frames"
	cmdQuit    = "quit"
)

// Privilege is what a session may do; the transport determines it.
// The local socket is admin because the OS already proved privileged
// access with its file permissions; network transports authenticate —
// and today's telnet is read-only. The two are indistinguishable for
// now: no command writes yet. The distinction is plumbed so the first
// admin command lands on a contract, not a retrofit.
type Privilege string

// The privilege levels sessions run at.
const (
	ReadOnly Privilege = "read-only"
	Admin    Privilege = "admin"
)

// Neighbour is one directly-heard repeater, as the console shows it.
type Neighbour struct {
	PubKey [32]byte
	SNR    float64
	Heard  time.Time
}

// RelayInfo is what the CLI knows about one relay.
type RelayInfo struct {
	Name     string
	Protocol string
	Radio    string
	Driver   string
	Waveform radio.Waveform
	State    func() string
	// Err reports the cause when State is error; may be nil.
	Err func() string
	// NoiseFloor reports the channel's last measured ambient level;
	// may be nil, and ok stays false until a measurement converges.
	NoiseFloor func() (radio.NoiseFloor, bool)
	// ChipStats reports the radio's own reception counters; may be nil.
	ChipStats func() (radio.ChipStats, bool)
	// TXMode is the transmit gate the relay runs behind: dry, shadow
	// or on-air; empty reads as dry.
	TXMode string
	// Scopes lists the transport scopes this relay carries; empty for
	// a protocol that has none.
	Scopes []string
	// AskScopes puts the scopes question to a neighbour named by a key
	// prefix, and waits for its answer; nil when the protocol has no
	// scopes to ask about.
	AskScopes func(prefix []byte) ([]string, error)
	// Discover runs a neighbourhood scan, yielding each answer as it
	// lands and closing when the window ends; nil when the protocol
	// has no scan to run.
	Discover func() (<-chan Neighbour, time.Time, error)
	// TriggerAdvert queues one operator announcement (flood or
	// zero-hop); nil when the engine has no transmit pipeline.
	TriggerAdvert func(flood bool) error
	// Neighbours lists the direct neighbourhood — repeaters heard with
	// no relay in between; nil when the engine keeps none.
	Neighbours func() []Neighbour
	// Duty reports the sliding-hour airtime spent against the band's
	// budget; may be nil, ok false when unbudgeted or not transmitting.
	Duty func() (used, budget time.Duration, ok bool)
	// Identity is the relay's node public key in hex, empty when none.
	Identity string
}

// RadioInfo is what the CLI knows about one radio attachment.
type RadioInfo struct {
	Name     string
	Driver   string
	Envelope radio.Envelope
	Relay    string
}

// Deps is everything the commands may consult. Sentinel may be nil —
// the commands that need it say so instead of pretending.
type Deps struct {
	Version  string
	Started  time.Time
	Relays   []RelayInfo
	Radios   []RadioInfo
	Sentinel *sentinel.Sentinel
	Bus      *bus.Bus
	// Privilege is the session's rights, set per listener by the
	// transport; empty reads as ReadOnly.
	Privilege Privilege
	// Traces holds the resolved-config provenance recorded at
	// assembly, keyed "radio <name>" and "relay <name>".
	Traces map[string][]config.Trace
}

// maxLineBytes bounds one command line: a client that never sends a
// newline exhausts this, not the daemon's memory.
const maxLineBytes = 4096

// session is one connected operator. Lines arrive on a channel fed by
// a single reader goroutine, so features like watch can select on
// input without fighting over the reader.
type session struct {
	deps  Deps
	lines <-chan string
	out   io.Writer
	// quitting is set when the operator asked to leave from inside a
	// nested command (a watch stopped by "quit").
	quitting bool
	// watching guards against nested watches piling subscriptions.
	watching bool
}

// Serve runs the REPL on plain line input — pipes, scripts, tests.
func Serve(ctx context.Context, rw io.ReadWriter, deps Deps) {
	lines := make(chan string)
	done := make(chan struct{})
	defer close(done)
	go readLines(rw, lines, done)
	repl(ctx, rw, deps, lines)
}

// ServeEdited runs the REPL behind the character-mode line editor:
// history on the arrows, a movable cursor, the daemon echoing. The
// transport delivers raw keystrokes (the telnet listener negotiates
// that; the console client sets its terminal raw).
func ServeEdited(ctx context.Context, rw io.ReadWriter, deps Deps) {
	lines := make(chan string)
	done := make(chan struct{})
	defer close(done)
	ed := newEditor(rw, rw)
	go func() {
		defer close(lines)
		for {
			line, err := ed.readLine()
			if err != nil {
				return
			}
			select {
			case lines <- line:
			case <-done:
				return
			}
		}
	}()
	repl(ctx, rw, deps, lines)
}

// repl is the loop both entrances share. It owns the prompt: printed
// after the banner and after every command, so it always lands below
// the output it follows.
func repl(ctx context.Context, out io.Writer, deps Deps, lines <-chan string) {
	s := &session{deps: deps, lines: lines, out: out}
	banner(s.out, deps.Version, deps.Privilege)
	for ctx.Err() == nil {
		fmt.Fprint(s.out, "> ")
		select {
		case <-ctx.Done():
			return
		case line, ok := <-lines:
			if !ok {
				return
			}
			if s.command(ctx, line); s.quitting {
				return
			}
		}
	}
}

// command runs one line; quit reports itself through s.quitting.
func (s *session) command(ctx context.Context, line string) {
	if args := splitArgs(line); len(args) > 0 {
		s.dispatch(ctx, args)
	}
}

// readLines feeds trimmed lines until EOF, an oversized line, or the
// session's end. A final line without a newline still counts — the
// operator's intent does not need a terminator. Oversized input ends
// the session: that is a hostile client, not a command.
func readLines(r io.Reader, lines chan<- string, done <-chan struct{}) {
	defer close(lines)
	br := bufio.NewReader(r)
	for {
		line, err := readBounded(br)
		if line != "" || err == nil {
			select {
			case lines <- line:
			case <-done:
				return
			}
		}
		if err != nil {
			return
		}
	}
}

func readBounded(br *bufio.Reader) (string, error) {
	var b strings.Builder
	for {
		c, err := br.ReadByte()
		if err != nil {
			return strings.TrimSpace(b.String()), err
		}
		if c == '\n' {
			return strings.TrimSpace(b.String()), nil
		}
		if b.Len() >= maxLineBytes {
			return "", errors.New("line too long")
		}
		b.WriteByte(c)
	}
}

// dispatch is pure mechanics: everything it enforces — the command
// set, flag admissibility, positional arity, both help levels — comes
// from the commands table.
func (s *session) dispatch(ctx context.Context, args []string) {
	name, rest := args[0], args[1:]
	var err error
	switch c := lookup(name); {
	case c == nil:
		err = unknownCommand(name)
	case slices.Contains(rest, "--help") || slices.Contains(rest, "-h"):
		err = s.helpFor(name)
	case c.admin && s.deps.Privilege != Admin:
		err = fmt.Errorf("%s is an admin command — use the local console socket", name)
	default:
		var in input
		if in, err = c.parse(rest); err == nil {
			err = c.run(s, ctx, in)
		}
	}
	if err != nil {
		fmt.Fprintf(s.out, "error: %s\r\n", err)
	}
}

func (s *session) findRelay(name string) (RelayInfo, error) {
	names := make([]string, 0, len(s.deps.Relays))
	for _, r := range s.deps.Relays {
		if r.Name == name {
			return r, nil
		}
		names = append(names, r.Name)
	}
	sort.Strings(names)
	return RelayInfo{}, fmt.Errorf("no relay %q (relays: %s)", name, strings.Join(names, ", "))
}

func (s *session) needSentinel() (*sentinel.Sentinel, error) {
	if s.deps.Sentinel == nil {
		return nil, errors.New("no sentinel configured — the journal commands need one")
	}
	return s.deps.Sentinel, nil
}

// splitArgs tokenises a command line, honouring double quotes so
// arguments may carry spaces — the mesh's names do.
func splitArgs(line string) []string {
	var args []string
	var cur strings.Builder
	inQuote, has := false, false
	for _, r := range line {
		switch {
		case r == '"':
			inQuote = !inQuote
			has = true
		case !inQuote && (r == ' ' || r == '\t'):
			if has || cur.Len() > 0 {
				args = append(args, cur.String())
				cur.Reset()
				has = false
			}
		default:
			cur.WriteRune(r)
		}
	}
	if has || cur.Len() > 0 {
		args = append(args, cur.String())
	}
	return args
}

func ago(t time.Time) string {
	d := time.Since(t)
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds ago", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm ago", int(d.Minutes()))
	case d < 48*time.Hour:
		return fmt.Sprintf("%dh ago", int(d.Hours()))
	default:
		return fmt.Sprintf("%dd ago", int(d.Hours()/24))
	}
}

func uptime(since time.Time) string {
	d := time.Since(since)
	if d < time.Hour {
		return fmt.Sprintf("%dm", int(d.Minutes()))
	}
	if d < 48*time.Hour {
		return fmt.Sprintf("%dh %02dm", int(d.Hours()), int(d.Minutes())%60)
	}
	return fmt.Sprintf("%dd %dh", int(d.Hours())/24, int(d.Hours())%24)
}
