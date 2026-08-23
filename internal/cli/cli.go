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
)

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
	fmt.Fprintf(s.out, "lotor %s — read-only. \"help\" lists commands, \"quit\" leaves.\r\n",
		deps.Version)
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

// command runs one line; it reports quit through s.quitting.
func (s *session) command(ctx context.Context, line string) {
	args := splitArgs(line)
	if len(args) == 0 {
		return
	}
	if args[0] == "quit" || args[0] == "exit" {
		fmt.Fprint(s.out, "bye.\r\n")
		s.quitting = true
		return
	}
	s.dispatch(ctx, args)
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

func (s *session) dispatch(ctx context.Context, args []string) {
	cmd, rest := args[0], args[1:]
	var err error
	switch cmd {
	case "help":
		s.help()
	case "status":
		err = s.status(ctx)
	case scopeRelay:
		err = s.relay(ctx, rest)
	case scopeRadio:
		err = s.radio(rest)
	case "config":
		err = s.config(rest)
	case "frames":
		err = s.frames(ctx, rest)
	case "txn":
		err = s.txn(ctx, rest)
	case "nodes":
		err = s.nodes(ctx, rest)
	case "sentinel":
		err = s.sentinelStatus(ctx)
	default:
		err = fmt.Errorf("unknown command %q — \"help\" lists them", cmd)
	}
	if err != nil {
		fmt.Fprintf(s.out, "error: %s\r\n", err)
	}
}

func (s *session) help() {
	fmt.Fprint(s.out, ""+
		"status                          daemon overview\r\n"+
		"relay list | relay show <name>  relays and their detail\r\n"+
		"radio list | radio show <name>  radios and their envelope\r\n"+
		"config show relay|radio <name>  effective config with provenance\r\n"+
		"frames [--last N] [--relay R] [--type T] [--verdict V] [--json]\r\n"+
		"frames watch [--type T]         live feed (enter stops)\r\n"+
		"txn <prefix>                    one transaction and its chain\r\n"+
		"nodes [--json]                  the directory the mesh writes about itself\r\n"+
		"sentinel                        journal status\r\n"+
		"quit\r\n")
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

// flags splits trailing --key value / --key options from positionals.
func flags(args []string) (pos []string, opts map[string]string, err error) {
	opts = map[string]string{}
	for i := 0; i < len(args); i++ {
		a := args[i]
		if !strings.HasPrefix(a, "--") {
			pos = append(pos, a)
			continue
		}
		key := strings.TrimPrefix(a, "--")
		switch key {
		case "json":
			opts[key] = optOn
		case "last", scopeRelay, "type", "verdict":
			if i+1 >= len(args) {
				return nil, nil, fmt.Errorf("--%s wants a value", key)
			}
			i++
			opts[key] = args[i]
		default:
			return nil, nil, fmt.Errorf("unknown flag --%s", key)
		}
	}
	return pos, opts, nil
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
