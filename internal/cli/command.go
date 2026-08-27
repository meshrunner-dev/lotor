package cli

import (
	"context"
	"fmt"
	"slices"
	"strings"
)

// form is one way to call a command: the positional shape an operator
// types, and what it does.
type form struct {
	call string
	desc string
}

// flagSpec declares one flag a command accepts; a valued flag consumes
// the word that follows it.
type flagSpec struct {
	name   string
	valued bool
}

// command declares one shell command completely: grammar, help and
// implementation. The commands table is the single source of truth —
// dispatch, both help levels, flag admissibility and positional arity
// all derive from it, so they cannot drift apart.
type command struct {
	name    string
	aliases []string
	// forms is the top-level listing: positional shapes and purpose,
	// no flags. detail is what --help prints — the full forms, flags
	// included; when empty, the forms' calls stand in.
	forms  []form
	detail []string
	// flags a command does not declare are errors, not noise to
	// swallow. maxPos bounds the positional words any form accepts;
	// their meaning stays the command's business.
	flags  []flagSpec
	maxPos int
	// admin commands act on the daemon — they need the local console
	// socket, whose file permissions are the authentication.
	admin bool
	run   func(*session, context.Context, input) error
}

// input is a parsed command line: positional words and flag values,
// already validated against the command's declaration.
type input struct {
	pos  []string
	opts map[string]string
}

// commands is populated in init: several commands reach dispatch —
// help iterates the table, watch runs the line that stops it — and a
// var initializer referencing them would be an initialization cycle.
var commands []*command

func init() {
	commands = append(commands, daemonCommands()...)
	commands = append(commands, relayCommands()...)
	commands = append(commands, journalCommands()...)
	commands = append(commands, sessionCommands()...)
}

// daemonCommands inspect the running daemon and its configuration.
func daemonCommands() []*command {
	return []*command{
		{
			name:  "status",
			forms: []form{{"status", "daemon overview"}},
			run:   (*session).status,
		},
		{
			name: scopeRelay,
			forms: []form{
				{"relay list", "relays and their detail"},
				{"relay show <name>", "one relay in full"},
			},
			maxPos: 2,
			run:    (*session).relay,
		},
		{
			name: scopeRadio,
			forms: []form{
				{"radio list", "radios and their envelope"},
				{"radio show <name>", "one radio in full"},
			},
			maxPos: 2,
			run:    (*session).radio,
		},
		{
			name:   "config",
			forms:  []form{{"config show relay|radio <name>", "effective config with provenance"}},
			maxPos: 3,
			run:    (*session).config,
		},
	}
}

// relayCommands reach into one relay's own state: what its engine
// knows, and the two things an operator can ask it to put on the air.
func relayCommands() []*command {
	return []*command{
		{
			name: "scopes",
			forms: []form{
				{"scopes", "the transport scopes this relay carries"},
				{"scopes ask <pubkey>", "ask a neighbour which it carries"},
			},
			detail: []string{
				"scopes [--relay R]",
				"scopes ask <pubkey-prefix> [--relay R]",
				"asking emits — admin only, one question at a time, and the",
				"answer comes from the neighbour itself, not from a directory",
			},
			flags:  []flagSpec{{name: scopeRelay, valued: true}},
			maxPos: 2,
			run:    (*session).scopes,
		},
		{
			name: "discover",
			forms: []form{
				{"discover", "ask the neighbourhood who is there"},
			},
			detail: []string{
				"discover [--watch] [--relay R]",
				"admin only. It emits and returns; answers arrive spread",
				"out over the following minute, on purpose, and join the",
				"neighbourhood as they land — \"neighbours\" reads them,",
				"freshest first. --watch waits there and prints them live.",
			},
			flags: []flagSpec{{name: scopeRelay, valued: true}, {name: optWatch}},
			admin: true,
			run:   (*session).discover,
		},
		{
			name:   "neighbours",
			forms:  []form{{"neighbours", "repeaters heard with no relay in between"}},
			detail: []string{"neighbours [--relay R]"},
			flags:  []flagSpec{{name: scopeRelay, valued: true}},
			run:    (*session).neighbours,
		},
		{
			name: "advert",
			forms: []form{
				{"advert [flood]", "announce this node now: zero-hop, or flood the mesh"},
			},
			detail: []string{
				"advert [flood] [--relay R]",
				"admin only; no limiter of its own — the duty budget has the last word",
			},
			flags:  []flagSpec{{name: scopeRelay, valued: true}},
			maxPos: 1,
			admin:  true,
			run:    (*session).advert,
		},
	}
}

// journalCommands read the sentinel's archive and the bus's live feed.
func journalCommands() []*command {
	return []*command{
		{
			name: cmdFrames,
			forms: []form{
				{"frames", "journalled receptions"},
				{"frames watch", "live feed (enter stops)"},
			},
			detail: []string{
				"frames [--last N] [--relay R] [--type T] [--verdict V] [--json]",
				"frames watch [--relay R] [--type T] [--verdict V] [--json]",
			},
			flags: []flagSpec{
				{name: optLast, valued: true}, {name: scopeRelay, valued: true},
				{name: "type", valued: true}, {name: "verdict", valued: true},
				{name: optJSON},
			},
			maxPos: 1,
			run:    (*session).frames,
		},
		{
			name:   "txn",
			forms:  []form{{"txn <prefix>", "one transaction and its chain"}},
			maxPos: 1,
			run:    (*session).txn,
		},
		{
			name:   "nodes",
			forms:  []form{{"nodes", "the directory the mesh writes about itself"}},
			detail: []string{"nodes [--json]"},
			flags:  []flagSpec{{name: optJSON}},
			run:    (*session).nodes,
		},
		{
			name:   "tx",
			forms:  []form{{"tx", "transmit-airtime history, consolidated"}},
			detail: []string{"tx [--relay R] [--last 24h|7d] [--json]"},
			flags: []flagSpec{
				{name: scopeRelay, valued: true}, {name: optLast, valued: true},
				{name: optJSON},
			},
			run: (*session).tx,
		},
		{
			name:   "noise",
			forms:  []form{{"noise", "noise-floor history, consolidated"}},
			detail: []string{"noise [--relay R] [--last 24h|7d] [--json]"},
			flags: []flagSpec{
				{name: scopeRelay, valued: true}, {name: optLast, valued: true},
				{name: optJSON},
			},
			run: (*session).noise,
		},
		{
			name:  "sentinel",
			forms: []form{{"sentinel", "journal status"}},
			run:   (*session).sentinelStatus,
		},
	}
}

// sessionCommands steer the session itself.
func sessionCommands() []*command {
	return []*command{
		{
			name:   "help",
			forms:  []form{{"help [command]", "all commands, or one command's usage"}},
			maxPos: 1,
			run:    (*session).help,
		},
		{
			name:    cmdQuit,
			aliases: []string{"exit"},
			forms:   []form{{cmdQuit, "leave the console"}},
			run:     (*session).quit,
		},
	}
}

// lookup resolves a command by name or alias.
func lookup(name string) *command {
	for _, c := range commands {
		if c.name == name || slices.Contains(c.aliases, name) {
			return c
		}
	}
	return nil
}

func unknownCommand(name string) error {
	return fmt.Errorf("unknown command %q — \"help\" lists them", name)
}

// parse validates a raw argument list against the declaration: only
// declared flags, no more positional words than any form takes.
func (c *command) parse(args []string) (input, error) {
	in := input{opts: map[string]string{}}
	for i := 0; i < len(args); i++ {
		a := args[i]
		if !strings.HasPrefix(a, "--") {
			in.pos = append(in.pos, a)
			continue
		}
		key := strings.TrimPrefix(a, "--")
		spec := c.flag(key)
		switch {
		case spec == nil:
			return in, fmt.Errorf("unknown flag --%s — try %s --help", key, c.name)
		case !spec.valued:
			in.opts[key] = optOn
		case i+1 >= len(args):
			return in, fmt.Errorf("--%s wants a value", key)
		default:
			i++
			in.opts[key] = args[i]
		}
	}
	if len(in.pos) > c.maxPos {
		return in, fmt.Errorf("unknown argument %q — try %s --help", in.pos[c.maxPos], c.name)
	}
	return in, nil
}

func (c *command) flag(name string) *flagSpec {
	for i := range c.flags {
		if c.flags[i].name == name {
			return &c.flags[i]
		}
	}
	return nil
}

// help is the help command: the whole listing, or one command's usage.
func (s *session) help(_ context.Context, in input) error {
	if len(in.pos) > 0 {
		return s.helpFor(in.pos[0])
	}
	width := 0
	for _, c := range commands {
		for _, f := range c.forms {
			width = max(width, len(f.call))
		}
	}
	for _, c := range commands {
		for _, f := range c.forms {
			fmt.Fprintf(s.out, "%-*s  %s\r\n", width, f.call, f.desc)
		}
	}
	return nil
}

// helpFor prints one command's full usage; asking about a command that
// does not exist is the same mistake as running one.
func (s *session) helpFor(name string) error {
	c := lookup(name)
	if c == nil {
		return unknownCommand(name)
	}
	lines := c.detail
	if len(lines) == 0 {
		for _, f := range c.forms {
			lines = append(lines, f.call)
		}
	}
	for _, l := range lines {
		fmt.Fprint(s.out, l+"\r\n")
	}
	return nil
}

// quit ends the session; the REPL reads the intent off the flag.
func (s *session) quit(context.Context, input) error {
	fmt.Fprint(s.out, "bye.\r\n")
	s.quitting = true
	return nil
}
