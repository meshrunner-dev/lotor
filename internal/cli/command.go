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
// The one-line descriptions the shared arguments carry, written once
// so seven commands cannot describe the same argument seven ways.
const (
	docRelay = "which relay, when several run"
	docJSON  = "machine-readable output instead of a table"
	docLast  = "how far back to reach"
	docWatch = "stay and print each answer as it lands"
	docFlood = "flood the mesh rather than announcing zero-hop"
)

// positional is an argument a command reads by where it sits rather
// than by name — one per command, and only where the command says so.
type positional struct {
	name string
	doc  string
}

type flagSpec struct {
	name   string
	valued bool
	// doc is the one line the help gives it. An argument nobody can
	// discover is an argument nobody uses, which is what the "--"
	// spelling used to guarantee.
	doc string
	// values answers what this flag accepts, for the completion that
	// offers them. A closed set says so here rather than in a second
	// list somewhere else.
	values func(*session) []string
}

// command declares one shell command completely: grammar, help and
// implementation. The commands table is the single source of truth —
// dispatch, both help levels, flag admissibility and positional arity
// all derive from it, so they cannot drift apart.
type command struct {
	name    string
	aliases []string
	// forms is the top-level listing: positional shapes and purpose,
	// no flags. detail is what "?" prints — the full forms, arguments
	// included; when empty, the forms' calls stand in.
	forms  []form
	detail []string
	// flags a command does not declare are errors, not noise to
	// swallow.
	// their meaning stays the command's business.
	flags []flagSpec
	// on names the kind whose instance this command acts on. Such a
	// command lives in its context and nowhere else: at the root
	// nobody has said which one, and the old answer — quietly picking
	// the only relay — was multi-relay hidden rather than handled.
	// onOne says the kind is a singleton: the block is its own
	// instance, and the command mounts one step higher.
	on    string
	onOne bool
	// takes is the one word a command reads without naming it,
	// declared like every other so help can describe it and the painter
	// can tell a chosen value from a mistake. A command without one
	// accepts no bare words at all.
	takes *positional
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
	commands = append(commands, regionCommands()...)
	commands = append(commands, regionFlagCommands()...)
	commands = append(commands, deprecatedRegionSpellings()...)
	commands = append(commands, journalCommands()...)
	commands = append(commands, updateCommands()...)
	commands = append(commands, sessionCommands()...)
	commands = append(commands, accessCommands()...)
}

// accessCommands manage a relay's access list — who may administer it.
func accessCommands() []*command {
	return []*command{
		{
			name:  cmdGrant,
			forms: []form{{cmdGrant + " key=<hex> [role=admin]", "grant this key a role, no password shared"}},
			detail: []string{
				cmdGrant + " key=<64-hex> [role=admin|read-write|read-only]",
				"admin only. It records a permission for a whole public",
				"key — the node need not have logged in, and the grant",
				"outlives idle where a login does not. The key must be",
				"complete: a prefix could name the wrong node. The role",
				"defaults to admin; only admin opens the command channel.",
			},
			flags: []flagSpec{
				{name: scopeRelay, valued: true, doc: docRelay},
				{name: optKey, valued: true, doc: "the whole public key, 64 hex characters"},
				{name: optRole, valued: true, doc: "admin, read-write or read-only (default admin)",
					values: func(*session) []string { return []string{roleAdmin, roleReadWrite, roleReadOnly} }},
			},
			admin: true,
			run:   (*session).grantAccess,
		},
		{
			name:  cmdRevoke,
			forms: []form{{cmdRevoke, "take back a grant, or drop a session"}},
			detail: []string{
				cmdRevoke,
				"admin only. It removes the entry you stand on from the",
				"access list — a granted admin loses the role, a session",
				"is dropped. Named by key prefix from the drawer.",
			},
			flags: []flagSpec{
				{name: scopeRelay, valued: true, doc: docRelay},
				{name: optKey, valued: true, doc: "which entry, by key prefix"},
			},
			admin: true,
			run:   (*session).revokeAccess,
		},
	}
}

// daemonCommands inspect the running daemon and its configuration.
func daemonCommands() []*command {
	return []*command{
		{
			name:  "status",
			forms: []form{{"status", "daemon overview"}},
			run:   (*session).status,
		},
	}
}

// relayCommands reach into one relay's own state: what its engine
// knows, and the two things an operator can ask it to put on the air.
// deprecatedRegionSpellings answer to the old scope words for one
// release, so a habit or a script survives the rename.
func deprecatedRegionSpellings() []*command {
	return []*command{
		{
			name: cmdScopes,
			on:   scopeRelay,
			forms: []form{
				{cmdScopes, "deprecated spelling of " + cmdRegions},
			},
			detail: []string{
				cmdScopes,
				"deprecated: the word is " + cmdRegions + " now — this",
				"spelling answers the same and leaves next release.",
			},
			flags: []flagSpec{{name: scopeRelay, valued: true, doc: docRelay}},
			run:   (*session).regions,
		},
		{
			name: cmdAskScopes,
			forms: []form{
				{cmdAskScopes, "deprecated spelling of " + cmdAskRegions},
			},
			detail: []string{
				cmdAskScopes,
				"deprecated: the word is " + cmdAskRegions + " now — this",
				"spelling answers the same and leaves next release.",
			},
			flags: []flagSpec{
				{name: scopeRelay, valued: true, doc: docRelay},
				{name: optNeighbour, valued: true, doc: "which one, by key prefix"},
			},
			admin: true,
			run:   (*session).askRegions,
		},
	}
}

// regionFlagCommands are the drawer's per-region verbs.
func regionFlagCommands() []*command {
	return []*command{
		{
			name:  cmdAllowF,
			forms: []form{{cmdAllowF, "allow floods in one region"}},
			detail: []string{
				cmdAllowF + " " + optRegion + "=<name>",
				"admin only; clears the region's deny-flood flag —",
				"the wildcard included, which governs plain floods.",
			},
			flags: []flagSpec{
				{name: scopeRelay, valued: true, doc: docRelay},
				{name: optRegion, valued: true, doc: "which region, by name or prefix"},
			},
			admin: true,
			run:   (*session).regionAllow,
		},
		{
			name:  cmdDenyF,
			forms: []form{{cmdDenyF, "deny floods in one region"}},
			detail: []string{
				cmdDenyF + " " + optRegion + "=<name>",
				"admin only; sets the region's deny-flood flag —",
				"the wildcard included, which governs plain floods.",
			},
			flags: []flagSpec{
				{name: scopeRelay, valued: true, doc: docRelay},
				{name: optRegion, valued: true, doc: "which region, by name or prefix"},
			},
			admin: true,
			run:   (*session).regionDeny,
		},
		{
			name:  cmdDrop,
			forms: []form{{cmdDrop, "remove one region from the table"}},
			detail: []string{
				cmdDrop + " " + optRegion + "=<name>",
				"admin only; exact name, children first — what is",
				"destroyed is looked up, never guessed at.",
			},
			flags: []flagSpec{
				{name: scopeRelay, valued: true, doc: docRelay},
				{name: optRegion, valued: true, doc: "which region, by its exact name"},
			},
			admin: true,
			run:   (*session).regionDrop,
		},
	}
}

func relayCommands() []*command {
	return []*command{
		{
			name: cmdDiscover,
			forms: []form{
				{cmdDiscover, "ask the neighbourhood who is there"},
			},
			detail: []string{
				"discover [watch]",
				"admin only. It emits and returns; answers arrive spread",
				"out over the following minute, on purpose, and join the",
				"neighbourhood as they land — \"neighbours\" reads them,",
				"freshest first. \"watch\" waits there and prints them live;",
				"enter stops watching, and the scan carries on without you.",
			},
			flags: []flagSpec{{name: scopeRelay, valued: true, doc: docRelay}, {name: optWatch, doc: docWatch}},
			admin: true,
			run:   (*session).discover,
		},
		{
			name: cmdAdvert,
			on:   scopeRelay,
			forms: []form{
				{"advert [flood]", "announce this node now: zero-hop, or flood the mesh"},
			},
			detail: []string{
				"advert [flood]",
				"admin only; one order per ten seconds, and the duty budget",
				"has the last word on all of them",
			},
			flags: []flagSpec{
				{name: scopeRelay, valued: true, doc: docRelay},
				{name: optFlood, doc: docFlood},
			},
			admin: true,
			run:   (*session).advert,
		},
	}
}

// regionCommands administer the region table — the wire's own
// grammar, spoken from the console.
func regionCommands() []*command {
	return []*command{
		{
			name: cmdRegion,
			on:   scopeRelay,
			forms: []form{
				{cmdRegion + ` ["<verb …>"]`, "administer the region table, the wire's own grammar"},
			},
			detail: []string{
				cmdRegion + ` ["put <name> [parent]"]`,
				"admin only. One line of the ecosystem's region grammar,",
				"quoted when it holds spaces: put, remove, get, home,",
				"default, allowf, denyf, list allowed|denied, def, save.",
				"Bare " + cmdRegion + " prints the tree. Replies are the wire's,",
				"and every change applies at once — no relay restart.",
			},
			flags: []flagSpec{{name: scopeRelay, valued: true, doc: docRelay}},
			takes: &positional{name: "verb", doc: "the rest of the region line, quoted if spaced"},
			admin: true,
			run:   (*session).regionLine,
		},
		{
			name: cmdAskRegions,
			forms: []form{
				{cmdAskRegions, "ask a neighbour which regions it carries"},
			},
			detail: []string{
				cmdAskRegions,
				"admin only; it emits, and the answer comes from the",
				"neighbour itself rather than from anything already known",
			},
			flags: []flagSpec{
				{name: scopeRelay, valued: true, doc: docRelay},
				{name: optNeighbour, valued: true, doc: "which one, by key prefix"},
			},
			admin: true,
			run:   (*session).askRegions,
		},
	}
}

// framesCommand is the journal reader, declared apart because its
// grammar — filters, three window selectors and the live feed — is a
// command's worth on its own.
func framesCommand() *command {
	return &command{
		name: cmdFrames,
		forms: []form{
			{"frames", "journalled receptions"},
			{"frames watch", "live feed (enter stops)"},
		},
		detail: []string{
			"frames [last=<n|span>] [since=<moment>] [until=<moment>] [relay=] [type=] [verdict=]",
			"frames around=<corr-prefix> [span=<duration>] [relay=] [type=] [verdict=]",
			"frames watch [relay=<name>] [type=<type>] [verdict=<verdict>]",
			"a moment is written the way the views write one: 00:52,",
			"00:52:18, or \"2026-08-27 23:00\" — a bare clock means its",
			"most recent occurrence",
		},
		flags: []flagSpec{
			{name: optLast, valued: true, doc: "the newest slice — a count (50) or a span (15m)"},
			{name: scopeRelay, valued: true, doc: docRelay},
			{name: optFrameType, valued: true, doc: "keep one payload type",
				values: (*session).frameTypes},
			{name: optVerdict, valued: true, doc: "keep one judgement",
				values: (*session).frameVerdicts},
			{name: optSince, valued: true, doc: "from this moment on"},
			{name: optUntil, valued: true, doc: "up to this moment"},
			{name: optAround, valued: true, doc: "the window around one correlation, by id prefix"},
			{name: optSpan, valued: true, doc: "how far around, each side (default 1m)"},
			{name: optWatch, doc: docWatch},
		},
		run: (*session).frames,
	}
}

// journalCommands read the sentinel's archive and the bus's live feed.
func journalCommands() []*command {
	return []*command{
		framesCommand(),
		{
			name:  "corr",
			forms: []form{{"corr <prefix>", "one correlation and its causal chain"}},
			takes: &positional{name: "prefix", doc: "a correlation id, or enough of one"},
			run:   (*session).corr,
		},
		{
			name:   "nodes",
			forms:  []form{{"nodes", "the directory the mesh writes about itself"}},
			detail: []string{"nodes [json]"},
			flags:  []flagSpec{{name: optJSON, doc: docJSON}},
			run:    (*session).nodes,
		},
		{
			name:  "states",
			forms: []form{{"states", "relay and observer lifecycle history"}},
			detail: []string{
				"states [last=<count|span>] [json]",
				"interleaves relay and observer transitions on one timeline;",
				"last defaults to the newest 20 and is capped at 1000",
			},
			flags: []flagSpec{
				{name: optLast, valued: true, doc: "the newest count or a span such as 15m"},
				{name: optJSON, doc: docJSON},
			},
			run: (*session).states,
		},
		{
			name:   "tx",
			forms:  []form{{"tx", "transmit-airtime history, consolidated"}},
			detail: []string{"tx [relay=<name>] [last=24h|7d] [json]"},
			flags: []flagSpec{
				{name: scopeRelay, valued: true, doc: docRelay}, {name: optLast, valued: true, doc: docLast},
				{name: optJSON, doc: docJSON},
			},
			run: (*session).tx,
		},
		{
			name:   "noise",
			forms:  []form{{"noise", "noise-floor history, consolidated"}},
			detail: []string{"noise [relay=<name>] [last=24h|7d] [json]"},
			flags: []flagSpec{
				{name: scopeRelay, valued: true, doc: docRelay}, {name: optLast, valued: true, doc: docLast},
				{name: optJSON, doc: docJSON},
			},
			run: (*session).noise,
		},
		{
			name:  cmdJournal,
			forms: []form{{cmdJournal, "the journal's own state"}},
			run:   (*session).sentinelStatus,
		},
	}
}

// updateCommands ask the release channels about this daemon.
func updateCommands() []*command {
	return []*command{
		{
			name:  cmdCheck,
			on:    kindUpdate,
			onOne: true,
			forms: []form{{cmdCheck, "ask the channel what it offers"}},
			detail: []string{
				cmdCheck,
				"fetches the channel's signed manifest, verifies it, and",
				"compares with what runs; nothing is downloaded or changed",
			},
			run: (*session).updateCheck,
		},
		{
			name:  cmdInstall,
			on:    kindUpdate,
			onOne: true,
			forms: []form{{"install [force]", "fetch what the channel offers and stage it"}},
			detail: []string{
				"install [force]",
				"downloads, verifies signature and hash, proves the new",
				"binary starts, and stages it for the privileged installer —",
				"which installs and restarts the daemon. force stages a",
				"version that is not newer, for stepping down a channel.",
			},
			flags: []flagSpec{{name: optForce, doc: "stage even when nothing is newer"}},
			admin: true,
			run:   (*session).updateInstall,
		},
	}
}

// sessionCommands steer the session itself.
func sessionCommands() []*command {
	return []*command{
		{
			name:  "help",
			forms: []form{{"help [command]", "all commands, or one command's usage"}},
			takes: &positional{name: "command", doc: "one command, for its own usage"},
			run:   (*session).help,
		},
		{
			name:   cmdUndo,
			forms:  []form{{cmdUndo, "invert the newest configuration change"}},
			detail: []string{cmdUndo, "one step back, recorded like any other change"},
			admin:  true,
			run:    (*session).undoCmd,
		},
		{
			name:    cmdQuit,
			aliases: []string{"exit"},
			forms:   []form{{cmdQuit, "leave the console"}},
			run:     (*session).quit,
		},
	}
}

// commandNames lists every flat command, for the tree's completion.
func commandNames() []string {
	out := make([]string, 0, len(commands))
	for _, c := range commands {
		out = append(out, c.name)
	}
	return out
}

// rootCommandNames is what the root itself answers to — the commands
// that do not live somewhere more specific.
func rootCommandNames() []string {
	out := make([]string, 0, len(commands))
	for _, c := range commands {
		if commandHome(c) == "" {
			out = append(out, c.name)
		}
	}
	return out
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
// parse reads a command's words the way the console speaks: a bare
// name is a switch, name=value is a parameter, and anything else is a
// positional word. There is no punctuation to remember, which is what
// lets an argument be completed and described like everything else.
func (c *command) parse(args []string) (input, error) {
	in := input{opts: map[string]string{}}
	for i := range args {
		a := args[i]
		key, value, hasValue := strings.Cut(a, "=")
		spec := c.flag(key)
		if spec == nil {
			if hasValue {
				return in, fmt.Errorf("no argument %q here — try %q", key, c.name+" ?")
			}
			in.pos = append(in.pos, a)
			continue
		}
		if hasValue {
			if !spec.valued {
				return in, fmt.Errorf("%s takes no value", key)
			}
			in.opts[key] = value
			continue
		}
		switch {
		case !spec.valued:
			in.opts[key] = optOn
		default:
			return in, fmt.Errorf("%s wants a value — %s=…", key, key)
		}
	}
	if allowed := 0; c.takes != nil {
		allowed = 1
		if len(in.pos) > allowed {
			return in, fmt.Errorf("%s takes one %s — try %q", c.name, c.takes.name, c.name+" ?")
		}
	} else if len(in.pos) > allowed {
		return in, fmt.Errorf("unknown argument %q — try %q", in.pos[0], c.name+" ?")
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
		if commandHome(c) != "" {
			continue
		}
		for _, f := range c.forms {
			width = max(width, len(f.call))
		}
	}
	for _, c := range commands {
		if commandHome(c) != "" {
			continue
		}
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
	if home := commandHome(c); home != "" {
		fmt.Fprintf(s.out, "lives in %s\r\n", home)
	}
	return nil
}

// quit ends the session; the REPL reads the intent off the flag.
func (s *session) quit(context.Context, input) error {
	fmt.Fprint(s.out, "bye.\r\n")
	s.quitting = true
	return nil
}
