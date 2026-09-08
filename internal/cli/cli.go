// Package cli is the line-based operator interface: a REPL that knows
// nothing about its transport (telnet today, SSH someday, a pipe in
// tests). Commands speak the design's vocabulary — relay, radio,
// sentinel, config — and v1 is read-only. Prefixes are keys: the short
// displayed form of a correlation or public key addresses its rows.
package cli

import (
	"bytes"
	"sync"

	"go.uber.org/zap"

	"meshrunner.dev/lotor/internal/schema"

	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"meshrunner.dev/lotor/internal/bus"
	"meshrunner.dev/lotor/internal/config"
	"meshrunner.dev/lotor/internal/radio"
	"meshrunner.dev/lotor/internal/sensor"
	"meshrunner.dev/lotor/internal/sentinel"
	"meshrunner.dev/lotor/internal/update"
)

// Command vocabulary reused across parsers.
const (
	scopeRelay   = "relay"
	scopeStation = "station"
	scopeApp     = "application"
	scopeRadio   = "radio"
	scopeSensor  = "sensor"
	scopeMQTT    = "mqtt"
	verbShow     = "show"
	optOn        = "true"
	optJSON      = "json"
	optWatch     = "watch"
	optFlood     = "flood"
	optForce     = "force"
	optNeighbour = "neighbour"
	optFrameType = "type"
	optVerdict   = "verdict"
	optLast      = "last"
	cmdFrames    = "frames"
	cmdQuit      = "quit"

	// The relay-scoped commands, named once: the flat table and the
	// context tree both mount them. The scope spellings are deprecated
	// aliases of the region ones, kept a release.
	cmdRegions    = "regions"
	cmdScopes     = "scopes"
	cmdDiscover   = "discover"
	cmdAskRegions = "ask-regions"
	cmdAskScopes  = "ask-scopes"
	cmdAllowF     = "allowf"
	cmdDenyF      = "denyf"
	cmdDrop       = "drop"
	cmdPut        = "put"
	cmdDef        = "def"
	attrDefault   = "default"
	attrHome      = "home"
	optRegion     = "region"
	optParent     = "parent"
	cmdGrant      = "grant"
	cmdRevoke     = "revoke"
	cmdClose      = "close"
	optKey        = "key"
	optRole       = "role"
	// The role words the console speaks — the boundary's RoleByte is
	// the authority; a word it refuses is refused at the door.
	roleAdmin     = "admin"
	roleReadWrite = "read-write"
	roleReadOnly  = "read-only"
	cmdAdvert     = "advert"
	cmdUndo       = "undo"
	verbList      = "list"
	cmdJournal    = "journal"
	cmdCheck      = "check"
	cmdInstall    = "install"
	kindUpdate    = "update"
	// helpWord asks about whatever it follows.
	helpWord = "?"
	// wordHelp and wordExit are the spelled-out halves of the two
	// questions every place answers: what is here, and how to leave.
	wordHelp = "help"
	wordExit = "exit"
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
	// Name is what the node calls itself, empty when it has not said.
	Name  string
	SNR   float64
	Heard time.Time
}

// AirSession is one companion logged in over the air, as the console
// shows it.
type AirSession struct {
	PubKey [32]byte
	// Role is the protocol's word for this session's effective
	// permissions. In particular, a granted read-only client is not a
	// guest merely because it is not an administrator.
	Role string
	// Path is the route home the client taught us, one hash byte per
	// hop; HasPath false means answers flood. A zero-hop path says
	// the client is adjacent, which is not the same as not knowing.
	Path        []byte
	HasPath     bool
	PathLearned time.Time
	LastActive  time.Time
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
	// Regions reads the relay's region state — live, because the
	// table mutates over the air; nil for a protocol that has none.
	Regions func() (RegionInfo, error)
	// RegionLine runs one line of the region grammar for an owner and
	// returns the wire's own reply; handled is false when the line was
	// not region business. Nil when the protocol has no regions.
	RegionLine func(owner, line string) (reply string, handled bool, err error)
	// RegionDesignations atomically changes the default and/or home
	// designation. A nil pointer leaves one unchanged; an empty value
	// clears it. This is the structured console door, distinct from
	// the wire grammar carried by RegionLine.
	RegionDesignations func(owner string, defaultRegion, homeRegion *string) (reply string, err error)
	// RegionLoadArmed reports whether a modal region load is armed for
	// this owner — the dispatcher's pre-check; nil with RegionLine.
	RegionLoadArmed func(owner string) bool
	// Sign signs a message under the relay's node identity — how an
	// observer proves the device to a broker; nil without an identity.
	Sign func(message []byte) []byte
	// AskRegions puts the regions question to a neighbour named by a
	// key prefix, and waits for its answer; nil when the protocol has
	// no regions to ask about.
	AskRegions func(prefix []byte) ([]string, error)
	// Discover runs a neighbourhood scan, yielding each answer as it
	// lands and closing when the window ends; nil when the protocol
	// has no scan to run.
	Discover func() (<-chan Neighbour, time.Time, error)
	// ScanWindow reports when a scan already listening closes — the
	// window a refused Discover may join by waiting; nil when the
	// protocol has none.
	ScanWindow func() (time.Time, bool)
	// TriggerAdvert queues one operator announcement (flood or
	// zero-hop); nil when the engine has no transmit pipeline.
	TriggerAdvert func(flood bool) error
	// Neighbours lists the direct neighbourhood — repeaters heard with
	// no relay in between; nil when the engine keeps none.
	Neighbours func() []Neighbour
	// RemoveNeighbours drops the neighbours a key prefix names — all
	// for the empty prefix — and reports how many went; nil when the
	// engine keeps none.
	RemoveNeighbours func(prefix []byte) int
	// AirSessions lists the companions logged in over the air; nil
	// when the protocol keeps no sessions.
	AirSessions func() ([]AirSession, error)
	// CloseSession ends one live conversation without taking back a
	// durable ACL role; nil when the protocol cannot close sessions.
	CloseSession func(pubKey []byte) error
	// Access lists durable authorisations; live guests belong to
	// AirSessions instead. Nil when the protocol keeps no access list.
	Access func() ([]Access, error)
	// Grant records a permission byte for a key — the wire's own
	// value, passed through whole for the channels that carry one;
	// nil when the protocol grants nothing.
	Grant func(pubKey []byte, perms byte) error
	// GrantRole and Revoke are the console's words for the same door,
	// the role spoken by name and translated at the boundary so no
	// number ever appears here.
	GrantRole func(pubKey []byte, role string) error
	Revoke    func(pubKey []byte) error
	// Duty reports the sliding-hour airtime spent against the band's
	// budget; may be nil, ok false when unbudgeted or not transmitting.
	Duty func() (used, budget time.Duration, ok bool)
	// NodeName is what the relay calls itself on the air.
	NodeName string
	// Traffic reports the lifetime frame tally; may be nil.
	Traffic func() (sent, received, recvErrors uint32, txAir, rxAir time.Duration)
	// Identity is the relay's node public key in hex, empty when none.
	Identity string
	// Started is when this assembly came up — device uptime starts
	// here, not at whichever observer asks.
	Started time.Time
}

// StationInfo is one locally hosted companion identity. Its application
// listener and RF state are separate by design: detached radio service must
// not make the TCP endpoint disappear.
type StationInfo struct {
	Name, Protocol, Listen, Radio string
	State, Cause, RF, RFCause     string
	Connected                     bool
	Remote                        string
	Mailbox, MailboxCap           int
	Waveform                      radio.Waveform
	Identity                      string
}

// ApplicationInfo is one hosted identity serving peers over the air.
// Summary is the type's own status line, keyed by the words it prints.
type ApplicationInfo struct {
	Name, Protocol, Type, Radio string
	State, Cause, RF, RFCause   string
	Waveform                    radio.Waveform
	Identity                    string
	Summary                     map[string]string
}

// HistoryQuery is one history print's answer to "which slice": the
// same vocabulary frames speaks — a count, window edges, or a
// revision to centre on.
type HistoryQuery struct {
	Count        int
	Since, Until time.Time
	AroundID     int64
	Span         time.Duration
}

// HistoryEntry is one recorded mutation, as the console shows it:
// values already rendered, secrets already masked by the store.
type HistoryEntry struct {
	ID        int64
	At        time.Time
	Principal string
	Kind      string
	Name      string
	Op        string
	Changes   []AttrDelta
}

// AttrDelta is one attribute's before and after. Empty means absent —
// no value on that side of the change.
type AttrDelta struct {
	Attr string
	Old  string
	New  string
}

// Access is one durable entry of the access list: who, what role,
// whether it was granted explicitly or earned by login, and how fresh.
type Access struct {
	PubKey [32]byte
	// Role is the protocol's own word for what this entry may do —
	// named at the boundary, so the console renders and never decodes.
	Role       string
	Granted    bool
	LastActive time.Time
}

// MQTTInfo is what the CLI knows about one observer connection.
type MQTTInfo struct {
	Name string
	// Disabled marks a parked connection: configured, deliberately
	// not running.
	Disabled bool
	// Down carries why a configured observer is not running — empty
	// when it runs, or when it was parked on purpose.
	Down  string
	URL   string
	Relay string
	// Connected reports the broker session's state right now; nil
	// when the observer is not running at all.
	Connected func() bool
	// Published, PublishErrors, BusDropped, Filtered and LastPublished
	// come back together from Counters.
	Counters func() (published, pubErrors, busDropped, filtered uint64, last time.Time)
}

// RadioInfo is what the CLI knows about one radio attachment.
type RadioInfo struct {
	Name         string
	Driver       string
	Envelope     radio.Envelope
	Relay        string
	Stations     []string
	Applications []string
	Authority    string
	State, Cause string
}

// SensorInfo is one configured part as the console shows it. No
// relay owns it, so it names none: the bus belongs to the machine.
type SensorInfo struct {
	Name           string
	Driver         string
	SampleInterval time.Duration
	// Running is false for a part that would not open — a bus that is
	// not there, a permission the unit does not grant.
	Running bool
	// Cause is why it is not running, in the driver's own words.
	Cause string
	// Readings is the sampler's last answer, empty until it has one.
	Readings []sensor.Reading
}

// Deps is everything the commands may consult. Sentinel may be nil —
// the commands that need it say so instead of pretending.
type Deps struct {
	// Log tells the sessions' life — opened, closed, accept trouble.
	// Nil is quiet, which is what the tests want.
	Log *zap.Logger

	Version string
	// Revision is the build's short commit, beside Version on the
	// status row — empty when the build carries none.
	Revision     string
	Started      time.Time
	Relays       []RelayInfo
	Stations     []StationInfo
	Applications []ApplicationInfo
	Radios       []RadioInfo
	Sensors      []SensorInfo
	Sentinel     *sentinel.Sentinel
	Bus          *bus.Bus
	// Privilege is the session's rights, set per listener by the
	// transport; empty reads as ReadOnly.
	Privilege Privilege
	// Traces holds the resolved-config provenance recorded at
	// assembly, keyed "radio <name>" and "relay <name>".
	Traces map[string][]config.Trace
	// Kinds is the configuration vocabulary — every kind of object,
	// its attributes and their docs — from which the console derives
	// contexts, help and completion.
	Kinds []schema.Kind
	// The live views, set by a daemon whose relays can be rebuilt
	// under a running session: they always name the current engine,
	// where the plain fields above froze at startup. Optional — tests
	// and static deployments use the fields.
	LiveRelays       func() []RelayInfo
	LiveStations     func() []StationInfo
	LiveApplications func() []ApplicationInfo
	LiveRadios       func() []RadioInfo
	// LiveSensors lists the configured parts as the daemon holds them.
	LiveSensors func() []SensorInfo
	// History reads the configuration's revision journal, newest
	// first — who changed what, when — and says how many the asked
	// window holds beyond what came back, so a capped listing can
	// confess. Nil hides the drawer's content behind its empty line.
	History func(ctx context.Context, q HistoryQuery) ([]HistoryEntry, int, error)
	// LiveMQTTs lists the observer connections as they run.
	LiveMQTTs  func() []MQTTInfo
	LiveTraces func() map[string][]config.Trace
	// Layers reads one instance's PERSISTED layering — the selected
	// profile and every override scope, inactive ones included. The
	// resolved traces cannot stand in for it: they only know the
	// active scope, and an export built from them silently lost the
	// settings prepared for every other band, board and broker. Nil
	// falls back to the resolved view.
	Layers func(kind, name string) (profile string, overrides map[string]map[string]any, ok bool)
	// Mutate applies configuration changes — parse, validate, persist,
	// bounce the owning relay — and says what happened. Nil when this
	// daemon has no mutation channel.
	Mutate func(ctx context.Context, kind, name string, set map[string]string,
		unset []string, principal string) (string, error)
	// Undo inverts the newest recorded mutation.
	Undo func(ctx context.Context, principal string) (string, error)
	// Create brings a new instance into existence; Remove takes one
	// out. Both go through the same manager door as Mutate.
	Create func(ctx context.Context, kind, name string, attrs map[string]string,
		principal string) (string, error)
	Remove func(ctx context.Context, kind, name, principal string) (string, error)
	// Sessions is the table of live console sessions, shared across
	// the listeners so any session can see the others. Nil means no
	// introspection.
	Sessions *Sessions
	// UpdateTrust resolves the verification keys for the update
	// channels; nil takes the built-in store. Tests inject theirs.
	UpdateTrust func() ([]update.PublicKey, error)
	// UpdateProbation reports the running daemon's actual liveness deadline
	// and any failure. Nil means no check was started by this process.
	UpdateProbation func() update.ProbationStatus
	// StateDir is where the daemon may stage an update; DBPath is the
	// configuration database, which a staged binary's selfcheck
	// reads. Empty disables installing.
	StateDir string
	DBPath   string
	// SystemName is what this installation calls itself — the prompt's
	// right-hand side, and the name a browser will show. Nil falls
	// back to the product's own name.
	SystemName func() string
}

// maxLineBytes bounds one command line: a client that never sends a
// newline exhausts this, not the daemon's memory.
const maxLineBytes = 4096

// session is one connected operator. Lines arrive from a single input
// owner, so watches can select on commands without competing for reads.
type session struct {
	deps  Deps
	lines <-chan string
	out   io.Writer
	// quitting is set when the operator asked to leave from inside a
	// nested command (a watch stopped by "quit").
	quitting bool
	// colors says the transport is a raw terminal that renders ANSI;
	// piped sessions read plain text.
	colors bool
	// greeted is set when negotiation already displayed the plain
	// banner and prompt while waiting for the peer's first input.
	greeted bool
	// The introspection fields: who this session is when another one
	// looks at it through /cli/sessions. id and began are set once at
	// registration, remote at construction; watching travels under mu
	// because other sessions read it live.
	id     string
	remote string
	began  time.Time
	// mu guards path — the editor's hooks read it from the transport
	// goroutine while commands move it from the REPL's — and watching.
	// It also protects colors while initial negotiation is visible to
	// other sessions; the mode is immutable once the REPL starts.
	mu       sync.Mutex
	path     []string
	watching bool
}

// Serve runs the REPL on plain line input — pipes, scripts, tests.
func Serve(ctx context.Context, rw io.ReadWriter, deps Deps) {
	s := &session{deps: deps, out: syncOut(rw), remote: remoteOf(rw)}
	s.servePlain(ctx, rw)
}

func (s *session) servePlain(ctx context.Context, r io.Reader) {
	lines := make(chan string)
	done := make(chan struct{})
	defer close(done)
	go readLines(r, lines, done)
	s.lines = lines
	s.repl(ctx)
}

// terminalGrace bounds the wait before the banner becomes visible.
// Silence is not proof of a pipe: mode selection remains open until a
// cursor report, a terminal capability, or the first complete line.
const terminalGrace = 300 * time.Millisecond

// ServeAuto chooses a session's mode once, before dispatching input.
// A client can identify its raw terminal with NAWS; legacy terminals
// can answer the cursor query. Typing ahead of that answer is retained,
// and a silent peer sees the banner after terminalGrace. Pipes need no
// negotiation: their first line or EOF selects plain input immediately.
func ServeAuto(ctx context.Context, rw io.ReadWriter, deps Deps) {
	s := &session{deps: deps, out: syncOut(rw), remote: remoteOf(rw)}
	defer s.register()()
	conn, clocked := rw.(interface {
		SetReadDeadline(deadline time.Time) error
	})
	if !clocked {
		s.servePlain(ctx, rw)
		return
	}
	terminal, width, eaten := s.negotiateTerminal(ctx, rw, conn)
	if ctx.Err() != nil {
		return
	}
	input := &replayedTerminal{Reader: io.MultiReader(bytes.NewReader(eaten), rw), source: rw}
	if terminal {
		s.mu.Lock()
		s.colors = true
		s.mu.Unlock()
		if s.greeted {
			// The plain prompt may already be on screen. Repaint its
			// one line with the chosen terminal palette, once.
			fmt.Fprint(s.out, "\r\x1b[K", s.prompt())
		}
		s.serveEdited(ctx, input, width)
		return
	}
	s.servePlain(ctx, input)
}

// negotiateTerminal owns the transport's probe state and deadlines only
// until it returns. No cleanup may mutate that state after the session's
// reader goroutine starts using the same stream.
func (s *session) negotiateTerminal(ctx context.Context, rw io.ReadWriter,
	conn interface {
		SetReadDeadline(deadline time.Time) error
	},
) (bool, int, []byte) {
	// Wake the initial read too: unlike the reader goroutines used by
	// an established session, negotiation runs before repl can select
	// on cancellation. Wait for an already-running callback before
	// clearing the deadline so cancellation cannot strand the read.
	wakeDone := make(chan struct{})
	wake := context.AfterFunc(ctx, func() {
		_ = conn.SetReadDeadline(time.Now())
		close(wakeDone)
	})
	defer func() {
		if !wake() {
			<-wakeDone
		}
		_ = conn.SetReadDeadline(time.Time{})
	}()
	if probe, ok := rw.(interface{ terminalProbe(enabled bool) }); ok {
		probe.terminalProbe(true)
		defer probe.terminalProbe(false)
	}
	_, _ = rw.Write([]byte("\x1b[6n"))
	_ = conn.SetReadDeadline(time.Now().Add(terminalGrace))
	terminal, width, eaten := awaitTerminal(ctx, rw, func() {
		s.greet()
		_ = conn.SetReadDeadline(time.Time{})
	})
	if ctx.Err() != nil {
		return false, 0, nil
	}
	if probe, ok := rw.(interface{ terminalProbe(enabled bool) }); ok {
		probe.terminalProbe(false)
	}
	_ = conn.SetReadDeadline(time.Time{})
	announced, _ := terminalSizeOf(rw)
	if terminal && !announced {
		var more []byte
		width, more = measureWidth(rw)
		eaten = append(eaten, more...)
	}
	return terminal, width, eaten
}

// awaitTerminal holds at most one command plus a bounded report prefix.
// A read timeout only makes the greeting visible; it does not turn a
// late terminal answer into a command. An entire first line is decisive
// for a legacy peer, keeping scripts independent of the probe timeout.
func awaitTerminal(ctx context.Context, rw io.Reader, idle func()) (terminal bool, width int, eaten []byte) {
	var input terminalInput
	var buf [1]byte
	for len(input.typed) < maxLineBytes && ctx.Err() == nil {
		n, err := rw.Read(buf[:])
		if n > 0 && input.accept(buf[0]) {
			return true, 0, input.typed
		}
		if terminal, columns := terminalSizeOf(rw); terminal {
			return true, columns, input.replay()
		}
		if n > 0 && (buf[0] == '\r' || buf[0] == '\n') {
			break
		}
		var timeout net.Error
		if errors.As(err, &timeout) && timeout.Timeout() && ctx.Err() == nil && idle != nil {
			idle()
			idle = nil
			continue
		}
		if n == 0 || err != nil {
			break
		}
	}
	return false, 0, input.replay()
}

// terminalInput separates a possible cursor report from the command
// prefix without assigning terminal meaning to any ordinary keystroke.
type terminalInput struct {
	typed  []byte
	report []byte
}

func (p *terminalInput) accept(c byte) bool {
	if c == 0x1b {
		p.typed = append(p.typed, p.report...)
		p.report = p.report[:0]
	}
	if len(p.report) == 0 && c != 0x1b {
		p.typed = append(p.typed, c)
		return false
	}
	p.report = append(p.report, c)
	if couldBeCursorReport(p.report) {
		return c == 'R'
	}
	p.typed = append(p.typed, p.report...)
	p.report = p.report[:0]
	return false
}

func (p *terminalInput) replay() []byte { return append(p.typed, p.report...) }

func terminalSizeOf(r any) (terminal bool, width int) {
	if peer, ok := r.(interface{ terminalSize() (int, bool) }); ok {
		width, terminal = peer.terminalSize()
	}
	return terminal, width
}

// measureWidth is only used by legacy terminals without NAWS. Input
// before the report is replayed, while the editor will absorb any report
// that arrives after this bounded measurement. Restore the cursor even
// on timeout: it was moved before the question was sent.
func measureWidth(rw io.ReadWriter) (width int, eaten []byte) {
	conn, ok := rw.(interface {
		SetReadDeadline(deadline time.Time) error
	})
	if !ok {
		return 0, nil
	}
	if _, err := rw.Write([]byte("\x1b[9999C\x1b[6n")); err != nil {
		return 0, nil
	}
	defer func() {
		_ = conn.SetReadDeadline(time.Time{})
		_, _ = rw.Write([]byte("\r"))
	}()
	if err := conn.SetReadDeadline(time.Now().Add(terminalGrace)); err != nil {
		return 0, nil
	}
	var got []byte
	var buf [1]byte
	for len(got) < cursorReportMax {
		n, err := rw.Read(buf[:])
		if n > 0 {
			got = append(got, buf[0])
			if !couldBeCursorReport(got) {
				return 0, got
			}
			if buf[0] == 'R' {
				return reportColumns(got), nil
			}
		}
		if n == 0 || err != nil {
			return 0, got
		}
	}
	return 0, got
}

// reportColumns reads the column out of ESC [ rows ; cols R.
func reportColumns(report []byte) int {
	text := string(report)
	i := strings.IndexByte(text, ';')
	if i < 0 || !strings.HasSuffix(text, "R") {
		return 0
	}
	cols, err := strconv.Atoi(text[i+1 : len(text)-1])
	if err != nil || cols < 1 {
		return 0
	}
	return cols
}

// cursorReportMax bounds both report detection and editor parameters.
const cursorReportMax = 32

// couldBeCursorReport accepts only prefixes of ESC [ row ; column R,
// with positive decimal coordinates. Other CSI sequences remain input.
func couldBeCursorReport(b []byte) bool {
	if len(b) == 0 || len(b) > cursorReportMax || b[0] != 0x1b {
		return false
	}
	if len(b) == 1 {
		return true
	}
	if b[1] != '[' {
		return false
	}
	row, col, separator := false, false, false
	for i, c := range b[2:] {
		switch {
		case c >= '0' && c <= '9':
			if separator {
				col = col || c != '0'
			} else {
				row = row || c != '0'
			}
		case c == ';' && row && !separator:
			separator = true
		case c == 'R':
			return row && col && separator && i == len(b)-3
		default:
			return false
		}
	}
	return true
}

// ServeEdited runs the REPL behind the character-mode line editor:
// history on the arrows, a movable cursor, the daemon echoing. The
// transport delivers raw keystrokes (the telnet listener negotiates
// that; the console client sets its terminal raw).
func ServeEdited(ctx context.Context, rw io.ReadWriter, deps Deps) {
	serveEdited(ctx, rw, deps, 0)
}

// serveEdited is ServeEdited told how wide the terminal is; zero
// leaves the editor wrap-blind.
func serveEdited(ctx context.Context, rw io.ReadWriter, deps Deps, width int) {
	s := &session{deps: deps, out: syncOut(rw), colors: true, remote: remoteOf(rw)}
	s.serveEdited(ctx, rw, width)
}

func (s *session) serveEdited(ctx context.Context, r io.Reader, width int) {
	defer s.register()()
	s.serveTerminal(ctx, r, width)
}

func (s *session) greet() {
	banner(s.out, s.deps.Version, s.systemName(), s.deps.Privilege)
	fmt.Fprint(s.out, s.prompt())
	s.greeted = true
}

// repl presents a plain transcript. Edited sessions use the same
// runCommands loop and ask their display owner to present each prompt.
func (s *session) repl(ctx context.Context) {
	defer s.register()()
	if !s.greeted {
		s.greet()
	}
	s.greeted = false
	s.runCommands(ctx, func() bool {
		fmt.Fprint(s.out, s.prompt())
		return true
	})
}

// runCommands is shared by transcript and terminal sessions. The caller
// owns presentation of the next prompt; watch still receives from the
// same line channel and can run the command that ends it.
func (s *session) runCommands(ctx context.Context, nextPrompt func() bool) {
	for ctx.Err() == nil {
		select {
		case <-ctx.Done():
			return
		case line, ok := <-s.lines:
			if !ok {
				return
			}
			s.command(ctx, line)
			if s.quitting || ctx.Err() != nil || !nextPrompt() {
				return
			}
		}
	}
}

// afterWatchCommand echoes an interactive watch's terminating command
// after the view has finished its cleanup, then executes the same line.
// Plain transcripts already supplied their own input and need no echo.
func (s *session) afterWatchCommand(ctx context.Context, line string, ok bool) {
	if !ok || line == "" {
		return
	}
	if out, supported := s.out.(*syncWriter); supported {
		out.echoCommand(line)
	}
	s.command(ctx, line)
}

// command runs one line; quit reports itself through s.quitting.
// Lines the tree grammar claims — absolute paths, and everything once
// the session stands in a context — go to it; the rest is the flat
// command set, exactly as it always was.
func (s *session) command(ctx context.Context, line string) {
	if s.treeLine(line) {
		if line != "" {
			s.tree(ctx, line)
		}
		return
	}
	if args := splitArgs(line); len(args) > 0 {
		s.rootDispatch(ctx, args)
	}
}

// rootDispatch runs a flat command typed at the root, refusing the
// ones that live somewhere more specific by saying where. The old
// answer — quietly acting on the only relay — was multi-relay hidden
// rather than handled: a command that acts on one instance runs where
// the instance is, and nowhere is not an instance.
func (s *session) rootDispatch(ctx context.Context, args []string) {
	c := lookup(args[0])
	if c == nil || commandHome(c) == "" || slices.Contains(args[1:], helpWord) {
		// A question is answerable anywhere — refusing "scopes ?"
		// would refuse a question — and the help itself says where
		// the command lives.
		s.dispatch(ctx, args)
		return
	}
	fmt.Fprintf(s.out, "error: %s lives in %s — stand there\r\n", c.name, commandHome(c))
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

// readBounded reads one line, ending it on either terminator. Both
// have to work here because the editor accepts both: a peer whose
// Enter the daemon honours on one path and swallows on the other is a
// peer that cannot tell which path it got.
func readBounded(br *bufio.Reader) (string, error) {
	var b strings.Builder
	for {
		c, err := br.ReadByte()
		if err != nil {
			return strings.TrimSpace(b.String()), err
		}
		if c == '\n' || c == '\r' {
			if c == '\r' {
				// A telnet peer sends CR LF or CR NUL; the partner is
				// not an empty line of its own.
				if next, perr := br.Peek(1); perr == nil && (next[0] == '\n' || next[0] == 0) {
					_, _ = br.Discard(1)
				}
			}
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
	case slices.Contains(rest, helpWord):
		// "?" asks about a command in a line the way the key asks
		// about it under the fingers — one question, two ways to put
		// it, and no punctuation to remember for either.
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
	names := make([]string, 0, len(s.relays()))
	for _, r := range s.relays() {
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

// RegionInfo is a relay's region state as the console reads it: the
// reference's own tree render, the carried names, the designations,
// and the entries for the drawer.
type RegionInfo struct {
	Tree     string
	Served   []string
	Default  string // bare name; empty when the relay speaks unscoped
	Home     string // bare name; "*" while none is designated
	Entries  []RegionEntry
	Unscoped bool // whether plain floods are carried
}

// RegionEntry is one region for the drawer: names resolved, the flood
// policy spelled out.
type RegionEntry struct {
	Name    string
	Parent  string // "*" for a top-level region
	Flood   bool   // whether floods in this region are carried
	Home    bool
	Default bool
}
