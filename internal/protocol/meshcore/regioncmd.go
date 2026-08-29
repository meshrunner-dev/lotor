package meshcore

// The region command door: one grammar, the reference's, serving the
// air and the console with identical replies. Every mutation composes
// on a clone of the map, persists, and only then installs — a store
// that refuses leaves the live table exactly as it was — and none of
// it bounces the relay: the verdict, the reply scope and the advert
// origination read the table on the pipeline's own turn.

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"go.uber.org/zap"

	"meshrunner.dev/pkg/meshcore"
)

// PersistedRegions is one relay's whole region map as the store keeps
// it: entries in wire order plus the remainder the map carries.
type PersistedRegions struct {
	Entries       []meshcore.Region
	NextID        uint16
	HomeID        uint16
	DefaultID     uint16
	WildcardFlags uint8
}

// RegionStore persists the region map. Load reports nil when the
// relay was never written — the engine keeps its fresh defaults —
// and Save replaces the state whole.
type RegionStore interface {
	LoadRegions() (*PersistedRegions, error)
	SaveRegions(state PersistedRegions) error
}

// AttachRegions gives the engine somewhere to persist its region map
// and loads what is already there. Called once, before Run. A store
// that cannot be read refuses the attachment, and with it the relay:
// the table is the relaying policy, and coming up without it would
// carry floods the operator has denied.
func (e *engine) AttachRegions(store RegionStore) error {
	e.regionStore = store
	pr, err := store.LoadRegions()
	if err != nil {
		return fmt.Errorf("regions: the store holds this relay's flood policy and could not be read: %w", err)
	}
	if pr == nil {
		return nil // never written: the fresh defaults stand
	}
	m, err := meshcore.RestoreRegionMap(pr.Entries,
		meshcore.RegionFlags(pr.WildcardFlags), pr.NextID, pr.HomeID, pr.DefaultID)
	if err != nil {
		return fmt.Errorf("regions: %w", err)
	}
	e.regions = newRegionTable(m)
	if n := m.Count(); n > 0 {
		e.log.Info("regions restored", zap.Int("count", n))
	}
	return nil
}

// regionReplyBudget is the reference's reply buffer: 160 chars, NUL
// included, so 159 usable — the tree and the CSV both cut there.
const regionReplyBudget = 159

// The reference's own refusals, spelled once.
const (
	regionErrSyntax  = "Err - ??"
	regionErrUnknown = "Err - unknown region"
)

// regionLoadWindow expires an armed `region load` staging that hears
// nothing: an admin who walked away mid-load must not leave the door
// swallowing every later line.
const regionLoadWindow = time.Minute

// regionOrder carries one command line into the pipeline's goroutine,
// which owns the map. The done ack arbitrates the deadline exactly as
// a grant's does: a mutation whose author was told it never happened
// must not happen afterwards.
type regionOrder struct {
	owner string
	line  string
	// reply and handled are written before the ack answers; handled
	// false means the line was not region business after all and the
	// caller dispatches it as whatever else it is.
	reply   string
	handled bool
	done    *ack
}

// regionLoad is an armed `region load` staging: whose it is, where it
// stands, and when it lapses. Engine-goroutine only.
type regionLoad struct {
	owner  string
	loader *meshcore.RegionLoader
	until  time.Time
}

// RegionLoadArmed reports whether a load staging is armed for this
// owner — the cheap pre-check a dispatcher makes before routing every
// line of a modal load through the order channel. The engine goroutine
// writes it; anyone may read.
func (e *engine) RegionLoadArmed(owner string) bool {
	v, _ := e.regionLoadState.Load().(regionLoadHint)
	return v.owner == owner && time.Now().Before(v.until)
}

// regionLoadHint mirrors the staging for RegionLoadArmed.
type regionLoadHint struct {
	owner string
	until time.Time
}

// RegionCommand runs one line through the region door: a `region …`
// command, or — while a load staging is armed for this owner — any
// line at all, which is how the reference's modal load consumes its
// dump. handled false means the line was neither, and the caller owns
// it. Any goroutine; the pipeline answers on its own turn.
func (e *engine) RegionCommand(owner, line string) (reply string, handled bool, err error) {
	o := &regionOrder{owner: owner, line: line, done: newAck()}
	select {
	case e.regionAsk <- o:
	default:
		return "", true, errors.New("a region command is already pending")
	}
	e.wakeReceiver()
	if err := o.done.wait("region command"); err != nil {
		return "", true, err
	}
	return o.reply, o.handled, nil
}

// drainRegionAsk serves a pending region command on the pipeline's
// turn. Like the session snapshot, it asks for no emission, so it is
// served whatever the gate's mode.
func (e *engine) drainRegionAsk() {
	select {
	case o := <-e.regionAsk:
		if !o.done.claim() {
			break
		}
		o.reply, o.handled = e.serveRegionLine(o.owner, o.line)
		o.done.taken()
	default:
	}
}

// serveRegionLine is the dispatcher the reference runs first on every
// command: the modal staging when one is armed for this owner, the
// region grammar when the line speaks it, nothing otherwise.
func (e *engine) serveRegionLine(owner, line string) (reply string, handled bool) {
	if e.regionStaging != nil {
		if time.Now().After(e.regionStaging.until) {
			e.dropRegionStaging("expired")
		} else if e.regionStaging.owner == owner {
			return e.serveRegionLoadLine(line), true
		}
	}
	trimmed := strings.TrimLeft(line, " ")
	if trimmed == "region" || strings.HasPrefix(trimmed, "region ") {
		return e.serveRegionCommand(owner, trimmed), true
	}
	return "", false
}

// dropRegionStaging discards an armed staging and says why.
func (e *engine) dropRegionStaging(why string) {
	e.log.Info("region load staging dropped", zap.String("why", why),
		zap.String("owner", e.regionStaging.owner))
	e.regionStaging = nil
	e.regionLoadState.Store(regionLoadHint{})
}

// serveRegionLoadLine feeds one modal line: blank commits the staged
// map — persisted before it is installed — and anything else stages,
// silently, exactly as the reference's load does.
func (e *engine) serveRegionLoadLine(line string) string {
	if strings.TrimSpace(line) != "" {
		e.regionStaging.loader.Line(line)
		e.regionStaging.until = time.Now().Add(regionLoadWindow)
		e.regionLoadState.Store(regionLoadHint{
			owner: e.regionStaging.owner, until: e.regionStaging.until})
		return ""
	}
	fresh := e.regionStaging.loader.Commit()
	e.regionStaging = nil
	e.regionLoadState.Store(regionLoadHint{})
	if err := e.installRegions(fresh); err != nil {
		return "Err - " + err.Error()
	}
	return fmt.Sprintf("OK - loaded %d regions", fresh.Count())
}

// installRegions persists a composed map and only then makes it live,
// re-deriving the speaking key — the compose-then-install discipline
// every mutation here follows.
func (e *engine) installRegions(m *meshcore.RegionMap) error {
	if e.regionStore != nil {
		if err := e.regionStore.SaveRegions(PersistedRegions{
			Entries:       m.Entries(),
			NextID:        m.NextID(),
			HomeID:        m.HomeID(),
			DefaultID:     m.DefaultID(),
			WildcardFlags: uint8(m.Wildcard().Flags),
		}); err != nil {
			return fmt.Errorf("the change would not persist: %w", err)
		}
	}
	e.regions.m = m
	e.regions.rederive()
	return nil
}

// cloneRegions composes a candidate: the live map rebuilt whole, which
// Restore validates on the way — a table the engine itself produced
// cannot fail it.
func (e *engine) cloneRegions() *meshcore.RegionMap {
	live := e.regions.m
	m, err := meshcore.RestoreRegionMap(live.Entries(),
		live.Wildcard().Flags, live.NextID(), live.HomeID(), live.DefaultID())
	if err != nil {
		// Unreachable by construction; refusing to mutate beats
		// mutating a table that no longer restores.
		e.log.Error("live region table failed its own restore", zap.Error(err))
		return nil
	}
	return m
}

// serveRegionCommand runs one `region …` line, replies included, in
// the reference's exact wording (CommonCLI handleRegionCmd).
func (e *engine) serveRegionCommand(owner, line string) string {
	line = strings.TrimRight(line, " ")
	// `region def` first: its payload is the rest of the line whole,
	// before any word splitting.
	if rest, ok := strings.CutPrefix(line, "region def"); ok && (rest == "" || rest[0] == ' ') {
		return e.regionDef(rest)
	}
	parts := regionParts(line)
	switch {
	case len(parts) == 1:
		return regionClip(e.regions.m.ExportTree())
	case parts[1] == "load":
		e.regionStaging = &regionLoad{
			owner:  owner,
			loader: e.regions.m.StartLoad(),
			until:  time.Now().Add(regionLoadWindow),
		}
		e.regionLoadState.Store(regionLoadHint{owner: owner, until: e.regionStaging.until})
		return ""
	case parts[1] == "save":
		// Every mutation here already persisted; save keeps its one
		// upstream side effect — this node is now "modified" for the
		// discovery info — and answers as the reference does.
		e.discoverySince = time.Now()
		return "OK"
	case len(parts) >= 3 && parts[1] == "allowf":
		return e.regionFlag(parts[2], false)
	case len(parts) >= 3 && parts[1] == "denyf":
		return e.regionFlag(parts[2], true)
	default:
		return e.serveRegionVerb(parts)
	}
}

// serveRegionVerb is the rest of the grammar: the designations and
// the table verbs.
func (e *engine) serveRegionVerb(parts []string) string {
	switch {
	case len(parts) >= 3 && parts[1] == "get":
		return e.regionGet(parts[2])
	case len(parts) >= 3 && parts[1] == "home":
		return e.regionHome(parts[2])
	case len(parts) == 2 && parts[1] == "home":
		home := e.regions.m.Home()
		name := wildcardRegion
		if home != nil {
			name = home.Name
		}
		return " home is " + name
	case len(parts) >= 3 && parts[1] == "default":
		return e.regionDefault(parts[2])
	case len(parts) == 2 && parts[1] == "default":
		name := "<null>"
		if def := e.regions.m.Default(); def != nil {
			name = def.Name
		}
		return " default scope is " + name
	default:
		return e.serveRegionTableVerb(parts)
	}
}

// serveRegionTableVerb is the table's own verbs.
func (e *engine) serveRegionTableVerb(parts []string) string {
	switch {
	case len(parts) >= 3 && parts[1] == "put":
		parent := ""
		if len(parts) >= 4 {
			parent = parts[3]
		}
		return e.regionPut(parts[2], parent)
	case len(parts) >= 3 && parts[1] == "remove":
		return e.regionRemove(parts[2])
	case len(parts) >= 3 && parts[1] == "list":
		return e.regionList(parts[2])
	default:
		return regionErrSyntax
	}
}

// regionParts is the reference's parseTextParts: split on single
// spaces, at most four parts, the rest of the line dropped.
func regionParts(line string) []string {
	parts := strings.SplitN(line, " ", 5)
	if len(parts) > 4 {
		parts = parts[:4]
	}
	if len(parts) == 4 {
		parts[3], _, _ = strings.Cut(parts[3], " ")
	}
	return parts
}

// regionClip cuts a reply at the reference's buffer, byte-exact.
func regionClip(s string) string {
	if len(s) > regionReplyBudget {
		return s[:regionReplyBudget]
	}
	return s
}

// regionDef runs the def DSL on a candidate and installs it. The
// reference applies segments with no rollback; composing on the clone
// keeps its replies while a refused batch leaves the live table
// untouched — same words, sounder outcome.
func (e *engine) regionDef(payload string) string {
	m := e.cloneRegions()
	if m == nil {
		return regionErrSyntax
	}
	if err := m.ApplyDef(payload); err != nil {
		if def, ok := errors.AsType[*meshcore.RegionDefError](err); ok {
			return "Err - " + def.Reason
		}
		return regionErrSyntax
	}
	if err := e.installRegions(m); err != nil {
		return "Err - " + err.Error()
	}
	return regionClip(e.regions.m.ExportTree())
}

// regionFlag is allowf/denyf: the flood bit on the region a prefix
// resolves to — the wildcard included, which is how plain floods are
// denied over the air.
func (e *engine) regionFlag(prefix string, deny bool) string {
	m := e.cloneRegions()
	if m == nil {
		return regionErrSyntax
	}
	r := m.FindByPrefix(prefix)
	if r == nil {
		return regionErrUnknown
	}
	if deny {
		r.Flags |= meshcore.RegionDenyFlood
	} else {
		r.Flags &^= meshcore.RegionDenyFlood
	}
	if err := e.installRegions(m); err != nil {
		return "Err - " + err.Error()
	}
	return "OK"
}

// regionGet renders one region: name, parent when it has a real one,
// and the flood mark — the reference's exact spacing.
func (e *engine) regionGet(prefix string) string {
	r := e.regions.m.FindByPrefix(prefix)
	if r == nil {
		return regionErrUnknown
	}
	flood := "F"
	if r.Flags&meshcore.RegionDenyFlood != 0 {
		flood = ""
	}
	if parent := e.regions.m.FindByID(r.Parent); parent != nil && parent.ID != 0 {
		return fmt.Sprintf(" %s (%s) %s", r.Name, parent.Name, flood)
	}
	return fmt.Sprintf(" %s %s", r.Name, flood)
}

// regionHome designates the home region.
func (e *engine) regionHome(prefix string) string {
	m := e.cloneRegions()
	if m == nil {
		return regionErrSyntax
	}
	home := m.FindByPrefix(prefix)
	if home == nil {
		return regionErrUnknown
	}
	m.SetHome(home)
	if err := e.installRegions(m); err != nil {
		return "Err - " + err.Error()
	}
	return " home is now " + home.Name
}

// regionDefault designates — or clears — the region this relay
// speaks in. An unknown name is created, as the reference
// auto-creates on designation, flood allowed either way.
func (e *engine) regionDefault(name string) string {
	m := e.cloneRegions()
	if m == nil {
		return regionErrSyntax
	}
	if name == "<null>" {
		m.SetDefault(nil)
		if err := e.installRegions(m); err != nil {
			return "Err - " + err.Error()
		}
		return " default scope is now <null>"
	}
	def := m.FindByPrefix(name)
	if def == nil {
		if err := checkRegionName(name); err != nil {
			return "Err - " + err.Error()
		}
		created, err := m.Put(name, 0)
		if err != nil {
			return "Err - region table full"
		}
		def = created
	}
	def.Flags = 0
	m.SetDefault(def)
	if err := e.installRegions(m); err != nil {
		return "Err - " + err.Error()
	}
	return " default scope is now " + def.Name
}

// regionPut creates or re-parents a region, flood allowed — the
// reference's put path clears the flags it created denied.
func (e *engine) regionPut(name, parentPrefix string) string {
	m := e.cloneRegions()
	if m == nil {
		return regionErrSyntax
	}
	parent := m.Wildcard()
	if parentPrefix != "" {
		if parent = m.FindByPrefix(parentPrefix); parent == nil {
			return "Err - unknown parent"
		}
	}
	if err := checkRegionName(name); err != nil {
		return "Err - " + err.Error()
	}
	r, err := m.Put(name, parent.ID)
	if err != nil {
		return "Err - unable to put"
	}
	r.Flags = 0
	if err := e.installRegions(m); err != nil {
		return "Err - " + err.Error()
	}
	return "OK - (flood allowed)"
}

// regionRemove deletes by exact name — the one verb that takes no
// prefix, because what is destroyed is looked up, not guessed at.
func (e *engine) regionRemove(name string) string {
	m := e.cloneRegions()
	if m == nil {
		return regionErrSyntax
	}
	switch err := m.Remove(name); {
	case errors.Is(err, meshcore.ErrRegionNotFound):
		return "Err - not found"
	case errors.Is(err, meshcore.ErrRegionNotEmpty):
		return "Err - not empty"
	case err != nil:
		return regionErrSyntax
	}
	if err := e.installRegions(m); err != nil {
		return "Err - " + err.Error()
	}
	return "OK"
}

// regionList is the CSV filter: allowed or denied, nothing else.
func (e *engine) regionList(which string) string {
	var invert bool
	switch which {
	case "allowed":
		invert = false
	case "denied":
		invert = true
	default:
		return "Err - use 'allowed' or 'denied'"
	}
	names := e.regions.m.ExportNames(meshcore.RegionDenyFlood, invert, regionReplyBudget+1)
	if names == "" {
		return "-none-"
	}
	return names
}

// RegionSnapshot is the region state as outside readers see it: the
// console's drawer, the observers, the summary line.
type RegionSnapshot struct {
	Tree     string
	Served   []string
	Default  string // bare name; empty when the relay speaks unscoped
	Home     string // bare name; "*" while none is designated
	Entries  []meshcore.Region
	Unscoped bool // whether plain floods are carried
}

// regionSnapOrder asks the pipeline for a snapshot.
type regionSnapOrder struct {
	reply chan RegionSnapshot
}

// Regions reports the region state — any goroutine.
func (e *engine) Regions() (RegionSnapshot, error) {
	o := &regionSnapOrder{reply: make(chan RegionSnapshot, 1)}
	select {
	case e.regionSnapAsk <- o:
	default:
		return RegionSnapshot{}, errors.New("a region snapshot is already pending")
	}
	e.wakeReceiver()
	select {
	case snap := <-o.reply:
		return snap, nil
	case <-time.After(askWait):
		return RegionSnapshot{}, errors.New("the relay never picked the region snapshot up")
	}
}

// drainRegionSnapAsk serves a pending snapshot on the pipeline's turn.
func (e *engine) drainRegionSnapAsk() {
	select {
	case o := <-e.regionSnapAsk:
		o.reply <- e.regionSnapshot()
	default:
	}
}

// regionSnapshot composes the outside view from the live table.
func (e *engine) regionSnapshot() RegionSnapshot {
	m := e.regions.m
	snap := RegionSnapshot{
		Tree:     m.ExportTree(),
		Served:   e.regions.served(),
		Home:     wildcardRegion,
		Entries:  m.Entries(),
		Unscoped: m.Wildcard().Flags&meshcore.RegionDenyFlood == 0,
	}
	if d := m.Default(); d != nil {
		snap.Default = d.BareName()
	}
	if h := m.Home(); h != nil && h.ID != 0 {
		snap.Home = h.BareName()
	}
	return snap
}

// Scopes reports what this relay carries, for the console and the
// observers — the same list the anonymous answer gives the mesh. Any
// goroutine: it reads through the snapshot order.
func (e *engine) Scopes() []string {
	snap, err := e.Regions()
	if err != nil {
		return nil
	}
	return snap.Served
}
