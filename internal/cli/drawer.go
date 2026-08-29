package cli

import (
	"context"
	"encoding/hex"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"
)

// A drawer is a runtime collection an instance holds: what the mesh is
// doing rather than what it was told to do. It is somewhere to stand
// and read, and nothing more — no attribute to set, nothing to export,
// because none of it was ever configured.
type drawer struct {
	name string
	doc  string
	// on names the kind that holds it — each instance of a collection
	// kind, or the block itself when the kind is a singleton, which
	// holds nothing between itself and what it holds.
	on string
	// verbs are the commands that belong here rather than on the
	// instance: what they act on is what the drawer holds, so this is
	// where an operator goes looking for them.
	verbs []string
	// itemVerbs are the commands about one of the things it holds,
	// and itemFlag is how such a command is told which one — filled
	// from the path, because standing on one is saying which.
	itemVerbs []string
	itemFlag  string
	// empty is what print says when the drawer holds nothing.
	empty string
	// keys names what stands inside — for the walker and for
	// completion, which ask from the terminal's goroutine and carry no
	// request context.
	keys func(s *session, instance string) map[string]string
	// view reads the drawer for printing; sel is the window the line
	// asked for, meaningful only where windowed says so.
	view func(s *session, ctx context.Context, instance string, sel frameSelectors) (drawerView, error)
	// windowed says the drawer answers the temporal selectors — the
	// vocabulary frames speaks, applied to what this drawer holds.
	windowed bool
}

// drawerView is a drawer as print shows it: the columns of its
// listing, the keys in display order, and one row per key. note, when
// set, follows the listing — the cap's confession, and anything else
// the view owes the reader beyond the rows.
type drawerView struct {
	header []string
	keys   []string
	rows   map[string][]field
	note   string
}

// field is one attribute of one thing a drawer holds.
type field struct {
	name, value string
	// rendered says the value already carries its own bounds — it
	// went through the one function allowed to render it — so no view
	// may wrap it again.
	rendered bool
}

// cells is a row for a table, where the columns do the separating.
func cells(row []field) []string {
	out := make([]string, len(row))
	for i, f := range row {
		out[i] = f.value
	}
	return out
}

// pairs is a row for the packed form, where a space would end a value
// early: what arrived rendered is left alone, the rest is quoted only
// where it needs to be.
func pairs(row []field) [][2]string {
	out := make([][2]string, len(row))
	for i, f := range row {
		v := f.value
		if !f.rendered {
			v = quoteIfSpaced(v)
		}
		out[i] = [2]string{f.name, v}
	}
	return out
}

// drawerSessions names two drawers, one per holder: the console's own
// sessions on /cli, the over-the-air ones on a relay. Same word on
// purpose — each reads as "the sessions this thing holds".
const (
	colKeyHdr        = "KEY"
	colNameHdr       = "NAME"
	fieldName        = "name"
	drawerNeighbours = "neighbours"
	drawerSessions   = "sessions"
	drawerHistory    = "history"
	drawerACL        = "acl"
	kindCLI          = "cli"
	kindSystem       = "system"
)

var drawers = []drawer{{
	name:      drawerNeighbours,
	doc:       "repeaters heard with no relay in between",
	on:        scopeRelay,
	verbs:     []string{cmdDiscover},
	itemVerbs: []string{cmdAskScopes},
	itemFlag:  optNeighbour,
	empty:     "nobody heard directly yet",
	keys:      (*session).neighbourKeys,
	view:      (*session).neighbourView,
}, {
	name:  drawerSessions,
	doc:   "who is on this console right now",
	on:    kindCLI,
	empty: "nobody connected", // unreachable: the reader is a session
	keys:  (*session).sessionKeys,
	view:  (*session).sessionView,
}, {
	name:  drawerSessions,
	doc:   "companions logged in over the air",
	on:    scopeRelay,
	empty: "nobody logged in",
	keys:  (*session).airSessionKeys,
	view:  (*session).airSessionView,
}, {
	name:      drawerACL,
	doc:       "who may administer this relay — grants and live sessions",
	on:        scopeRelay,
	verbs:     []string{cmdGrant},
	itemVerbs: []string{cmdRevoke},
	itemFlag:  optKey,
	empty:     "nobody granted, nobody logged in",
	keys:      (*session).accessKeys,
	view:      (*session).accessView,
}, {
	name:     drawerHistory,
	doc:      "the configuration's revision journal — who changed what, when",
	on:       kindSystem,
	empty:    "no changes recorded yet",
	keys:     (*session).historyKeys,
	view:     (*session).historyView,
	windowed: true,
}}

// historyDrawerDepth bounds what one print fetches: a journal, not a
// dump — the store keeps everything, the console shows the recent.
const historyDrawerDepth = 50

// history reads the revision journal through the dependency.
func (s *session) history(ctx context.Context, q HistoryQuery) ([]HistoryEntry, int, error) {
	if s.deps.History == nil {
		return nil, 0, nil
	}
	return s.deps.History(ctx, q)
}

// historyKeys answers the walker and completion: revision ids, as the
// operator would type them.
func (s *session) historyKeys(string) map[string]string {
	type result struct{ rows []HistoryEntry }
	ch := make(chan result, 1)
	go func() {
		rows, _, _ := s.history(context.Background(), HistoryQuery{Count: historyDrawerDepth})
		ch <- result{rows}
	}()
	select {
	case got := <-ch:
		out := map[string]string{}
		for _, r := range got.rows {
			out[strconv.FormatInt(r.ID, 10)] = r.Op + " " + historyWhat(r)
		}
		return out
	case <-time.After(completionBudget):
		return nil
	}
}

// historyView reads the journal for printing, newest first — the
// order a "what just happened" question is asked in. The selectors
// pick the slice, in frames' own vocabulary; around= takes a
// revision id.
func (s *session) historyView(ctx context.Context, _ string, sel frameSelectors) (drawerView, error) {
	q := HistoryQuery{
		Count: sel.count, Since: sel.since, Until: sel.until, Span: sel.span,
	}
	if sel.aroundPrefix != "" {
		id, err := strconv.ParseInt(sel.aroundPrefix, 10, 64)
		if err != nil {
			return drawerView{}, fmt.Errorf("%s= wants a revision id here", optAround)
		}
		q.AroundID = id
	}
	rows, total, err := s.history(ctx, q)
	if err != nil {
		return drawerView{}, err
	}
	v := drawerView{
		header: []string{"ID", "WHEN", "BY", "OP", "WHAT", "CHANGES"},
		rows:   map[string][]field{},
	}
	if total > len(rows) {
		v.note = fmt.Sprintf("newest %d of %d shown — narrow the window", len(rows), total)
	}
	for _, r := range rows {
		key := strconv.FormatInt(r.ID, 10)
		v.keys = append(v.keys, key)
		v.rows[key] = []field{
			{name: "when", value: ago(r.At)},
			{name: "by", value: r.Principal},
			{name: "op", value: r.Op},
			{name: "what", value: historyWhat(r), rendered: true},
			{name: "changes", value: historyChanges(r.Changes), rendered: true},
		}
	}
	return v, nil
}

// historyWhat names the object a revision touched; a singleton is its
// kind alone.
func historyWhat(r HistoryEntry) string {
	if r.Name == "" {
		return r.Kind
	}
	return r.Kind + " " + r.Name
}

// historyChanges spells the deltas, attribute by attribute. Long
// values are cut for the listing's sake — the journal is an answer to
// "what happened", and sqlite still holds every byte.
func historyChanges(deltas []AttrDelta) string {
	parts := make([]string, 0, len(deltas))
	for _, d := range deltas {
		switch {
		case d.Old == "":
			parts = append(parts, fmt.Sprintf("%s: → %s", d.Attr, historyCut(d.New)))
		case d.New == "":
			parts = append(parts, fmt.Sprintf("%s: %s → (unset)", d.Attr, historyCut(d.Old)))
		default:
			parts = append(parts, fmt.Sprintf("%s: %s → %s",
				d.Attr, historyCut(d.Old), historyCut(d.New)))
		}
	}
	return strings.Join(parts, ", ")
}

// historyCut bounds one value for the listing.
func historyCut(v string) string {
	const keep = 48
	if len(v) <= keep {
		return v
	}
	return v[:keep] + "…"
}

// airSessions reads a relay's over-the-air session table, refusing
// with the relay's own reason when it is down.
func (s *session) airSessions(instance string) ([]AirSession, error) {
	r, err := s.findRelay(instance)
	if err != nil {
		return nil, err
	}
	if err := working(r); err != nil {
		return nil, err
	}
	if r.AirSessions == nil {
		return nil, fmt.Errorf("relay %q keeps no over-the-air sessions", r.Name)
	}
	return r.AirSessions()
}

// airSessionKeys is the walker's and completion's answer — bounded,
// because a completion that hangs on a stuck relay is worse than one
// that offers nothing.
func (s *session) airSessionKeys(instance string) map[string]string {
	type result struct{ rows []AirSession }
	ch := make(chan result, 1)
	go func() {
		rows, _ := s.airSessions(instance)
		ch <- result{rows}
	}()
	select {
	case got := <-ch:
		out := map[string]string{}
		for _, c := range got.rows {
			out[hex.EncodeToString(c.PubKey[:6])] = airRole(c)
		}
		return out
	case <-time.After(completionBudget):
		return nil
	}
}

// airSessionView reads the table for printing.
func (s *session) airSessionView(ctx context.Context, instance string, _ frameSelectors) (drawerView, error) {
	rows, err := s.airSessions(instance)
	if err != nil {
		return drawerView{}, err
	}
	named := s.nodeNames(ctx)
	v := drawerView{
		header: []string{colKeyHdr, colNameHdr, "WHO", "ANSWERS", "PATH", "ACTIVE"},
		rows:   map[string][]field{},
	}
	for _, c := range rows {
		key := hex.EncodeToString(c.PubKey[:6])
		v.keys = append(v.keys, key)
		v.rows[key] = []field{
			{name: fieldName, value: meshName(named[key]), rendered: true},
			{name: "who", value: airRole(c)},
			{name: "answers", value: airAnswers(c)},
			{name: "path", value: airPath(c)},
			{name: "active", value: ago(c.LastActive)},
		}
	}
	sort.Strings(v.keys)
	return v, nil
}

// airRole names what a companion may do here.
func airRole(c AirSession) string {
	if c.Admin {
		return "admin"
	}
	return "guest"
}

// airAnswers says how a reply to this companion travels: down the
// route it taught us, or flooded across the mesh until it does.
func airAnswers(c AirSession) string {
	if !c.HasPath {
		return "flood"
	}
	return "direct"
}

// airPath spells the route home, hop by hop, oldest step first — the
// hashes a directed answer will visit on its way to the source. No
// route yet reads as what it costs: every answer floods.
func airPath(c AirSession) string {
	switch {
	case !c.HasPath:
		return "none yet — answers flood"
	case len(c.Path) == 0:
		return "adjacent (0 hops)"
	default:
		hops := make([]string, len(c.Path))
		for i, h := range c.Path {
			hops[i] = hex.EncodeToString([]byte{h})
		}
		return strings.Join(hops, "→") + fmt.Sprintf(" (%d hops)", len(c.Path))
	}
}

// access reads a relay's access list, refusing with the relay's own
// reason when it is down.
func (s *session) access(instance string) ([]Access, error) {
	r, err := s.findRelay(instance)
	if err != nil {
		return nil, err
	}
	if err := working(r); err != nil {
		return nil, err
	}
	if r.Access == nil {
		return nil, fmt.Errorf("relay %q keeps no access list", r.Name)
	}
	return r.Access()
}

// accessKeys answers the walker and completion, bounded.
func (s *session) accessKeys(instance string) map[string]string {
	type result struct{ rows []Access }
	ch := make(chan result, 1)
	go func() {
		rows, _ := s.access(instance)
		ch <- result{rows}
	}()
	select {
	case got := <-ch:
		out := map[string]string{}
		for _, a := range got.rows {
			out[hex.EncodeToString(a.PubKey[:6])] = a.Role
		}
		return out
	case <-time.After(completionBudget):
		return nil
	}
}

// accessView reads the access list for printing.
func (s *session) accessView(ctx context.Context, instance string, _ frameSelectors) (drawerView, error) {
	rows, err := s.access(instance)
	if err != nil {
		return drawerView{}, err
	}
	named := s.nodeNames(ctx)
	v := drawerView{
		header: []string{colKeyHdr, colNameHdr, "ROLE", "HOW", "ACTIVE"},
		rows:   map[string][]field{},
	}
	for _, a := range rows {
		key := hex.EncodeToString(a.PubKey[:6])
		v.keys = append(v.keys, key)
		v.rows[key] = []field{
			{name: fieldName, value: meshName(named[key]), rendered: true},
			{name: "role", value: a.Role},
			{name: "how", value: accessHow(a)},
			{name: "active", value: ago(a.LastActive)},
		}
	}
	sort.Strings(v.keys)
	return v, nil
}

// accessHow says whether the entry was granted or merely logged in —
// the distinction that decides whether it outlives idle.
func accessHow(a Access) string {
	if a.Granted {
		return "granted"
	}
	return "logged in"
}

// drawerSite is where a path stands relative to a drawer: which one,
// the instance holding it — empty for a singleton's — and the item,
// when the path reaches one.
type drawerSite struct {
	d        *drawer
	instance string
	item     string
}

// drawerSiteAt resolves a path against the drawers, or nil when it
// names no drawer at all.
func (s *session) drawerSiteAt(path []string) *drawerSite {
	if len(path) < 2 {
		return nil
	}
	base := 2
	if s.isSingleton(path[:1]) {
		base = 1
	}
	if len(path) <= base || len(path) > base+2 {
		return nil
	}
	d := drawerOn(path[0], path[base])
	if d == nil {
		return nil
	}
	site := &drawerSite{d: d}
	if base == 2 {
		site.instance = path[1]
	}
	if len(path) > base+1 {
		site.item = path[base+1]
	}
	return site
}

// drawersOn lists what a kind's instances hold.
func drawersOn(kind string) []drawer {
	var out []drawer
	for _, d := range drawers {
		if d.on == kind {
			out = append(out, d)
		}
	}
	return out
}

// claimedByDrawer reports whether a drawer of this kind holds the
// verb. A command belongs in one place, and this is what keeps it out
// of the instance it would otherwise have mounted on.
func claimedByDrawer(kind, verb string) bool {
	for _, d := range drawers {
		if d.on == kind && (slices.Contains(d.verbs, verb) || slices.Contains(d.itemVerbs, verb)) {
			return true
		}
	}
	return false
}

// commandHome says where a command lives when it is not a root
// citizen: the instance it acts on, or the drawer that claimed it.
// The path comes back as a template an operator can follow.
func commandHome(c *command) string {
	for _, d := range drawers {
		if slices.Contains(d.verbs, c.name) {
			return "/" + d.on + "/<name>/" + d.name
		}
		if slices.Contains(d.itemVerbs, c.name) {
			return "/" + d.on + "/<name>/" + d.name + "/<" + d.itemFlag + ">"
		}
	}
	if c.on != "" {
		if c.onOne {
			return "/" + c.on // a singleton is its own instance
		}
		return "/" + c.on + "/<name>"
	}
	return ""
}

// drawerOn resolves one drawer by the kind that holds it and its name.
func drawerOn(kind, name string) *drawer {
	for i := range drawers {
		if drawers[i].on == kind && drawers[i].name == name {
			return &drawers[i]
		}
	}
	return nil
}

// place says what kind of somewhere a path names. Every question the
// console asks about a path — what verbs answer here, what may be
// typed next, what print shows — asks it here first, so none of them
// can disagree about where the session is standing.
type place int

const (
	atNowhere place = iota
	atRoot
	atCollection // /relay
	atSingleton  // /system
	atInstance   // /relay/meshcore-868
	atDrawer     // /relay/meshcore-868/neighbours
	atDrawerItem // /relay/meshcore-868/neighbours/0d139b6421d0
)

func (s *session) placeAt(path []string) place {
	switch len(path) {
	case 0:
		return atRoot
	case 1:
		if s.isSingleton(path) {
			return atSingleton
		}
		return atCollection
	}
	if site := s.drawerSiteAt(path); site != nil {
		if site.item != "" {
			return atDrawerItem
		}
		return atDrawer
	}
	if len(path) == 2 && !s.isSingleton(path[:1]) {
		return atInstance
	}
	return atNowhere
}

// drawerKeys names what stands in a drawer, and what each one is —
// engine-only, because completion asks from the terminal's goroutine.
func (s *session) drawerKeys(path []string) map[string]string {
	site := s.drawerSiteAt(path)
	if site == nil || site.d.keys == nil {
		return nil
	}
	return site.d.keys(s, site.instance)
}

// neighbourKeys is the neighbours drawer's answer.
func (s *session) neighbourKeys(instance string) map[string]string {
	r, err := s.findRelay(instance)
	if err != nil || r.Neighbours == nil {
		return nil
	}
	out := map[string]string{}
	for _, n := range r.Neighbours() {
		out[hex.EncodeToString(n.PubKey[:6])] = meshName(n.Name)
	}
	return out
}

// neighbourView reads the neighbourhood for printing. Unlike the
// walker's keys it may consult the journal, which knows a name for a
// node that only ever answered a scan — the engine learns one only
// from an advert heard zero-hop.
func (s *session) neighbourView(ctx context.Context, instance string, _ frameSelectors) (drawerView, error) {
	r, err := s.findRelay(instance)
	if err != nil {
		return drawerView{}, err
	}
	if err := working(r); err != nil {
		return drawerView{}, err
	}
	if r.Neighbours == nil {
		return drawerView{}, fmt.Errorf("relay %q does not keep a neighbourhood", r.Name)
	}
	named := s.nodeNames(ctx)
	v := drawerView{header: []string{colKeyHdr, colNameHdr, "SNR", "HEARD"}, rows: map[string][]field{}}
	for _, n := range r.Neighbours() {
		key := hex.EncodeToString(n.PubKey[:6])
		name := n.Name
		if name == "" {
			name = named[key]
		}
		v.keys = append(v.keys, key)
		v.rows[key] = []field{
			{name: fieldName, value: meshName(name), rendered: true},
			{name: "snr", value: fmt.Sprintf("%+.2f dB", n.SNR)},
			{name: "heard", value: ago(n.Heard)},
		}
	}
	sort.Strings(v.keys)
	return v, nil
}

// printDrawer shows what a drawer holds: a listing that names each one
// and little else, or — asked for detail — each one opened out.
func (s *session) printDrawer(ctx context.Context, path []string, detail bool, sel frameSelectors) error {
	site := s.drawerSiteAt(path)
	v, err := site.d.view(s, ctx, site.instance, sel)
	if err != nil {
		return err
	}
	if len(v.keys) == 0 {
		fmt.Fprintf(s.out, "%s\r\n", site.d.empty)
		return nil
	}
	defer func() {
		if v.note != "" {
			fmt.Fprintf(s.out, "%s\r\n", v.note)
		}
	}()
	if detail {
		gutter := 0
		for _, k := range v.keys {
			gutter = max(gutter, len(k))
		}
		gutter += 3
		for i, k := range v.keys {
			if i > 0 {
				fmt.Fprint(s.out, "\r\n")
			}
			s.writeDetail(k, gutter, pairs(v.rows[k]))
		}
		return nil
	}
	tb := s.table()
	tb.header(v.header...)
	for _, k := range v.keys {
		tb.row(append([]string{k}, cells(v.rows[k])...)...)
	}
	return tb.flush(s.out)
}

// printDrawerItem shows one of them, attribute by attribute — the
// shape print has everywhere it stands on a single object.
func (s *session) printDrawerItem(ctx context.Context, path []string) error {
	site := s.drawerSiteAt(path)
	v, err := site.d.view(s, ctx, site.instance, frameSelectors{})
	if err != nil {
		return err
	}
	row, ok := v.rows[site.item]
	if !ok {
		return fmt.Errorf("no %q in this %s — %s lists %d",
			site.item, site.d.name, verbPrint, len(v.keys))
	}
	tb := s.table()
	tb.header("ATTRIBUTE", "VALUE")
	for _, f := range row {
		tb.row(f.name, f.value)
	}
	return tb.flush(s.out)
}
