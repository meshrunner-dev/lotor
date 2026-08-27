package cli

import (
	"context"
	"encoding/hex"
	"fmt"
	"sort"
)

// A drawer is a runtime collection an instance holds: what the mesh is
// doing rather than what it was told to do. It is somewhere to stand
// and read, and nothing more — no attribute to set, nothing to export,
// because none of it was ever configured.
type drawer struct {
	name string
	doc  string
	// on names the kind whose instances hold it.
	on string
}

const drawerNeighbours = "neighbours"

var drawers = []drawer{{
	name: drawerNeighbours,
	doc:  "repeaters heard with no relay in between",
	on:   scopeRelay,
}}

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
	case 2:
		return atInstance
	case 3:
		return atDrawer
	case 4:
		return atDrawerItem
	}
	return atNowhere
}

// drawerKeys names what stands in a drawer, and what each one is. It
// reads the engine alone, because completion asks this question from
// the terminal's goroutine where there is no request to carry.
func (s *session) drawerKeys(path []string) map[string]string {
	if len(path) < 3 || path[2] != drawerNeighbours {
		return nil
	}
	r, err := s.findRelay(path[1])
	if err != nil || r.Neighbours == nil {
		return nil
	}
	out := map[string]string{}
	for _, n := range r.Neighbours() {
		out[hex.EncodeToString(n.PubKey[:6])] = meshName(n.Name)
	}
	return out
}

// neighbourRow is one neighbour as a view will show it. The name is
// rendered here and only here, by the one function allowed to render
// a name off the air; the rest stay as they read, and a view that
// needs them to survive as single tokens quotes them itself.
type neighbourRow struct {
	name  string
	snr   string
	heard string
}

// fields is the row as a reader sees it, where whatever separates the
// values is not the values' problem.
func (n neighbourRow) fields() [][2]string {
	return [][2]string{{"name", n.name}, {"snr", n.snr}, {"heard", n.heard}}
}

// pairs renders the row for the packed form, where a space would end
// a value early. The name is left alone: it arrived quoted, and a
// second pass over an already-rendered value is a second pair of
// quotes around it.
func (n neighbourRow) pairs() [][2]string {
	return [][2]string{
		{"name", n.name},
		{"snr", quoteIfSpaced(n.snr)},
		{"heard", quoteIfSpaced(n.heard)},
	}
}

// cells renders the row for a table, where the columns do the
// separating and nothing needs quoting to survive.
func (n neighbourRow) cells() []string { return []string{n.name, n.snr, n.heard} }

// drawerRows reads a drawer for printing: the keys in a stable order,
// and what each one holds. Unlike drawerKeys it may consult the
// journal, which knows a name for a node that only ever answered a
// scan — the engine learns one only from an advert heard zero-hop.
func (s *session) drawerRows(ctx context.Context, path []string) ([]string, map[string]neighbourRow, error) {
	r, err := s.findRelay(path[1])
	if err != nil {
		return nil, nil, err
	}
	if err := working(r); err != nil {
		return nil, nil, err
	}
	if r.Neighbours == nil {
		return nil, nil, fmt.Errorf("relay %q does not keep a neighbourhood", r.Name)
	}
	named := s.nodeNames(ctx)
	keys := []string{}
	rows := map[string]neighbourRow{}
	for _, n := range r.Neighbours() {
		key := hex.EncodeToString(n.PubKey[:6])
		name := n.Name
		if name == "" {
			name = named[key]
		}
		keys = append(keys, key)
		rows[key] = neighbourRow{
			name:  meshName(name),
			snr:   fmt.Sprintf("%+.2f dB", n.SNR),
			heard: ago(n.Heard),
		}
	}
	sort.Strings(keys)
	return keys, rows, nil
}

// printDrawer shows what a drawer holds: a listing that names each one
// and little else, or — asked for detail — each one opened out.
func (s *session) printDrawer(ctx context.Context, path []string, detail bool) error {
	keys, rows, err := s.drawerRows(ctx, path)
	if err != nil {
		return err
	}
	if len(keys) == 0 {
		fmt.Fprint(s.out, "nobody heard directly yet\r\n")
		return nil
	}
	if detail {
		gutter := 0
		for _, k := range keys {
			gutter = max(gutter, len(k))
		}
		gutter += 3
		for i, k := range keys {
			if i > 0 {
				fmt.Fprint(s.out, "\r\n")
			}
			s.writeDetail(k, gutter, rows[k].pairs())
		}
		return nil
	}
	tb := s.table()
	tb.header("KEY", "NAME", "SNR", "HEARD")
	for _, k := range keys {
		tb.row(append([]string{k}, rows[k].cells()...)...)
	}
	return tb.flush(s.out)
}

// printDrawerItem shows one of them, attribute by attribute — the
// shape print has everywhere it stands on a single object.
func (s *session) printDrawerItem(ctx context.Context, path []string) error {
	keys, rows, err := s.drawerRows(ctx, path[:3])
	if err != nil {
		return err
	}
	row, ok := rows[path[3]]
	if !ok {
		return fmt.Errorf("no %q in this %s — %s lists %d", path[3], path[2], verbPrint, len(keys))
	}
	tb := s.table()
	tb.header("ATTRIBUTE", "VALUE")
	for _, p := range row.fields() {
		tb.row(p[0], p[1])
	}
	return tb.flush(s.out)
}
