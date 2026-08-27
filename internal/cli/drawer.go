package cli

import (
	"context"
	"encoding/hex"
	"fmt"
	"sort"

	"meshrunner.dev/lotor/internal/config"
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

// drawerRows reads a drawer for printing: the keys in a stable order,
// and what each one holds. Unlike drawerKeys it may consult the
// journal, which knows a name for a node that only ever answered a
// scan — the engine learns one only from an advert heard zero-hop.
func (s *session) drawerRows(ctx context.Context, path []string) ([]string, map[string][]config.Trace, error) {
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
	rows := map[string][]config.Trace{}
	for _, n := range r.Neighbours() {
		key := hex.EncodeToString(n.PubKey[:6])
		name := n.Name
		if name == "" {
			name = named[key]
		}
		keys = append(keys, key)
		rows[key] = []config.Trace{
			{Key: "name", Value: meshName(name)},
			{Key: "snr", Value: fmt.Sprintf("%+.2f dB", n.SNR)},
			{Key: "heard", Value: ago(n.Heard)},
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
			s.writeDetail(k, gutter, rows[k], nil)
		}
		return nil
	}
	tb := s.table()
	tb.header("KEY", "NAME", "SNR", "HEARD")
	for _, k := range keys {
		cells := []string{k}
		for _, t := range rows[k] {
			cells = append(cells, fmt.Sprintf("%v", t.Value))
		}
		tb.row(cells...)
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
	for _, t := range row {
		tb.row(t.Key, fmt.Sprintf("%v", t.Value))
	}
	return tb.flush(s.out)
}
