package cli

import (
	"slices"
	"sort"

	"meshrunner.dev/lotor/internal/schema"
)

// valueCandidates is the one vocabulary door for values. Each owner
// supplies its words: schemas, drawer and command callbacks, preset
// catalogs, or the live instances a reference points to. The caller
// handles prefix matching and the command's quoting or encoding.
func (s *session) valueCandidates(path, rest []string, attr string) (words []string, class string) {
	if words := s.attributeCandidates(path, rest, attr); words != nil {
		return words, cAttr
	}
	if site := s.drawerSiteAt(path); site != nil && site.d.values != nil {
		if words := site.d.values(s, site.instance, attr); len(words) > 0 {
			return words, cAttr
		}
	}
	if attr == "profile" && len(path) > 0 {
		if k := s.kindByName(path[0]); k != nil && k.Profiles != nil {
			words := slices.Clone(k.Profiles(s.choiceOn(k, path, rest)))
			if !slices.Contains(words, "custom") {
				words = append(words, "custom")
			}
			sort.Strings(words)
			return words, cAttr
		}
	}
	return s.flagCandidates(path, rest, attr)
}

func (s *session) attributeCandidates(path, rest []string, attr string) []string {
	a, found := schema.Find(s.attrsForLine(path, rest), attr)
	if !found {
		return nil
	}
	if len(path) > 0 && len(path) <= 2 {
		if k := s.kindByName(path[0]); k != nil && k.ValueSuggestions != nil {
			if words := k.ValueSuggestions(s.choiceOn(k, path, rest), attr); words != nil {
				return words
			}
		}
	}
	return a.Candidates()
}

func (s *session) flagCandidates(path, rest []string, attr string) (words []string, class string) {
	if len(rest) > 0 {
		if c := lookup(rest[0]); c != nil {
			if f := c.flag(attr); f != nil && f.values != nil {
				return f.values(s, path), cAttr
			}
		}
	}
	// The existing reference convention is shared by structural
	// attributes and flags: radio= names a radio, relay= a relay.
	if k := s.kindByName(attr); k != nil && !k.Singleton {
		for name := range s.instances(attr) {
			words = append(words, name)
		}
		sort.Strings(words)
		return words, cPath
	}
	return nil, ""
}
