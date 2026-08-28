package update

// Deciding what "newer" means. The stable channels speak semver and
// compare by it; dev and try move too fast for version arithmetic and
// compare by when the manifest was published. Both directions of the
// same rule: within a channel, time only moves forward — a replayed
// old manifest, however honestly signed, must never read as an
// update. Switching channels is the operator saying the rule does not
// apply, which is why the comparison takes the channel and not a
// guess.

import (
	"strconv"
	"strings"
)

// semverChannel reports whether a channel's versions are comparable
// numbers rather than build stamps.
func semverChannel(channel string) bool {
	switch channel {
	case "release", "rc", "beta":
		return true
	}
	return false
}

// CompareVersions orders two semver strings: -1, 0, or 1 as a is
// older than, the same as, or newer than b. A leading v is tolerated
// because tags carry one. Pre-release ordering follows semver: a
// pre-release precedes its release, identifiers compare numerically
// when numeric and bytewise otherwise, and a longer pre-release chain
// wins over its own prefix.
func CompareVersions(a, b string) int {
	ac, apre := splitVersion(a)
	bc, bpre := splitVersion(b)
	for i := range 3 {
		if c := compareNum(num(ac, i), num(bc, i)); c != 0 {
			return c
		}
	}
	switch {
	case apre == "" && bpre == "":
		return 0
	case apre == "":
		return 1 // the release outranks its own pre-releases
	case bpre == "":
		return -1
	}
	return comparePre(apre, bpre)
}

func splitVersion(v string) (core, pre string) {
	v = strings.TrimPrefix(v, "v")
	// Build metadata orders nothing, per semver.
	v, _, _ = strings.Cut(v, "+")
	core, pre, _ = strings.Cut(v, "-")
	return core, pre
}

func num(core string, i int) int {
	parts := strings.Split(core, ".")
	if i >= len(parts) {
		return 0
	}
	n, err := strconv.Atoi(parts[i])
	if err != nil {
		return 0
	}
	return n
}

func compareNum(a, b int) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

func comparePre(a, b string) int {
	as, bs := strings.Split(a, "."), strings.Split(b, ".")
	for i := 0; i < len(as) && i < len(bs); i++ {
		an, aerr := strconv.Atoi(as[i])
		bn, berr := strconv.Atoi(bs[i])
		switch {
		case aerr == nil && berr == nil:
			if c := compareNum(an, bn); c != 0 {
				return c
			}
		case aerr == nil:
			return -1 // numeric identifiers order before alphanumeric
		case berr == nil:
			return 1
		default:
			if c := strings.Compare(as[i], bs[i]); c != 0 {
				return c
			}
		}
	}
	return compareNum(len(as), len(bs))
}

// Newer reports whether the manifest offers something ahead of what
// runs. current is the running version; lastPublished is when the
// newest manifest this relay ever accepted on this channel was
// published, zero when none was.
func Newer(m *Manifest, current string, lastPublished int64) bool {
	if semverChannel(m.Channel) {
		return CompareVersions(m.Version, current) > 0
	}
	return m.Published.Unix() > lastPublished
}
