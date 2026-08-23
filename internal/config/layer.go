package config

import (
	"bytes"
	"fmt"
	"sort"

	"gopkg.in/yaml.v3"
)

// CustomProfile is the reserved profile name whose base is empty:
// every value comes from the override scope.
const CustomProfile = "custom"

// Layered is the reusable configuration mechanism: a named profile
// selects a preset from a catalog, and overrides scoped *by profile
// name* patch on top of it. Switching profiles is non-destructive —
// overrides live under the profile they belong to, so several tuned
// profiles coexist in one file and the Profile knob alone decides
// which one is live.
type Layered struct {
	Profile   string                    `yaml:"profile"`
	Overrides map[string]map[string]any `yaml:"overrides"`
}

// Trace records where one effective key came from.
type Trace struct {
	Key    string
	Source string // "profile:<name>" or "override:<name>"
	Value  any
}

// Resolve merges the selected preset with its override scope and
// returns the effective key set plus, key by key, its provenance.
// Unknown profile names — selected or used as an override scope — are
// errors: a typo must fail the load, not silently configure nothing.
func (l Layered) Resolve(catalog map[string]map[string]any) (map[string]any, []Trace, error) {
	profile := l.Profile
	if profile == "" {
		profile = CustomProfile
	}
	if _, hijack := catalog[CustomProfile]; hijack {
		return nil, nil, fmt.Errorf("catalog reserves the %q profile as the empty base", CustomProfile)
	}
	base, ok := catalog[profile]
	if !ok && profile != CustomProfile {
		return nil, nil, fmt.Errorf("unknown profile %q (known: %s)", profile, knownProfiles(catalog))
	}
	for scope := range l.Overrides {
		if _, ok := catalog[scope]; !ok && scope != CustomProfile {
			return nil, nil, fmt.Errorf("override scope %q names no known profile (known: %s)",
				scope, knownProfiles(catalog))
		}
	}

	effective := make(map[string]any, len(base)+len(l.Overrides[profile]))
	traces := make([]Trace, 0, len(base)+len(l.Overrides[profile]))
	for k, v := range base {
		effective[k] = v
		traces = append(traces, Trace{Key: k, Source: "profile:" + profile, Value: v})
	}
	for k, v := range l.Overrides[profile] {
		if _, shadowed := effective[k]; shadowed {
			for i := range traces {
				if traces[i].Key == k {
					traces[i] = Trace{Key: k, Source: "override:" + profile, Value: v}
				}
			}
		} else {
			traces = append(traces, Trace{Key: k, Source: "override:" + profile, Value: v})
		}
		effective[k] = v
	}
	sort.Slice(traces, func(i, j int) bool { return traces[i].Key < traces[j].Key })
	return effective, traces, nil
}

func knownProfiles(catalog map[string]map[string]any) string {
	names := make([]string, 0, len(catalog)+1)
	for name := range catalog {
		names = append(names, name)
	}
	names = append(names, CustomProfile)
	sort.Strings(names)
	buf := bytes.Buffer{}
	for i, n := range names {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(n)
	}
	return buf.String()
}

// Decode strictly maps an effective key set onto a typed struct:
// unknown keys are errors, in the driver-parameter spirit — a typo is
// a load-time failure, never a silently ignored setting.
func Decode[T any](m map[string]any) (T, error) {
	var out T
	raw, err := yaml.Marshal(m)
	if err != nil {
		return out, fmt.Errorf("re-encode: %w", err)
	}
	dec := yaml.NewDecoder(bytes.NewReader(raw))
	dec.KnownFields(true)
	if err := dec.Decode(&out); err != nil {
		return out, err
	}
	return out, nil
}
