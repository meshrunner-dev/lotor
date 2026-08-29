package main

// The export's whole promise, proved through the whole chain: what
// treeExport prints, replayed line by line through the real CLI, the
// real manager and the real validation into an empty store, comes
// back as the same persisted form. The matrix is the values the
// inline form cannot carry — where only the atomic encoded carriers
// keep cross-attribute constraints and secrets in one transaction.

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"

	"go.uber.org/zap"

	"meshrunner.dev/lotor/internal/bus"
	"meshrunner.dev/lotor/internal/cli"
	"meshrunner.dev/lotor/internal/confdb"
	"meshrunner.dev/lotor/internal/config"
)

// replayManager builds a manager over an in-memory store seeded with
// f — or empty when f is nil — ready to take console commands.
func replayManager(t *testing.T, f *config.File) (*manager, *bus.Bus) {
	t.Helper()
	ctx := context.Background()
	store, err := confdb.Open(ctx, confdb.Memory, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if f == nil {
		f = &config.File{}
	} else if err := store.ImportFile(ctx, f, "test"); err != nil {
		t.Fatal(err)
	}
	b := bus.New()
	m := newManager(store, f, b, nil, buildKinds(), zap.NewNop())
	mctx, cancel := context.WithCancel(ctx)
	// The real boot: relays start (and fail honestly without their
	// hardware), observers reconcile — the states export speaks from.
	m.Start(mctx)
	t.Cleanup(func() {
		m.mu.Lock()
		for name := range m.running {
			m.stopRelay(name)
		}
		for name := range m.observers {
			m.stopObserver(name)
		}
		m.mu.Unlock()
		cancel()
		m.wg.Wait()
	})
	return m, b
}

// console runs input through a real admin session wired to the
// manager, and returns the transcript.
func adminConsole(t *testing.T, m *manager, b *bus.Bus, input string) string {
	t.Helper()
	deps := consoleDeps(m, b, nil)
	deps.Privilege = cli.Admin
	var out bytes.Buffer
	rw := struct {
		io.Reader
		io.Writer
	}{strings.NewReader(input), &out}
	cli.Serve(context.Background(), rw, deps)
	return out.String()
}

// commandLines keeps the transcript's absolute command lines — the
// export's output, which is what an operator would paste back.
func commandLines(transcript string) []string {
	var lines []string
	for l := range strings.SplitSeq(transcript, "\n") {
		l = strings.TrimSuffix(l, "\r")
		// The prompt is printed without a newline, so a command's
		// first output line rides the prompt's own line.
		if strings.HasPrefix(l, "[") {
			if _, rest, ok := strings.Cut(l, "] > "); ok {
				l = rest
			}
		}
		if strings.HasPrefix(l, "/") {
			lines = append(lines, l)
		}
	}
	return lines
}

// canonical renders every override value the way the grammar writes
// it, so a YAML-typed 0 and a console-parsed 0 compare as the same
// persisted form.
func canonical(overrides map[string]map[string]any) map[string]map[string]string {
	out := make(map[string]map[string]string, len(overrides))
	for scope, kv := range overrides {
		m := make(map[string]string, len(kv))
		for k, v := range kv {
			m[k] = fmt.Sprintf("%v", v)
		}
		out[scope] = m
	}
	return out
}

func TestExportReplaysThroughTheRealChain(t *testing.T) {
	// guest_access=password only exists WITH its password: split
	// across two commands the creation is refused with the secret
	// still in flight and the object is lost whole. The rest of the
	// matrix: an inactive scope whose profile switch and encoded
	// value must mutate together, and an observer whose password
	// cannot ride the inline form.
	f := sampleFile()
	r := f.Relays["meshcore-868"]
	r.Layered.Overrides["eu-868-narrow"]["guest_access"] = "password"
	r.Layered.Overrides["eu-868-narrow"]["guest_password"] = `h"unter`
	f.Relays["meshcore-868"] = r
	// The observer carries the inactive-scope leg: its custom scope
	// must arrive through ONE mutation — profile switch, url and
	// control-laden password together — because the composed
	// candidate is judged whole at the switch, exactly as a console
	// operator would be judged.
	f.MQTT = map[string]config.MQTT{"obs": {
		Disabled: true,
		Layered: config.Layered{
			Profile: "analyzer-eu",
			Overrides: map[string]map[string]any{
				"analyzer-eu": {"iata": "TLS", "password": `p"wd`},
				"custom": {"url": "tcp://127.0.0.1:1", "iata": "AJA",
					"password": "p\"wd\ntwo"},
			},
		},
	}}
	m1, b1 := replayManager(t, f)
	exported := commandLines(adminConsole(t, m1, b1, "export\n"))
	if len(exported) == 0 {
		t.Fatal("the export produced no command lines")
	}

	m2, b2 := replayManager(t, nil)
	transcript := adminConsole(t, m2, b2, strings.Join(exported, "\n")+"\n")

	// The canonical persisted form, compared object by object.
	for _, obj := range [][2]string{
		{"radio", "slot1"}, {"relay", "meshcore-868"}, {"mqtt", "obs"},
	} {
		p1, o1, ok1 := m1.Layers(obj[0], obj[1])
		p2, o2, ok2 := m2.Layers(obj[0], obj[1])
		if !ok1 || !ok2 {
			t.Fatalf("%s %s: layered=%v/%v — the replay lost it:\n%s\n%s",
				obj[0], obj[1], ok1, ok2, strings.Join(exported, "\n"), transcript)
		}
		if p1 != p2 || !reflect.DeepEqual(canonical(o1), canonical(o2)) {
			t.Errorf("%s %s came back different:\nprofile %q -> %q\n%v\n->\n%v",
				obj[0], obj[1], p1, p2, o1, o2)
		}
	}
	// And the values that forced the carrier, byte for byte.
	_, overrides, _ := m2.Layers("relay", "meshcore-868")
	if got := overrides["eu-868-narrow"]["guest_password"]; got != `h"unter` {
		t.Errorf("guest_password = %q — the atomic creation lost it (transcript:\n%s)",
			got, transcript)
	}
	_, obs, _ := m2.Layers("mqtt", "obs")
	if got := obs["analyzer-eu"]["password"]; got != `p"wd` {
		t.Errorf("observer password = %q", got)
	}
	if got := obs["custom"]["password"]; got != "p\"wd\ntwo" {
		t.Errorf("inactive scope password = %q — the scope switch travelled apart", got)
	}
}
