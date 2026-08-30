package cli

import (
	"bytes"
	"strings"
	"testing"

	"meshrunner.dev/lotor/internal/update"
)

func TestHumanBytesReadsLikeASize(t *testing.T) {
	for _, c := range []struct {
		n    int64
		want string
	}{
		{0, "0 B"},
		{512, "512 B"},
		{1024, "1.0 kiB"},
		{6_212_861, "5.9 MiB"},
		{1024 * 1024 * 1024, "1.0 GiB"},
		// Past the last unit the figure grows rather than inventing
		// a name nobody uses on a relay.
		{5 * 1024 * 1024 * 1024 * 1024, "5.0 TiB"},
	} {
		if got := humanBytes(c.n); got != c.want {
			t.Errorf("humanBytes(%d) = %q, want %q", c.n, got, c.want)
		}
	}
}

func TestProgressNeverOversteps(t *testing.T) {
	// A manifest that never said how big, and a server that sent one
	// byte too many: neither may draw past the end or report 101%.
	if got := percentOf(10, 0); got != 0 {
		t.Errorf("percentOf(10, 0) = %d, want 0", got)
	}
	if got := percentOf(101, 100); got != 100 {
		t.Errorf("a server that overran reported %d%%", got)
	}
	if bar := progressBar(10, 0); strings.Count(bar, "█") != 0 {
		t.Errorf("an unknown total drew a filled bar: %q", bar)
	}
	full := progressBar(200, 100)
	if strings.Count(full, "█") != progressCells || strings.Count(full, "░") != 0 {
		t.Errorf("an overrun drew %q", full)
	}
	half := progressBar(50, 100)
	if strings.Count(half, "█") != progressCells/2 {
		t.Errorf("half drew %q", half)
	}
}

func TestFetchProgressDrawsOnlyForATerminal(t *testing.T) {
	// Repainting a line means nothing to a pipe: a script's
	// transcript keeps the two plain lines and no escape sequences.
	var out bytes.Buffer
	plain := &session{out: &out}
	client := &update.Client{}
	stop := plain.showFetchProgress(client)
	if client.Progress != nil {
		t.Error("a pipe was handed a progress hook")
	}
	stop()
	if out.Len() != 0 {
		t.Errorf("a pipe was painted into: %q", out.String())
	}

	// A terminal gets the bar, and the LAST chunk always paints —
	// a download that fits inside one pacing interval must still
	// reach its end rather than stopping at whatever it drew first.
	out.Reset()
	term := &session{out: &out, colors: true}
	client = &update.Client{}
	stop = term.showFetchProgress(client)
	if client.Progress == nil {
		t.Fatal("a terminal was handed no progress hook")
	}
	client.Progress(1024, 6_212_861)
	client.Progress(3_000_000, 6_212_861) // inside the interval: folded
	client.Progress(6_212_861, 6_212_861) // the end: always drawn
	painted := out.String()
	if !strings.Contains(painted, "100%") || !strings.Contains(painted, "5.9 MiB / 5.9 MiB") {
		t.Errorf("the bar never reached its end:\n%q", painted)
	}
	if strings.Count(painted, "%") != 2 {
		t.Errorf("the pacing did not fold the middle chunk:\n%q", painted)
	}

	// Stopping takes the line back and unhooks: nothing arriving
	// late may paint over what comes next.
	out.Reset()
	stop()
	if client.Progress != nil {
		t.Error("the hook outlived the download")
	}
	if got := out.String(); got != "\r\x1b[K" {
		t.Errorf("the line was not taken back: %q", got)
	}
}
