package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	xterm "golang.org/x/term"

	"meshrunner.dev/lotor/internal/schema"
)

const terminalScreenPrompt = "[admin@wanadoo] /station/szer> "

// This optional integration test uses tmux as the terminal emulator: its
// actual screen and cursor are the oracle, not another copy of our layout.
// No daemon, radio or external connection is started. Without tmux the
// ordinary input/layout tests still run; this one reports a skip.
func TestTerminalScreenEditor(t *testing.T) {
	path, err := exec.LookPath("tmux")
	if err != nil {
		t.Skip("tmux is unavailable; skipping real terminal screen checks")
	}
	dir := t.TempDir()
	screen := &terminalScreen{tmux: path, socket: filepath.Join(dir, "tmux.sock"), config: filepath.Join(dir, "tmux.conf")}
	// Disable the status bar before the first pane is created, so the
	// child observes the requested height from its very first GetSize.
	if err := os.WriteFile(screen.config, []byte("set -g status off\nset -g remain-on-exit on\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = screen.command("kill-server") })
	for _, name := range []string{"identity", "wide", "viewport", "watch-narrow", "watch-resize", "watch-tall", "watch-footer"} {
		t.Run(name, func(t *testing.T) {
			pane := *screen
			pane.name, pane.state = name, filepath.Join(dir, name+".json")
			width, height := 80, 24
			switch name {
			case "viewport", "watch-narrow":
				width, height = 40, 8
			case "watch-resize":
				width, height = 80, 8
			case "watch-tall":
				width, height = 40, 4
			case "watch-footer":
				width, height = 12, 8
			}
			pane.start(t, width, height)
			switch name {
			case "identity":
				pane.identity(t)
			case "wide":
				pane.wide(t)
			case "viewport":
				pane.viewport(t)
			default:
				pane.watch(t)
			}
		})
	}
}

type terminalScreen struct {
	tmux, socket, config string
	name, state          string
	sequence             int
}

type terminalScreenState struct {
	Sequence int    `json:"sequence"`
	Accepted string `json:"accepted"`
}

type terminalScreenImage struct {
	x, y, width, height int
	text                string
}

func (s *terminalScreen) command(args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	base := make([]string, 0, 5+len(args))
	base = append(base, "-u", "-f", s.config, "-S", s.socket)
	cmd := exec.CommandContext(ctx, s.tmux, append(base, args...)...)
	for _, value := range os.Environ() {
		if !strings.HasPrefix(value, "TMUX=") {
			cmd.Env = append(cmd.Env, value)
		}
	}
	return cmd.CombinedOutput()
}

func (s *terminalScreen) run(t *testing.T, args ...string) []byte {
	t.Helper()
	out, err := s.command(args...)
	if err != nil {
		t.Fatalf("tmux %v: %v\n%s", args, err, out)
	}
	return out
}

func (s *terminalScreen) start(t *testing.T, width, height int) {
	t.Helper()
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	// Multiple command arguments make tmux exec the helper directly;
	// paths and environment values never become shell source.
	helper := "TestTerminalScreenHelper"
	if strings.HasPrefix(s.name, "watch-") {
		helper = "TestTerminalWatchScreenHelper"
	}
	s.run(t, "new-session", "-d", "-s", s.name, "-x", strconv.Itoa(width), "-y", strconv.Itoa(height),
		"env", "LOTOR_CLI_SCREEN_HELPER="+s.state, "LOTOR_CLI_SCREEN_CASE="+s.name,
		binary, "-test.run=^"+helper+"$", "-test.count=1")
	s.waitState(t)
	s.waitImage(t, func(image terminalScreenImage) bool {
		return image.width == width && image.height == height &&
			strings.Contains(strings.ReplaceAll(image.text, "\n", ""), strings.TrimSpace(terminalScreenPrompt))
	})
}

func (s *terminalScreen) send(t *testing.T, literal bool, keys string) {
	t.Helper()
	args := []string{"send-keys", "-t", s.name}
	if literal {
		args = append(args, "-l")
	}
	s.run(t, append(args, keys)...)
	s.receipt(t)
}

func (s *terminalScreen) receipt(t *testing.T) {
	t.Helper()
	// Ctrl+] is ignored by the editor and acts only as a helper receipt:
	// all preceding keystrokes have been processed before the file changes.
	s.sequence++
	s.run(t, "send-keys", "-t", s.name, "C-]")
	s.waitState(t)
}

func (s *terminalScreen) waitState(t *testing.T) terminalScreenState {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		data, err := os.ReadFile(s.state)
		var state terminalScreenState
		if err == nil && json.Unmarshal(data, &state) == nil && state.Sequence == s.sequence {
			return state
		}
		time.Sleep(10 * time.Millisecond)
	}
	out, _ := s.command("capture-pane", "-p", "-t", s.name)
	t.Fatalf("terminal helper did not acknowledge %d\n%s", s.sequence, out)
	return terminalScreenState{}
}

func (s *terminalScreen) image(t *testing.T) terminalScreenImage {
	t.Helper()
	var image terminalScreenImage
	meta := s.run(t, "display-message", "-p", "-t", s.name, "#{cursor_x} #{cursor_y} #{pane_width} #{pane_height}")
	if _, err := fmt.Sscanf(string(meta), "%d %d %d %d", &image.x, &image.y, &image.width, &image.height); err != nil {
		t.Fatal(err)
	}
	image.text = string(s.run(t, "capture-pane", "-p", "-t", s.name))
	return image
}

func (s *terminalScreen) waitImage(t *testing.T, match func(terminalScreenImage) bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var image terminalScreenImage
	for time.Now().Before(deadline) {
		image = s.image(t)
		if match(image) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("terminal cursor=(%d,%d) size=%dx%d\n%s", image.x, image.y, image.width, image.height, image.text)
}

func (s *terminalScreen) identity(t *testing.T) {
	t.Helper()
	line := "set identity=" + strings.Repeat("0123456789abcdef", 4)
	s.send(t, true, line)
	s.waitImage(t, func(image terminalScreenImage) bool {
		return image.x == 28 && image.y == 1 && strings.ReplaceAll(image.text, "\n", "") == terminalScreenPrompt+line
	})
	s.send(t, false, "Left")
	s.send(t, false, "BSpace")
	want := line[:len(line)-2] + line[len(line)-1:]
	s.waitImage(t, func(image terminalScreenImage) bool {
		return image.x == 26 && image.y == 1 && strings.ReplaceAll(image.text, "\n", "") == terminalScreenPrompt+want
	})
	s.send(t, false, "Enter")
	if state := s.waitState(t); state.Accepted != want {
		t.Fatalf("accepted identity=%q; want %q", state.Accepted, want)
	}
}

func (s *terminalScreen) wide(t *testing.T) {
	t.Helper()
	prefix := "set node_name="
	line := prefix + strings.Repeat("a", 79-len(terminalScreenPrompt)-len(prefix)) + "🦝XY"
	s.send(t, true, line)
	s.send(t, false, "Left")
	s.waitImage(t, func(image terminalScreenImage) bool {
		rows := strings.Split(image.text, "\n")
		return image.x == 3 && image.y == 1 && len(rows) > 1 && rows[1] == "🦝XY"
	})
	s.send(t, false, "Enter")
	if state := s.waitState(t); state.Accepted != line {
		t.Fatalf("wide line changed: %q", state.Accepted)
	}
}

func (s *terminalScreen) viewport(t *testing.T) {
	t.Helper()
	line := "set identity=" + strings.Repeat("0123456789abcdef", 32)
	s.send(t, true, line)
	s.waitImage(t, func(image terminalScreenImage) bool { return image.y == 7 })
	s.send(t, false, "C-a")
	s.waitImage(t, func(image terminalScreenImage) bool {
		visible := strings.ReplaceAll(image.text, "\n", "")
		return image.x == 31 && image.y == 0 && visible == (terminalScreenPrompt + line)[:320]
	})
	s.run(t, "resize-window", "-t", s.name+":0", "-x", "20", "-y", "4")
	s.receipt(t)
	s.waitImage(t, func(image terminalScreenImage) bool {
		visible := strings.ReplaceAll(image.text, "\n", "")
		return image.width == 20 && image.height == 4 && image.x == 11 && image.y == 1 &&
			visible == (terminalScreenPrompt + line)[:80]
	})
	s.send(t, false, "Enter")
	if state := s.waitState(t); state.Accepted != line {
		t.Fatalf("tall draft changed: %q", state.Accepted)
	}
}

func (s *terminalScreen) watch(t *testing.T) {
	t.Helper()
	s.send(t, true, "/screen-watch\r")
	s.waitImage(t, func(image terminalScreenImage) bool {
		return strings.Contains(image.text, "FRAME-1-") &&
			strings.Contains(strings.ReplaceAll(image.text, "\n", ""), intervalStop)
	})
	if s.name == "watch-resize" {
		s.run(t, "resize-window", "-t", s.name+":0", "-x", "40", "-y", "8")
		s.receipt(t)
		s.waitImage(t, func(image terminalScreenImage) bool {
			return image.width == 40 && image.height == 8 && strings.Contains(image.text, "FRAME-1-") &&
				strings.Contains(image.text, intervalStop)
		})
	}
	// The private helper consumes Ctrl+\ to change the frame's version.
	// The real repaint timer then redraws through the display owner.
	s.send(t, false, "C-\\")
	s.waitImage(t, func(image terminalScreenImage) bool {
		if strings.Contains(image.text, "FRAME-1-") {
			return false
		}
		if s.name == "watch-tall" {
			rows := strings.Split(image.text, "\n")
			return rows[0] == "FRAME-2-00-"+strings.Repeat("x", 27)+"12" &&
				rows[1] == strings.Repeat("y", 20) && rows[2] == "FRAME-2-01-"+strings.Repeat("x", 27)+"12" &&
				rows[image.height-1] == "-- ["+intervalStop+"]"
		}
		return s.watchBodyMatches(image, "2")
	})
	// A third frame catches accumulation after a resize even in terminal
	// emulators whose first narrow repaint happens to replace the old row.
	s.send(t, false, "C-\\")
	s.waitImage(t, func(image terminalScreenImage) bool {
		return strings.Contains(image.text, "FRAME-3-") && !strings.Contains(image.text, "FRAME-2-") &&
			!strings.Contains(image.text, "FRAME-1-") &&
			strings.Contains(strings.ReplaceAll(image.text, "\n", ""), intervalStop)
	})
	s.send(t, false, "Enter")
	s.waitWatchPrompt(t, "")
	s.send(t, true, "x")
	s.waitWatchPrompt(t, "x")
}

func (s *terminalScreen) watchBodyMatches(image terminalScreenImage, version string) bool {
	var rows []string
	if s.name == "watch-footer" {
		rows = []string{"FRAME-" + version + "-xxxx", strings.Repeat("x", 12), strings.Repeat("x", 12),
			"xx12yyyyyyyy", strings.Repeat("y", 12), "-- [enter st", "ops]"}
	} else {
		rows = []string{"FRAME-" + version + "-" + strings.Repeat("x", 30) + "12", strings.Repeat("y", 20), "-- [" + intervalStop + "]"}
	}
	// Assert the exact terminal rows, including the distinct last cell in
	// each full row: EL at a pending wrap must not erase that character.
	return strings.Count(image.text, "FRAME-"+version+"-") == 1 && strings.Contains(image.text, strings.Join(rows, "\n"))
}

func (s *terminalScreen) waitWatchPrompt(t *testing.T, draft string) {
	t.Helper()
	s.waitImage(t, func(image terminalScreenImage) bool {
		rows := strings.Split(image.text, "\n")
		length := len(terminalScreenPrompt) + len(draft)
		start := image.y - length/image.width
		return start >= 0 && image.y < len(rows) && image.x == length%image.width &&
			strings.Join(rows[start:image.y+1], "") == strings.TrimRight(terminalScreenPrompt+draft, " ") &&
			!strings.Contains(strings.ReplaceAll(image.text, "\n", ""), intervalStop)
	})
}

// TestTerminalScreenHelper is selected only by the isolated tmux child.
// It exposes the editor directly and records accepted text; it has no
// command dispatcher and cannot mutate a daemon or configuration database.
func TestTerminalScreenHelper(t *testing.T) {
	path := os.Getenv("LOTOR_CLI_SCREEN_HELPER")
	if path == "" {
		t.Skip("real terminal helper is launched by TestTerminalScreenEditor")
	}
	fd := int(os.Stdin.Fd())
	old, err := xterm.MakeRaw(fd)
	if err != nil {
		t.Fatal(err)
	}
	defer xterm.Restore(fd, old)
	width, height, err := xterm.GetSize(fd)
	if err != nil {
		t.Fatal(err)
	}
	session := &session{colors: true, path: []string{"station", "szer"}, deps: Deps{
		Privilege: Admin, SystemName: func() string { return "wanadoo" },
		Stations: []StationInfo{{Name: "szer", Protocol: "meshcore"}},
		Kinds: []schema.Kind{{Name: "station", ChoiceAttr: "protocol", Attrs: []schema.Attr{
			{Name: "identity", Type: schema.String}, {Name: "node_name", Type: schema.String},
		}}},
	}}
	ed := newEditor(os.Stdin, os.Stdout)
	ed.width, ed.height = width, height
	ed.prompt, ed.paint = session.promptWith, session.paintLine
	fmt.Fprint(os.Stdout, "\x1b[H\x1b[2J", session.prompt())
	var state terminalScreenState
	writeScreenState(t, path, state)
	for {
		key, err := ed.in.ReadByte()
		if err != nil {
			return
		}
		done, line, err := ed.key(key)
		if err != nil {
			return
		}
		if done {
			state.Accepted = line
			fmt.Fprint(os.Stdout, session.prompt())
			ed.reset()
			ed.walk = -1
		}
		if key == 0x1d {
			width, height, err := xterm.GetSize(fd)
			if err != nil {
				t.Fatal(err)
			}
			ed.resize(width, height)
			state.Sequence++
			writeScreenState(t, path, state)
		}
	}
}

// This helper runs the normal session dispatcher and the real repaint
// command. Only its draw callback is synthetic, so long frames need no
// daemon state, radio, database or network connection.
func TestTerminalWatchScreenHelper(t *testing.T) {
	path := os.Getenv("LOTOR_CLI_SCREEN_HELPER")
	if path == "" {
		t.Skip("watch terminal helper is launched by TestTerminalScreenEditor")
	}
	fd := int(os.Stdin.Fd())
	old, err := xterm.MakeRaw(fd)
	if err != nil {
		t.Fatal(err)
	}
	defer xterm.Restore(fd, old)
	terminal := &screenWatchTerminal{path: path, t: t}
	terminal.version.Store(1)
	tall := os.Getenv("LOTOR_CLI_SCREEN_CASE") == "watch-tall"
	commands = append(commands, &command{name: "screen-watch", run: func(s *session, ctx context.Context, _ input) error {
		return s.repaint(ctx, 50*time.Millisecond, func() error {
			version := terminal.version.Load()
			if !tall {
				_, err := fmt.Fprintf(s.out, "FRAME-%d-%s12%s\r\n", version, strings.Repeat("x", 30), strings.Repeat("y", 20))
				return err
			}
			for i := range 12 {
				if _, err := fmt.Fprintf(s.out, "FRAME-%d-%02d-%s12%s\r\n", version, i, strings.Repeat("x", 27), strings.Repeat("y", 20)); err != nil {
					return err
				}
			}
			return nil
		})
	}})
	s := &session{colors: true, out: syncOut(terminal), path: []string{"station", "szer"}, deps: Deps{
		Privilege: Admin, SystemName: func() string { return "wanadoo" },
		Stations: []StationInfo{{Name: "szer", Protocol: "meshcore"}},
		Kinds:    []schema.Kind{{Name: "station", ChoiceAttr: "protocol"}},
	}}
	writeScreenState(t, path, terminal.state)
	s.serveEdited(t.Context(), terminal, terminal.terminalDimensions().width)
}

type screenWatchTerminal struct {
	path    string
	t       *testing.T
	state   terminalScreenState
	version atomic.Int64
	resize  func(terminalDimensions)
}

func (r *screenWatchTerminal) Read(p []byte) (int, error) {
	for {
		n, err := os.Stdin.Read(p)
		out := 0
		for _, key := range p[:n] {
			switch key {
			case 0x1c:
				r.version.Add(1)
			case 0x1d:
				if r.resize != nil {
					r.resize(r.terminalDimensions())
				}
				r.state.Sequence++
				writeScreenState(r.t, r.path, r.state)
			default:
				p[out] = key
				out++
			}
		}
		if out > 0 || err != nil {
			return out, err
		}
	}
}

func (r *screenWatchTerminal) Write(p []byte) (int, error) { return os.Stdout.Write(p) }

func (r *screenWatchTerminal) terminalDimensions() terminalDimensions {
	width, height, err := xterm.GetSize(int(os.Stdin.Fd()))
	if err != nil {
		r.t.Fatal(err)
	}
	return terminalDimensions{width: width, height: height}
}

func (r *screenWatchTerminal) onTerminalResize(handler func(terminalDimensions)) {
	r.resize = handler
}

func writeScreenState(t *testing.T, path string, state terminalScreenState) {
	t.Helper()
	data, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path+".next", data, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(path+".next", path); err != nil {
		t.Fatal(err)
	}
}
