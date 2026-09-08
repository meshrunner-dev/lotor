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
	for _, name := range []string{"identity", "wide", "viewport"} {
		t.Run(name, func(t *testing.T) {
			pane := *screen
			pane.name, pane.state = name, filepath.Join(dir, name+".json")
			width, height := 80, 24
			if name == "viewport" {
				width, height = 40, 8
			}
			pane.start(t, width, height)
			switch name {
			case "identity":
				pane.identity(t)
			case "wide":
				pane.wide(t)
			case "viewport":
				pane.viewport(t)
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
	s.run(t, "new-session", "-d", "-s", s.name, "-x", strconv.Itoa(width), "-y", strconv.Itoa(height),
		"env", "LOTOR_CLI_SCREEN_HELPER="+s.state, binary, "-test.run=^TestTerminalScreenHelper$", "-test.count=1")
	s.waitState(t)
	s.waitImage(t, func(image terminalScreenImage) bool {
		return image.width == width && image.height == height && strings.Contains(image.text, strings.TrimSpace(terminalScreenPrompt))
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
