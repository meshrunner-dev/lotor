//go:build !lean

package cli

// historyDepth bounds the per-session command history. Small on
// purpose: recall, not archaeology.
const historyDepth = 32

// history remembers a session's last commands, newest first.
type history struct {
	lines []string
}

// add records a command; empty lines and immediate repeats are not
// worth remembering.
func (h *history) add(line string) {
	if line == "" || (len(h.lines) > 0 && h.lines[0] == line) {
		return
	}
	h.lines = append([]string{line}, h.lines...)
	if len(h.lines) > historyDepth {
		h.lines = h.lines[:historyDepth]
	}
}

// at returns the i-th most recent command; 0 is the newest.
func (h *history) at(i int) (string, bool) {
	if i < 0 || i >= len(h.lines) {
		return "", false
	}
	return h.lines[i], true
}

func (h *history) size() int { return len(h.lines) }
