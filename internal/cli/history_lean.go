//go:build lean

package cli

// The lean build spends no memory on comfort: the history exists so
// the editor compiles, and remembers nothing.
type history struct{}

func (h *history) add(string) {}

func (h *history) at(int) (string, bool) { return "", false }

func (h *history) size() int { return 0 }
