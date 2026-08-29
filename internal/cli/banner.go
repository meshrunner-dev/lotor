package cli

import (
	"fmt"
	"io"

	"meshrunner.dev/lotor/internal/product"
)

// banner writes the connection greeting: the mascot on the left, the
// product lines beside it — the system's name and the session's
// privilege included, so an operator knows which machine they reached
// and which door they came through.
func banner(w io.Writer, version, system string, priv Privilege) {
	if priv == "" {
		priv = ReadOnly
	}
	info := map[int]string{
		1: product.Name + " " + version + " on " + printable(system),
		2: product.Description + " — " + product.Homepage,
		4: string(priv) + " console",
		5: "\"help\" lists commands, \"quit\" leaves.",
	}
	fmt.Fprint(w, "\r\n")
	for i, line := range product.MascotLines() {
		if txt, ok := info[i]; ok {
			fmt.Fprintf(w, " %s  %s\r\n", line, txt)
		} else {
			fmt.Fprintf(w, " %s\r\n", line)
		}
	}
	fmt.Fprint(w, "\r\n")
}
