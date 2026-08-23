package cli

import (
	"fmt"
	"io"
)

// mascot is Lotor himself — Procyon lotor — in Braille shading: plain
// UTF-8 text, one column per glyph, no colour, any terminal.
var mascot = []string{
	"⣿⣿⣿⣿⣿⡟⣛⠻⣿⣿⣿⣿⡿⢛⡻⣿⣿⣿⣿⣿",
	"⣿⣿⣿⣿⣿⢰⡟⣳⣤⣶⣶⣦⣾⡋⣧⢸⣿⣿⣿⣿",
	"⣿⣿⣿⡿⢃⣾⠿⢿⣿⣿⣿⣿⡿⠿⢿⣌⢿⣿⣿⣿",
	"⣿⣿⠋⣴⣿⠁⢀⠀⠈⢿⣿⠏⠀⢀⠀⣻⣷⠍⣻⣿",
	"⣿⣿⠟⣠⣿⣧⡸⢷⡄⠘⡏⠀⣴⠗⣠⣿⣥⡘⣿⣿",
	"⣿⣿⣷⣦⣍⡻⣿⣦⠉⠀⡇⠈⢠⣾⠟⣋⣵⣾⣿⣿",
	"⣿⣿⣿⣿⣿⣿⣮⡉⠀⣠⣧⡀⠈⣡⣾⣿⣿⣿⣿⣿",
	"⣿⣿⣿⣿⣿⣿⣿⣿⣦⣭⣭⣵⣾⣿⣿⣿⣿⣿⣿⣿",
}

// banner writes the connection greeting: the mascot on the left, the
// product lines beside it.
func banner(w io.Writer, version string) {
	info := map[int]string{
		1: "Lotor " + version,
		2: "A mesh relay daemon — https://meshrunner.dev/lotor",
		4: "read-only console",
		5: "\"help\" lists commands, \"quit\" leaves.",
	}
	fmt.Fprint(w, "\r\n")
	for i, line := range mascot {
		if txt, ok := info[i]; ok {
			fmt.Fprintf(w, " %s  %s\r\n", line, txt)
		} else {
			fmt.Fprintf(w, " %s\r\n", line)
		}
	}
	fmt.Fprint(w, "\r\n")
}
