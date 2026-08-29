package product

import "strings"

// Mascot is Lotor himself — Procyon lotor — in Braille shading: plain
// UTF-8 text, one column per glyph, no colour, any terminal. One
// constant string, because a package-level slice is mutable state
// anyone could deface.
const Mascot = "" +
	"⣿⣿⣿⣿⣿⡟⣛⠻⣿⣿⣿⣿⡿⢛⡻⣿⣿⣿⣿⣿\n" +
	"⣿⣿⣿⣿⣿⢰⡟⣳⣤⣶⣶⣦⣾⡋⣧⢸⣿⣿⣿⣿\n" +
	"⣿⣿⣿⡿⢃⣾⠿⢿⣿⣿⣿⣿⡿⠿⢿⣌⢿⣿⣿⣿\n" +
	"⣿⣿⠋⣴⣿⠁⢀⠀⠈⢿⣿⠏⠀⢀⠀⣻⣷⠍⣻⣿\n" +
	"⣿⣿⠟⣠⣿⣧⡸⢷⡄⠘⡏⠀⣴⠗⣠⣿⣥⡘⣿⣿\n" +
	"⣿⣿⣷⣦⣍⡻⣿⣦⠉⠀⡇⠈⢠⣾⠟⣋⣵⣾⣿⣿\n" +
	"⣿⣿⣿⣿⣿⣿⣮⡉⠀⣠⣧⡀⠈⣡⣾⣿⣿⣿⣿⣿\n" +
	"⣿⣿⣿⣿⣿⣿⣿⣿⣦⣭⣭⣵⣾⣿⣿⣿⣿⣿⣿⣿"

// MascotLines returns the mascot line by line — a fresh slice per
// call, so no caller can deface another's banner.
func MascotLines() []string {
	return strings.Split(Mascot, "\n")
}
