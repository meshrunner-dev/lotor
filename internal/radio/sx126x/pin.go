package sx126x

// A pin reference, and the one grammar every pin attribute speaks.

import (
	"fmt"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

// Pin names one GPIO line as "offset" or "chip:offset". The bare form
// is an offset on the board's gpiochip, which is what a HAT on a
// header looks like; the qualified form names its own chip, for the
// boards that route one line to a bank the rest of the wiring does not
// share. Neither the kernel nor the device tree promises a board keeps
// its lines on one chip, and a driver that cannot say so cannot
// describe such a board at all.
//
// Chip is empty until Resolve fills it in, so a preset states the pins
// the PCB fixes without knowing what the host kernel calls its chips.
type Pin struct {
	Chip   string
	Offset int
}

// UnmarshalYAML reads both forms, and reads a bare integer as the
// offset it is: presets, and every configuration written before this
// grammar existed, say `reset_pin: 16` and must keep meaning it.
func (p *Pin) UnmarshalYAML(n *yaml.Node) error {
	var text string
	if err := n.Decode(&text); err != nil {
		return fmt.Errorf("pin: %q is not an offset or chip:offset", n.Value)
	}
	chip, offset, qualified := strings.Cut(text, ":")
	if !qualified {
		chip, offset = "", text
	}
	if qualified && chip == "" {
		return fmt.Errorf("pin: %q names no chip before the colon", text)
	}
	v, err := strconv.Atoi(strings.TrimSpace(offset))
	if err != nil {
		return fmt.Errorf("pin: %q is not an offset or chip:offset", text)
	}
	if v < 0 {
		return fmt.Errorf("pin: %q — a line offset is not negative", text)
	}
	p.Chip, p.Offset = chip, v
	return nil
}

// String is the configuration form, and the form errors name.
func (p Pin) String() string {
	if p.Chip == "" {
		return strconv.Itoa(p.Offset)
	}
	return p.Chip + ":" + strconv.Itoa(p.Offset)
}

// Resolve fills in the board's default chip for a pin that named
// none, and canonicalises the chip's spelling: the GPIO library reads
// "gpiochip0" and "/dev/gpiochip0" as the same chip, so this seam
// must too — two spellings of one line used to pass the uniqueness
// check and fail deterministically at acquisition, where the kernel
// hands a line to a single requester.
func (p Pin) Resolve(defaultChip string) Pin {
	if p.Chip == "" {
		p.Chip = defaultChip
	}
	p.Chip = strings.TrimPrefix(p.Chip, "/dev/")
	return p
}
