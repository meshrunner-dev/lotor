// Package txn issues the transaction identifiers that make frames
// traceable across the daemon: every frame heard gets one, and every
// log line that concerns the frame carries it.
//
// Identifiers are 128-bit random values — the size and shape of an
// OpenTelemetry trace id, so a future exporter maps them instead of
// migrating them — but only a 12-hex-character prefix is displayed
// and logged. The prefix is greppable: searching a short id in a log
// file finds every line of the transaction, and structured fields
// such as duplicate_of chain transactions together.
package txn

import (
	"crypto/rand"
	"encoding/hex"
)

// ShortLen is the number of hex characters displayed and logged.
const ShortLen = 12

// ID is a full 128-bit transaction identifier.
type ID [16]byte

// New draws a random transaction identifier.
func New() ID {
	var id ID
	// crypto/rand.Read never fails on the platforms this daemon
	// targets; a theoretical failure would leave a zero id, which is
	// still harmless (identification is best-effort by design).
	_, _ = rand.Read(id[:])
	return id
}

// String returns the full 32-character hex form.
func (id ID) String() string { return hex.EncodeToString(id[:]) }

// Short returns the displayed form: the first ShortLen hex characters,
// a prefix of String so grep finds either from the other.
func (id ID) Short() string { return id.String()[:ShortLen] }
