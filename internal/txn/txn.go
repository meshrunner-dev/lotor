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
	"context"
	"crypto/rand"
	"encoding/hex"
)

type contextKey struct{}

// ShortLen is the number of hex characters displayed and logged.
const ShortLen = 12

// ID is a full 128-bit transaction identifier.
type ID [16]byte

// New draws a random transaction identifier.
func New() ID {
	var id ID
	// crypto/rand.Read never fails on the platforms this daemon
	// targets. If it somehow did, several zero ids would collide on
	// the journal's primary key and fuse unrelated transactions into
	// one false story — an explicit death is the honest outcome, and
	// matches what crypto/rand itself promises since Go 1.24.
	if _, err := rand.Read(id[:]); err != nil {
		panic("txn: the entropy source failed: " + err.Error())
	}
	return id
}

// String returns the full 32-character hex form.
func (id ID) String() string { return hex.EncodeToString(id[:]) }

// Short returns the displayed form: the first ShortLen hex characters,
// a prefix of String so grep finds either from the other.
func (id ID) Short() string { return id.String()[:ShortLen] }

// IsZero reports whether no transaction was assigned. Production
// frame paths replace a zero at the first seam; it remains useful to
// lightweight test devices that do not model that seam themselves.
func (id ID) IsZero() bool { return id == ID{} }

// WithContext carries a frame transaction through an API whose
// context already spans the work, notably the radio transmit seam.
// It is correlation metadata only: cancellation and deadlines remain
// the caller's.
func WithContext(ctx context.Context, id ID) context.Context {
	return context.WithValue(ctx, contextKey{}, id)
}

// FromContext retrieves frame correlation metadata when the caller
// supplied it.
func FromContext(ctx context.Context) (ID, bool) {
	id, ok := ctx.Value(contextKey{}).(ID)
	return id, ok && !id.IsZero()
}
