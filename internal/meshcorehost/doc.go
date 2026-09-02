// Package meshcorehost is the server side every MeshCore node role
// shares: the table of clients that logged in, the roles their
// passwords earned, the login itself with its replay and skew guards,
// the opening of what a client sealed to us, and the way an answer is
// routed home. The reference builds its repeater and its room server
// on one ClientACL for exactly this reason; this daemon's relay engine
// was the first owner of that machinery and is its first consumer here,
// the room server its second.
//
// The discipline is the engine's, kept: everything in a Table belongs
// to its owner's goroutine and carries no lock. The kernel decides what
// a login earns and how an answer travels; it does not emit, publish or
// log — the owner does, with its own queue, bus and name — and it
// composes on candidates so a refused attempt leaves the table exactly
// as it found it.
package meshcorehost
