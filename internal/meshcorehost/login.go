package meshcorehost

// The login: what a password attempt earns, judged on a candidate so a
// refused word or a replayed recording leaves the table untouched.

import (
	"time"

	"meshrunner.dev/pkg/meshcore"
)

// LoginMaxSkew bounds how far a login's own timestamp may sit from
// ours before we read it as a recording rather than a request. A
// session that does not survive a restart is a session an attacker
// can resurrect by replaying the login that made it, rolling its
// replay clock back to the capture; nothing in the packet says how old
// it is, so our own clock does. The window is generous — a companion's
// clock is its own — but finite, which is the part the reference's
// RTC-less nodes cannot afford.
const LoginMaxSkew = 24 * time.Hour

// Refusal says why a login earned silence; the empty value admits.
type Refusal string

const (
	// RefusedWord is a password no door opens for.
	RefusedWord Refusal = "refused"
	// RefusedReplay is a login stamped at or before the client's last.
	RefusedReplay Refusal = "replay"
	// RefusedSkew is a login stamped too far from our own clock.
	RefusedSkew Refusal = "stale-or-future"
)

// Doors resolves the role a password earns for a role's own posture —
// the repeater's admin word and guest door, the room's admin, member
// and read-only words. ok false means the word opens nothing.
type Doors func(password string) (perms byte, ok bool)

// Skewed reports whether a login's timestamp sits outside the window
// around now.
func Skewed(ts uint32, now time.Time) bool {
	skew := now.Sub(time.Unix(int64(ts), 0))
	return skew > LoginMaxSkew || skew < -LoginMaxSkew
}

// Admit resolves a password attempt into the session it earns, or nil
// with the reason it earned silence.
//
// Everything is composed on a candidate and nothing touches the live
// session: a refused attempt — a wrong word, a replay — must leave
// the table exactly as it found it. Writing the role first and
// checking the timestamp after let an old guest login, captured
// before that same key was promoted, demote the admin it replayed
// against while being correctly refused.
//
// A blank word from a known key is the reference's recheck and keeps
// the role a grant recorded. Any other word is resolved by the doors;
// a password login sets the role the word earns — the reference
// rewrites the bits on every one, demotion included — and a demoted
// entry is no grant either. The route a client taught survives its
// next login, whatever the password, as it does in the reference:
// ClientACL::putClient returns a known entry untouched, and only a new
// one is blanked. A successful login is also the one operation that
// reopens an operator-closed durable session.
func Admit(live *Client, senderPub, secret []byte, password string, ts uint32, doors Doors) (*Client, Refusal) {
	var c Client
	if live != nil {
		// A shallow copy: Out is replaced, never written through, so
		// the live session's own route is untouched until this one is
		// installed in its place.
		c = *live
	} else {
		copy(c.PubKey[:], senderPub)
	}
	if password != "" || live == nil {
		role, ok := doors(password)
		if !ok {
			return nil, RefusedWord
		}
		c.Perms = (c.Perms &^ meshcore.PermRoleMask) | role
		if role == meshcore.PermGuest {
			c.Granted = false
		}
		c.Secret = secret
	}
	if ts <= c.LastTimestamp {
		return nil, RefusedReplay
	}
	c.Closed = false
	return &c, ""
}

// LoginReply composes what the reference sends back: our clock, the
// verdict, its legacy keep-alive hint, the role, the permissions, a
// random blob so two logins never hash alike, and the reply level we
// answer at.
func LoginReply(c *Client, firmwareLevel uint8, now time.Time) ([]byte, error) {
	return meshcore.FrameLoginReply(meshcore.LoginReply{
		Clock:         uint32(now.Unix()),
		Result:        meshcore.LoginOK,
		KeepAlive:     0, // legacy hint, in units of sixteen seconds
		IsAdmin:       c.IsAdmin(),
		Permissions:   c.Perms,
		FirmwareLevel: firmwareLevel,
	})
}
