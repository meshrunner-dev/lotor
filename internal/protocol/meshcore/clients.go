package meshcore

// The console's window onto the session table. The table belongs to
// the pipeline's goroutine and carries live credentials, so nothing
// outside reads it directly: a snapshot is an order like any other,
// served on the pipeline's own turn — and unlike every other order it
// asks for no emission, so a dry gate serves it too.

import (
	"errors"
	"time"

	"meshrunner.dev/pkg/meshcore"
)

// ClientSession is one logged-in companion, as an operator may see
// it: who, how it is reached, and how fresh the conversation is. No
// secret leaves the pipeline with it.
type ClientSession struct {
	PubKey [meshcore.PubKeySize]byte
	Admin  bool
	// Path is the route home the client taught us, one hash byte per
	// hop; HasPath false means it has not, and answers flood. The two
	// are distinct on purpose: a zero-hop path says the client is
	// adjacent, which is not the same as not knowing.
	Path        []byte
	HasPath     bool
	PathLearned time.Time
	LastActive  time.Time
}

// sessionsOrder asks the pipeline for a snapshot of the client table.
type sessionsOrder struct {
	reply chan []ClientSession
}

// ClientSessions reports the logged-in companions — any goroutine.
func (e *engine) ClientSessions() ([]ClientSession, error) {
	o := &sessionsOrder{reply: make(chan []ClientSession, 1)}
	select {
	case e.sessionsAsk <- o:
	default:
		return nil, errors.New("a session snapshot is already pending")
	}
	e.wakeReceiver()
	select {
	case rows := <-o.reply:
		return rows, nil
	case <-time.After(askWait):
		return nil, errors.New("the relay never picked the session snapshot up" +
			" — see \"status\" for what it is doing")
	}
}

// drainSessionsAsk serves a pending snapshot, on the pipeline's turn.
func (e *engine) drainSessionsAsk(now time.Time) {
	select {
	case o := <-e.sessionsAsk:
		o.reply <- e.acl.sessions(now)
	default:
	}
}

// sessions renders the table for the snapshot. Idle entries are
// skipped rather than retired: a read must not change what it reads.
func (a *acl) sessions(now time.Time) []ClientSession {
	out := make([]ClientSession, 0, len(a.by))
	for _, c := range a.by {
		if now.Sub(c.lastActive) > sessionIdle {
			continue
		}
		row := ClientSession{
			PubKey:     c.pubKey,
			Admin:      c.isAdmin(),
			LastActive: c.lastActive,
		}
		if c.out != nil {
			row.HasPath = true
			row.Path = append([]byte(nil), c.out.path...)
			row.PathLearned = c.out.learned
		}
		out = append(out, row)
	}
	return out
}
