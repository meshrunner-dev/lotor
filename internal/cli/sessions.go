package cli

import (
	"context"
	"errors"
	"maps"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Sessions is every console session alive right now, shared by the
// listeners so any session can see the others. The daemon makes one
// and hands it to every surface through Deps.
type Sessions struct {
	mu   sync.Mutex
	next int
	open map[string]*session
}

// NewSessions makes the table the listeners share.
func NewSessions() *Sessions {
	return &Sessions{open: map[string]*session{}}
}

// add registers a session under the next number and returns its key.
// Numbers are never reused within a daemon's life, so "session 3" in
// one operator's mouth cannot quietly become a different session in
// another's.
func (t *Sessions) add(s *session) string {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.next++
	id := strconv.Itoa(t.next)
	t.open[id] = s
	return id
}

func (t *Sessions) remove(id string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.open, id)
}

// snapshot hands back the table as it stands, keys and sessions both,
// so a view can read it without holding the lock while it renders.
func (t *Sessions) snapshot() map[string]*session {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make(map[string]*session, len(t.open))
	maps.Copy(out, t.open)
	return out
}

// remoteOf names the far end of a session's transport. The local
// socket and the tests' pipes have no address worth speaking: the
// OS's file permissions were the introduction, and "local" is the
// honest word for where they came from.
func remoteOf(rw any) string {
	c, ok := rw.(interface{ RemoteAddr() net.Addr })
	if !ok {
		return "local"
	}
	addr := c.RemoteAddr()
	if addr == nil || addr.Network() == "unix" {
		return "local"
	}
	return addr.String()
}

// register puts this session in the shared table for the length of
// its life. The id doubles as the row an operator sees in
// /cli/sessions, and as the word they type to stand on it.
func (s *session) register() func() {
	if s.deps.Sessions == nil {
		return func() {}
	}
	s.began = time.Now()
	s.id = s.deps.Sessions.add(s)
	return func() { s.deps.Sessions.remove(s.id) }
}

// beginWatch claims the session's one live-view slot, and reports
// whether it was free. The flag is read by other sessions through
// /cli/sessions, which is why it travels under the lock.
func (s *session) beginWatch() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.watching {
		return false
	}
	s.watching = true
	return true
}

func (s *session) endWatch() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.watching = false
}

// isWatching answers for another session's benefit.
func (s *session) isWatching() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.watching
}

// sessionKeys is the sessions drawer's answer: the ids, each named by
// who holds it and from where.
func (s *session) sessionKeys(string) map[string]string {
	if s.deps.Sessions == nil {
		return nil
	}
	out := map[string]string{}
	for id, other := range s.deps.Sessions.snapshot() {
		out[id] = string(other.privilege()) + " from " + other.remote
	}
	return out
}

// sessionView reads the table for printing. Everything here is either
// immutable for the session's life or read under its own lock, because
// the reader is one session and the subject is another.
func (s *session) sessionView(_ context.Context, _ string) (drawerView, error) {
	if s.deps.Sessions == nil {
		return drawerView{}, errors.New("this daemon keeps no session table")
	}
	v := drawerView{
		header: []string{"#", "WHO", "FROM", "AT", "DOING", "SINCE", "TERM"},
		rows:   map[string][]field{},
	}
	open := s.deps.Sessions.snapshot()
	for id, other := range open {
		at := "/" + strings.Join(other.curPath(), "/")
		doing := "at the prompt"
		if other.isWatching() {
			doing = "watching"
		}
		if other == s {
			doing += " — this session"
		}
		term := "terminal"
		if !other.colors {
			term = "plain"
		}
		v.keys = append(v.keys, id)
		v.rows[id] = []field{
			{name: "who", value: string(other.privilege())},
			{name: "from", value: other.remote},
			{name: "at", value: at},
			{name: "doing", value: doing},
			{name: "since", value: ago(other.began)},
			{name: "term", value: term},
		}
	}
	// Numeric order, not lexical: session 10 comes after 9.
	sort.Slice(v.keys, func(i, j int) bool {
		a, _ := strconv.Atoi(v.keys[i])
		b, _ := strconv.Atoi(v.keys[j])
		return a < b
	})
	return v, nil
}

// privilege is what this session may do, with the default spelled out.
func (s *session) privilege() Privilege {
	if s.deps.Privilege == "" {
		return ReadOnly
	}
	return s.deps.Privilege
}
