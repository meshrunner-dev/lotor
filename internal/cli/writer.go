package cli

import (
	"fmt"
	"io"
	"strings"
	"sync"
)

// syncWriter is the session's stable output address. Command rendering can
// be captured without replacing session.out, while notices always reach
// the visible transport. Edited sessions route that transport through the
// display owner; plain sessions serialize writes directly.
type syncWriter struct {
	mu        sync.Mutex
	w         io.Writer
	captureTo io.Writer
}

func syncOut(w io.Writer) *syncWriter { return &syncWriter{w: w} }

func (s *syncWriter) Write(b []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.captureTo != nil {
		return s.captureTo.Write(b)
	}
	return s.w.Write(b)
}

func (s *syncWriter) route(w io.Writer) io.Writer {
	s.mu.Lock()
	defer s.mu.Unlock()
	previous := s.w
	s.w = w
	return previous
}

func (s *syncWriter) capture(draw func() error) (string, error) {
	var b strings.Builder
	s.mu.Lock()
	previous := s.captureTo
	s.captureTo = &b
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.captureTo = previous
		s.mu.Unlock()
	}()
	err := draw()
	s.mu.Lock()
	body := b.String()
	s.mu.Unlock()
	return body, err
}

func (s *syncWriter) notice(text string, terminal bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if out, ok := s.w.(interface{ notice(text string) }); ok {
		out.notice(text)
		return
	}
	if terminal {
		fmt.Fprintf(s.w, "\r\x1b[K%s\r\n", text)
	} else {
		fmt.Fprintf(s.w, "\r\n%s\r\n", text)
	}
}

func (s *syncWriter) echoCommand(line string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if out, ok := s.w.(interface{ echoCommand(line string) }); ok {
		out.echoCommand(line)
	}
}
