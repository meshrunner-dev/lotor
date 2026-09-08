package cli

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"time"
)

type terminalWriteKind uint8

const (
	terminalOutput terminalWriteKind = iota
	terminalNotice
	terminalCommandEcho
)

type terminalWrite struct {
	kind  terminalWriteKind
	bytes []byte
	reply chan terminalWritten
}

type terminalWritten struct {
	n   int
	err error
}

type terminalKey struct {
	input editorInput
	err   error
}

type queuedLine struct {
	text      string
	shown     bool
	cancelled bool
}

// terminalDisplay alone mutates the editor and writes the terminal.
// Commands keep their own goroutine so watches can consume complete lines
// while their output is serialized with keystrokes and resize events here.
type terminalDisplay struct {
	s       *session
	ed      *editor
	raw     io.Writer
	writes  chan terminalWrite
	keys    chan terminalKey
	ready   chan struct{}
	resizes chan terminalDimensions
	done    chan struct{}
	lines   chan string
	queue   []queuedLine
	// readyPrompt is false while a command owns the output. Keystrokes
	// still edit a bounded draft, but cannot paint over a progress frame.
	readyPrompt bool
	inputEnded  bool
	linesClosed bool
}

const terminalQueuedLines = 4

func (s *session) serveTerminal(ctx context.Context, r io.Reader, width int) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	d := &terminalDisplay{
		s: s, writes: make(chan terminalWrite), keys: make(chan terminalKey),
		ready: make(chan struct{}), resizes: make(chan terminalDimensions, 1),
		done: make(chan struct{}), lines: make(chan string), readyPrompt: true,
	}
	out, ok := s.out.(*syncWriter)
	if !ok {
		return // every public session entry installs its stable writer
	}
	d.raw = out.route(d)
	d.ed = newEditor(nil, terminalEcho{display: d})
	d.ed.width = width
	d.ed.prompt, d.ed.completeAt = s.promptWith, s.completeAt
	d.ed.helpFor, d.ed.paint = s.helpForLine, s.paintLine
	d.bindDimensions(r)
	s.lines = d.lines
	if !s.greeted {
		banner(d.raw, s.deps.Version, s.systemName(), s.deps.Privilege)
	}
	d.ed.render()
	commandsDone := make(chan struct{})
	go func() {
		defer close(commandsDone)
		s.runCommands(ctx, func() bool {
			select {
			case d.ready <- struct{}{}:
				return true
			case <-ctx.Done():
				return false
			}
		})
	}()
	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		readTerminalKeys(ctx, r, d.keys)
	}()
	d.run(ctx, commandsDone)
	cancel()
	<-commandsDone
	if _, interruptible := r.(interface {
		SetReadDeadline(deadline time.Time) error
	}); interruptible {
		<-readDone
	}
	// A generic Reader has no cancellation door. Its caller must close it;
	// the pump's next read/send then observes cancellation and returns.
}

func (d *terminalDisplay) run(ctx context.Context, commandsDone <-chan struct{}) {
	defer close(d.done)
	for {
		var lines chan string
		var next, echo string
		if len(d.queue) > 0 {
			lines, next = d.lines, d.queue[0].text
			echo = d.queuedEcho()
		} else if d.inputEnded && !d.linesClosed {
			close(d.lines)
			d.linesClosed = true
		}
		keys := d.keys
		if d.inputEnded || len(d.queue) >= terminalQueuedLines {
			keys = nil // bounded typeahead applies transport backpressure
		}
		select {
		case <-ctx.Done():
			return
		case <-commandsDone:
			return
		case request := <-d.writes:
			if d.writeRequest(request) {
				return
			}
		case key := <-keys:
			d.key(key)
		case <-d.ready:
			d.promptReady()
		case dimensions := <-d.resizes:
			d.ed.resize(dimensions.width, dimensions.height)
		case lines <- next:
			d.lineSent(echo)
		}
	}
}

func (d *terminalDisplay) key(key terminalKey) {
	shown := !d.ed.suspended
	done, line, err := d.ed.applyInput(key.input)
	if !done && key.err != nil && len(d.ed.buf) > 0 {
		line, err = d.ed.finishLine()
		done = true
	}
	if done && err == nil {
		d.queue = append(d.queue, queuedLine{
			text: line, shown: shown,
			cancelled: key.input.kind == inputControl && key.input.key == 0x03,
		})
		d.ed.reset()
		d.ed.walk = -1
		d.ed.search = nil
	}
	if err != nil || key.err != nil {
		d.inputEnded = true
	}
	d.ed.suspended = d.inputEnded || !d.readyPrompt || len(d.queue) > 0
}

func (d *terminalDisplay) promptReady() {
	d.readyPrompt = true
	d.ed.screenRow, d.ed.viewRows, d.ed.viewTop = 0, 0, 0
	d.ed.suspended = d.inputEnded || len(d.queue) > 0
	if !d.ed.suspended && !d.inputEnded {
		d.ed.render()
	}
}

func (d *terminalDisplay) queuedEcho() string {
	line := d.queue[0]
	if !d.readyPrompt || line.shown {
		return ""
	}
	shown := d.s.paintLine(line.text)
	if line.cancelled {
		shown = "^C"
	}
	return d.s.prompt() + shown + "\r\n"
}

func (d *terminalDisplay) lineSent(echo string) {
	d.queue = d.queue[1:]
	// The spelling was prepared before delivery, while the command's old
	// context still held. The owner emits it before serving command output.
	fmt.Fprint(d.raw, echo)
	d.readyPrompt = false
	d.ed.suspended = true
}

func (d *terminalDisplay) writeRequest(request terminalWrite) (closing bool) {
	var n int
	var err error
	switch request.kind {
	case terminalNotice:
		if !d.ed.suspended {
			d.ed.clearBlock()
		}
		n, err = fmt.Fprintf(d.raw, "\r\x1b[K%s\r\n", request.bytes)
		closing = true
	case terminalCommandEcho:
		n, err = fmt.Fprint(d.raw, "\r\x1b[K", d.s.prompt(), d.s.paintLine(string(request.bytes)), "\r\n")
	case terminalOutput:
		n, err = d.raw.Write(request.bytes)
	}
	request.reply <- terminalWritten{n: n, err: err}
	return closing
}

func (d *terminalDisplay) Write(p []byte) (int, error) {
	return d.request(terminalWrite{bytes: p})
}

func (d *terminalDisplay) echoCommand(line string) {
	_, _ = d.request(terminalWrite{kind: terminalCommandEcho, bytes: []byte(line)})
}

func (d *terminalDisplay) notice(text string) {
	_, _ = d.request(terminalWrite{kind: terminalNotice, bytes: []byte(text)})
}

func (d *terminalDisplay) request(write terminalWrite) (int, error) {
	write.reply = make(chan terminalWritten, 1)
	select {
	case d.writes <- write:
	case <-d.done:
		return 0, io.ErrClosedPipe
	}
	select {
	case result := <-write.reply:
		return result.n, result.err
	case <-d.done:
		return 0, io.ErrClosedPipe
	}
}

// terminalEcho is only called by the owner through editor operations.
// It also silences direct help/control-key writes while a command runs.
type terminalEcho struct{ display *terminalDisplay }

func (w terminalEcho) Write(p []byte) (int, error) {
	if w.display.ed.suspended {
		return len(p), nil
	}
	return w.display.raw.Write(p)
}

func readTerminalKeys(ctx context.Context, r io.Reader, keys chan<- terminalKey) {
	if conn, ok := r.(interface {
		SetReadDeadline(deadline time.Time) error
	}); ok {
		stop := context.AfterFunc(ctx, func() { _ = conn.SetReadDeadline(time.Now()) })
		defer stop()
	}
	var decoder inputDecoder
	reader := bufio.NewReader(r)
	send := func(key terminalKey) bool {
		if key.input.kind == inputNone && key.err == nil {
			return true
		}
		select {
		case keys <- key:
			return true
		case <-ctx.Done():
			return false
		}
	}
	for {
		b, err := reader.ReadByte()
		if err != nil {
			send(terminalKey{input: decoder.finish(), err: err})
			return
		}
		first, next := decoder.decode(b)
		if !send(terminalKey{input: first}) || !send(terminalKey{input: next}) {
			return
		}
	}
}

func (d *terminalDisplay) bindDimensions(r io.Reader) {
	if source, ok := r.(interface{ terminalDimensions() terminalDimensions }); ok {
		dimensions := source.terminalDimensions()
		if dimensions.width > 0 {
			d.ed.width = dimensions.width
		}
		d.ed.height = dimensions.height
	}
	if source, ok := r.(interface {
		onTerminalResize(handler func(terminalDimensions))
	}); ok {
		source.onTerminalResize(func(dimensions terminalDimensions) {
			select {
			case d.resizes <- dimensions:
			default:
				select {
				case previous := <-d.resizes:
					if dimensions.width <= 0 {
						dimensions.width = previous.width
					}
					if dimensions.height <= 0 {
						dimensions.height = previous.height
					}
				default:
				}
				select {
				case d.resizes <- dimensions:
				default:
				}
			}
		})
	}
}

// replayedTerminal preserves the transport's metadata hooks while replaying
// keys consumed during detection. A plain io.MultiReader would hide them.
type replayedTerminal struct {
	io.Reader

	source io.ReadWriter
}

func (r *replayedTerminal) terminalDimensions() terminalDimensions {
	if source, ok := r.source.(interface{ terminalDimensions() terminalDimensions }); ok {
		return source.terminalDimensions()
	}
	return terminalDimensions{}
}

func (r *replayedTerminal) onTerminalResize(handler func(terminalDimensions)) {
	if source, ok := r.source.(interface {
		onTerminalResize(handler func(terminalDimensions))
	}); ok {
		source.onTerminalResize(handler)
	}
}

func (r *replayedTerminal) SetReadDeadline(deadline time.Time) error {
	if source, ok := r.source.(interface {
		SetReadDeadline(deadline time.Time) error
	}); ok {
		return source.SetReadDeadline(deadline)
	}
	return nil
}
