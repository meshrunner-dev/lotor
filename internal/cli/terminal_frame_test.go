package cli

import (
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"
)

func TestTerminalFrameRewindsPhysicalRowsAndClearsShrinkingBody(t *testing.T) {
	var out strings.Builder
	f := &terminalFrame{content: frameContent{body: strings.Repeat("a", 60) + "\r\n", footer: "-- [enter stops]"}}
	if err := f.render(&out, 40, 8, false); err != nil {
		t.Fatal(err)
	}
	if f.rows != 3 || f.footerRows != 1 {
		t.Fatalf("physical frame = %d rows, %d footer rows", f.rows, f.footerRows)
	}
	out.Reset()
	f.content.body = "short\r\n"
	if err := f.render(&out, 40, 8, false); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(out.String(), "\x1b[2A\r\x1b[Jshort") || f.rows != 2 {
		t.Fatalf("shrinking redraw = %q, %d rows", out.String(), f.rows)
	}
}

func TestTerminalFrameBoundsBodyAndWrappedFooter(t *testing.T) {
	for _, height := range []int{1, 2, 3, 8} {
		var out strings.Builder
		f := &terminalFrame{content: frameContent{body: strings.Repeat("row\r\n", 20), footer: "-- [enter stops]"}}
		if err := f.render(&out, 8, height, false); err != nil {
			t.Fatal(err)
		}
		if f.rows != height || f.footerRows != min(2, height) || strings.Count(out.String(), "\r\n") != height-1 {
			t.Errorf("height %d: rows %d, footer %d, output %q", height, f.rows, f.footerRows, out.String())
		}
		out.Reset()
		if err := f.end(&out); err != nil {
			t.Fatal(err)
		}
		want := "\r\x1b[J"
		if height > 1 {
			want = "\x1b[1A" + want
		}
		if out.String() != want {
			t.Errorf("height %d: footer cleanup = %q, want %q", height, out.String(), want)
		}
	}
}

func TestTerminalFrameSharesWideGlyphAndANSIWrapping(t *testing.T) {
	var out strings.Builder
	f := &terminalFrame{content: frameContent{body: "\x1b[36mabc🦝\r\nx\x1b[0m\r\n", footer: "stop"}}
	if err := f.render(&out, 4, 8, false); err != nil {
		t.Fatal(err)
	}
	if f.rows != 4 || !strings.Contains(out.String(), "\r\n\x1b[36m🦝") || !strings.Contains(out.String(), "\r\n\x1b[36mx") {
		t.Fatalf("styled wide frame = %q, %d rows", out.String(), f.rows)
	}
}

func TestDisplayResizeRedrawsTheActiveFrameAndRetainsTypeahead(t *testing.T) {
	var out strings.Builder
	ed := newEditor(nil, io.Discard)
	ed.width, ed.height, ed.suspended = 80, 24, true
	ed.set("pending")
	d := &terminalDisplay{raw: &out, ed: ed, view: &terminalFrame{content: frameContent{
		body: strings.Repeat("x", 60) + "\r\n", footer: "-- [enter stops]",
	}}}
	if err := d.view.render(&out, 80, 24, false); err != nil {
		t.Fatal(err)
	}
	out.Reset()
	d.resize(terminalDimensions{width: 40, height: 8})
	if !strings.HasPrefix(out.String(), "\x1b[H\x1b[2J") || d.view.rows != 3 || strings.Contains(out.String(), "pending") {
		t.Fatalf("resize = %q, frame %+v", out.String(), d.view)
	}
	if string(ed.buf) != "pending" || ed.cur != len("pending") {
		t.Fatalf("resize changed typeahead: %q at %d", string(ed.buf), ed.cur)
	}
	out.Reset()
	if err := d.view.render(&out, 40, 8, false); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(out.String(), "\x1b[2A") {
		t.Fatalf("post-resize refresh used old origin: %q", out.String())
	}
}

func TestDisplayFarewellRemovesAnEntireWrappedFooter(t *testing.T) {
	var out strings.Builder
	ed := newEditor(nil, io.Discard)
	ed.suspended = true
	d := &terminalDisplay{raw: &out, ed: ed, view: &terminalFrame{content: frameContent{body: "row", footer: "-- [enter stops]"}}}
	if err := d.view.render(&out, 8, 4, false); err != nil {
		t.Fatal(err)
	}
	out.Reset()
	reply := make(chan terminalWritten, 1)
	if !d.writeRequest(terminalWrite{kind: terminalNotice, bytes: []byte("bye"), reply: reply}) {
		t.Fatal("notice did not close the display")
	}
	if d.view != nil || out.String() != "\x1b[1A\r\x1b[J\r\x1b[Kbye\r\n" {
		t.Fatalf("farewell left a footer: %q", out.String())
	}
}

type failedFrameWriter struct{}

func (failedFrameWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }

func TestRepaintDropsTheViewWhenItsFirstFrameFails(t *testing.T) {
	out := syncOut(failedFrameWriter{})
	s := &session{out: out, colors: true}
	err := s.repaint(t.Context(), time.Second, func() error {
		_, err := fmt.Fprint(s.out, "first frame\r\n")
		return err
	})
	if !errors.Is(err, io.ErrClosedPipe) || out.view != nil {
		t.Fatalf("failed first frame retained a view: %v, %+v", err, out.view)
	}
}
