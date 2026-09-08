package update

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestSelfcheckReportsFailureAndHonorsBothDeadlines(t *testing.T) {
	for _, tc := range []struct {
		name, script string
		cancel       bool
		want         error
	}{
		{"success", "exit 0", false, nil},
		{"timeout", "exec sleep 30", false, context.DeadlineExceeded},
		{"cancel", "exec sleep 30", true, context.Canceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			binary := filepath.Join(t.TempDir(), "probe")
			if err := os.WriteFile(binary, []byte("#!/bin/sh\n"+tc.script+"\n"), 0o700); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if tc.cancel {
				cancel()
			}
			timeout := 2 * time.Second
			if errors.Is(tc.want, context.DeadlineExceeded) {
				timeout = 50 * time.Millisecond
			}
			if err := runSelfcheck(ctx, binary, "config.db", timeout); !errors.Is(err, tc.want) {
				t.Fatalf("selfcheck = %v, want %v", err, tc.want)
			}
		})
	}
	var out probeOutput
	text := strings.Repeat("x", 128*1024)
	if n, err := io.Copy(&out, strings.NewReader(text)); err != nil || n != int64(len(text)) || out.buf.Len() != 64*1024 {
		t.Fatalf("probe output not bounded: %d, %d, %v", n, out.buf.Len(), err)
	}
}
