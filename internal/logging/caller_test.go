package logging

import (
	"strings"
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type sinkWriter struct{ b strings.Builder }

func (w *sinkWriter) Write(p []byte) (int, error) { return w.b.Write(p) }

func TestTraceCallerIsTheCallSite(t *testing.T) {
	var sink sinkWriter
	enc := zapcore.NewConsoleEncoder(zap.NewProductionEncoderConfig())
	core := zapcore.NewCore(enc, zapcore.AddSync(&sink), TraceLevel)
	log := zap.New(core, zap.AddCaller())
	Trace(log, "who am I")
	got := sink.b.String()
	if strings.Contains(got, "logging.go") || !strings.Contains(got, "caller_test.go") {
		t.Errorf("caller wrong: %s", got)
	}
}
