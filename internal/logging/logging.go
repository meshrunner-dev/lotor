// Package logging holds the daemon's one addition to zap's ladder: a
// trace level below debug. The dividing line: debug is an event with
// a cause an operator can act on — a refusal, a rate limit, a dial
// that failed, a measurement that converged. Trace is an instrument
// reading or a state-machine step that speaks only to whoever reads
// the driver's code — an IRQ edge, a CAD verdict, an LBT retry.
package logging

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// TraceLevel sits one rung below debug.
const TraceLevel = zapcore.DebugLevel - 1

// ParseLevel reads the daemon's level vocabulary: zap's words plus
// trace.
func ParseLevel(text string) (zapcore.Level, error) {
	if text == "trace" {
		return TraceLevel, nil
	}
	return zapcore.ParseLevel(text)
}

// LevelName is ParseLevel backwards, for showing the level in effect.
func LevelName(l zapcore.Level) string {
	if l == TraceLevel {
		return "trace"
	}
	return l.String()
}

// EncodeLevel renders the ladder for the console encoder — trace by
// its name, not zap's Level(-2).
func EncodeLevel(l zapcore.Level, enc zapcore.PrimitiveArrayEncoder) {
	if l == TraceLevel {
		enc.AppendString("trace")
		return
	}
	zapcore.LowercaseLevelEncoder(l, enc)
}

// Trace logs at the trace level; zap has no method for a level it
// does not name.
func Trace(log *zap.Logger, msg string, fields ...zap.Field) {
	log.Log(TraceLevel, msg, fields...)
}

// On reports whether trace is being written at all — the guard for
// call sites that would otherwise compute fields per IRQ edge.
func On(log *zap.Logger) bool {
	return log.Core().Enabled(TraceLevel)
}
