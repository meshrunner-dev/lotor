// Package logging holds the daemon's one addition to zap's ladder: a
// trace level below debug. The dividing line: debug follows mesh
// traffic and the decisions made about it; trace exposes radio,
// hardware, timing and state-machine detail for developers — an IRQ
// edge, an RF measurement, a CAD verdict, an LBT retry.
package logging

import (
	"time"

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

// levelWidth is the longest word in the ladder — "trace", "debug" and
// "error" all reach it.
const levelWidth = 5

// EncodeLevel renders the ladder for the console encoder — trace by
// its name, not zap's Level(-2) — padded so the level is a column
// rather than a word. The console encoder joins its parts with a
// single space, which is not enough on its own: "info" and "trace"
// would push the caller to different offsets on consecutive lines,
// and a reader scanning down for the errors would have nothing
// straight to scan along.
func EncodeLevel(l zapcore.Level, enc zapcore.PrimitiveArrayEncoder) {
	name := LevelName(l)
	for len(name) < levelWidth {
		name += " "
	}
	enc.AppendString(name)
}

// EncodeTime renders the clock alone, to the microsecond. The date is
// left out because nothing reads these lines without one already:
// journald stamps every entry it takes, and a terminal session knows
// what day it is. The microsecond is what the journal cannot give
// back — it stores that much but shows whole seconds unless asked,
// and radio work is full of events a second apart in the log and
// milliseconds apart in truth.
func EncodeTime(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
	enc.AppendString(t.Format("15:04:05.000000"))
}

// Trace logs at the trace level; zap has no method for a level it
// does not name. The caller shown is Trace's caller, not this file.
func Trace(log *zap.Logger, msg string, fields ...zap.Field) {
	log.WithOptions(zap.AddCallerSkip(1)).Log(TraceLevel, msg, fields...)
}

// On reports whether trace is being written at all — the guard for
// call sites that would otherwise compute fields per IRQ edge.
func On(log *zap.Logger) bool {
	return log.Core().Enabled(TraceLevel)
}
