package logging

import (
	"strings"
	"testing"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func TestTheLadderGainsARung(t *testing.T) {
	lvl, err := ParseLevel("trace")
	if err != nil || lvl != TraceLevel {
		t.Fatalf("ParseLevel(trace) = %v, %v", lvl, err)
	}
	if _, err := ParseLevel("verbose"); err == nil {
		t.Error("nonsense level accepted")
	}
	if name := LevelName(TraceLevel); name != "trace" {
		t.Errorf("LevelName = %q", name)
	}
	if name := LevelName(zapcore.InfoLevel); name != "info" {
		t.Errorf("LevelName(info) = %q", name)
	}

	// A debug logger stays silent at trace; a trace logger speaks.
	core, seen := observer.New(zapcore.DebugLevel)
	log := zap.New(core)
	if On(log) {
		t.Error("debug logger claims trace is on")
	}
	Trace(log, "irq edge")
	if seen.Len() != 0 {
		t.Errorf("debug logger wrote %d trace lines", seen.Len())
	}
	core, seen = observer.New(TraceLevel)
	log = zap.New(core)
	if !On(log) {
		t.Error("trace logger claims trace is off")
	}
	Trace(log, "irq edge", zap.Bool("busy", true))
	if seen.Len() != 1 || seen.All()[0].Message != "irq edge" {
		t.Errorf("trace lost: %+v", seen.All())
	}
}

func TestTraceRendersByName(t *testing.T) {
	var enc sliceEncoder
	EncodeLevel(TraceLevel, &enc)
	EncodeLevel(zapcore.WarnLevel, &enc)
	// Padded, because the level is a column: the shorter words carry
	// the difference so what follows starts at one offset whatever a
	// tab stop is worth.
	if got := strings.Join(enc, ","); got != "trace,warn " {
		t.Errorf("rendered %q", got)
	}
	for _, l := range []zapcore.Level{
		TraceLevel, zapcore.DebugLevel, zapcore.InfoLevel,
		zapcore.WarnLevel, zapcore.ErrorLevel,
	} {
		var one sliceEncoder
		EncodeLevel(l, &one)
		if len(one) != 1 || len(one[0]) != levelWidth {
			t.Errorf("%v rendered %q, want %d characters", l, one, levelWidth)
		}
	}
}

// The clock stands alone, to the microsecond: journald shows whole
// seconds unless asked, so anything coarser would say nothing the
// journal has not already said.
func TestTimeRendersTheClockAlone(t *testing.T) {
	var enc sliceEncoder
	EncodeTime(time.Date(2026, 9, 2, 2, 14, 2, 557474000, time.UTC), &enc)
	if got := strings.Join(enc, ""); got != "02:14:02.557474" {
		t.Errorf("rendered %q", got)
	}
}

type sliceEncoder []string

func (s *sliceEncoder) AppendString(v string) { *s = append(*s, v) }

func (s *sliceEncoder) AppendBool(bool)             {}
func (s *sliceEncoder) AppendByteString([]byte)     {}
func (s *sliceEncoder) AppendComplex128(complex128) {}
func (s *sliceEncoder) AppendComplex64(complex64)   {}
func (s *sliceEncoder) AppendFloat64(float64)       {}
func (s *sliceEncoder) AppendFloat32(float32)       {}
func (s *sliceEncoder) AppendInt(int)               {}
func (s *sliceEncoder) AppendInt64(int64)           {}
func (s *sliceEncoder) AppendInt32(int32)           {}
func (s *sliceEncoder) AppendInt16(int16)           {}
func (s *sliceEncoder) AppendInt8(int8)             {}
func (s *sliceEncoder) AppendUint(uint)             {}
func (s *sliceEncoder) AppendUint64(uint64)         {}
func (s *sliceEncoder) AppendUint32(uint32)         {}
func (s *sliceEncoder) AppendUint16(uint16)         {}
func (s *sliceEncoder) AppendUint8(uint8)           {}
func (s *sliceEncoder) AppendUintptr(uintptr)       {}
