package meshcore

import (
	"bytes"
	"strings"
	"testing"

	"meshrunner.dev/pkg/meshcore/companion"
)

func TestCLIReadsTheStationName(t *testing.T) {
	spec := testSpec(t)
	spec.Config["node_name"] = "FR94VIN Étienne"
	built, err := build(spec)
	if err != nil {
		t.Fatal(err)
	}
	svc := requireService(t, built)

	answer := func(t *testing.T, line string) string {
		t.Helper()
		return cliLine(t, svc, line)
	}

	if got := answer(t, "get name"); got != "> FR94VIN Étienne" {
		t.Errorf("get name = %q", got)
	}
	if got := answer(t, "   get name"); got != "> FR94VIN Étienne" {
		t.Errorf("indented get name = %q", got)
	}
	if got := answer(t, "a1|get name"); got != "a1|> FR94VIN Étienne" {
		t.Errorf("tagged get name = %q", got)
	}
	if got := answer(t, "set freq 869.525"); got != cliUnknown {
		t.Errorf("unknown line = %q", got)
	}
	if got := answer(t, "a1|frobnicate"); got != "a1|"+cliUnknown {
		t.Errorf("tagged unknown line = %q", got)
	}
	if got := answer(t, "ver"); !strings.Contains(got, "(Build: ") || strings.HasPrefix(got, "> ") {
		t.Errorf("ver = %q", got)
	}
	if got := answer(t, "board"); got != "Lotor Virtual Station" {
		t.Errorf("board = %q", got)
	}
	if got := answer(t, "set name Larsen"); got != "OK" {
		t.Errorf("set name = %q", got)
	}
	if got := answer(t, "get name"); got != "> Larsen" {
		t.Errorf("get name after set name = %q", got)
	}
	if got := answer(t, "set name bad,name"); got != "Error, bad chars" {
		t.Errorf("set name with a comma = %q", got)
	}
	if got := answer(t, "get name"); got != "> Larsen" {
		t.Errorf("a refused rename changed the name: %q", got)
	}
	long := strings.Repeat("z", maxStationName+10)
	if got := answer(t, "set name "+long); got != "OK" {
		t.Errorf("set name long = %q", got)
	}
	if got := answer(t, "get name"); got != "> "+long[:maxStationName] {
		t.Errorf("truncated name = %q", got)
	}
	svc.handle(t.Context(), companion.SetAdvertName{Name: "Pouet"})
	if got := answer(t, "get name"); got != "> Pouet" {
		t.Errorf("get name after the typed rename = %q", got)
	}
}

func TestCLITunesTheStationsOwnPreferences(t *testing.T) {
	built, err := build(testSpec(t))
	if err != nil {
		t.Fatal(err)
	}
	svc := requireService(t, built)
	for _, step := range []struct{ line, want string }{
		{"set radio 869.618,62.5,7,8", "OK"},
		{"get radio", "> 869.618,62.5,7,8"},
		{"set radio 869.618,62.5,13,8", "Error, invalid radio params"},
		{"set radio 869.618,62.5", "Error, invalid radio params"},
		{"set radio 3000,62.5,8,8", "Error, invalid radio params"},
		{"get radio", "> 869.618,62.5,7,8"},
		{"set radio 869.525,250,11,5,trailing", "OK"},
		{"get radio", "> 869.525,250,11,5"},

		{"set tx 20", "OK"},
		{"get tx", "> 20"},
		{"get tx now", "> 20"},
		{"get txdelay", cliUnknown},
		{"set tx -9", "OK"},
		{"get tx", "> -9"},
		{"set tx 30", "Error, invalid tx power"},
		{"get tx", "> -9"},

		{"set af 1.5", "OK"},
		{"get af", "> 1.5"},
		{"set af 2", "OK"},
		{"get af", "> 2.0"},
		{"set af 10", "ERROR: af must be 0-9"},
		{"set af NaN", "ERROR: af must be 0-9"},
		{"set af x", "ERROR: af must be 0-9"},

		{"set rxdelay 3.25", "OK"},
		{"get rxdelay", "> 3.25"},
		{"set rxdelay 21", "Error, must be 0-20"},

		{"set path.hash.mode 2", "OK"},
		{"get path.hash.mode", "> 2"},
		{"set path.hash.mode 3", "Error, must be 0,1, or 2"},

		{"set multi.acks 1", "OK"},
		{"get multi.acks", "> 1"},
		{"set multi.acks 300", "Error, must be 0-255"},
		{"get multi.acks", "> 1"},

		{"set cad on", cliUnknown},
		{"get int.thresh", cliUnknown},
		{"set dutycycle 10", cliUnknown},
		{"set txdelay 1", cliUnknown},
		{"reboot", cliUnknown},
		{"set pin 123456", cliUnknown},
		{"get wifi.ssid", cliUnknown},
		{"get tz.offset", cliUnknown},
	} {
		if got := cliLine(t, svc, step.line); got != step.want {
			t.Errorf("%q = %q, want %q", step.line, got, step.want)
		}
	}
	svc.handle(t.Context(), companion.SetTuningParams{RXDelayMilli: 500, AirtimeFactorMilli: 2250})
	if got := cliLine(t, svc, "get rxdelay"); got != "> 0.5" {
		t.Errorf("get rxdelay after the typed command = %q", got)
	}
	if got := cliLine(t, svc, "get af"); got != "> 2.25" {
		t.Errorf("get af after the typed command = %q", got)
	}
}

func cliLine(t *testing.T, svc *service, line string) string {
	t.Helper()
	responses := svc.handle(t.Context(), companion.RunCLICommand{Line: line})
	if len(responses) != 1 {
		t.Fatalf("%q answered %d responses", line, len(responses))
	}
	out, err := companion.MarshalResponse(responses[0])
	if err != nil {
		t.Fatal(err)
	}
	if out[0] != byte(companion.ResponseCLIReply) {
		t.Fatalf("%q answered code %d, want a CLI reply", line, out[0])
	}
	return string(out[1:])
}

func TestCLIReplyIsAResponseNotAPush(t *testing.T) {
	reply, err := companion.MarshalResponse(companion.CLIReply{Text: "> x"})
	if err != nil {
		t.Fatal(err)
	}
	stored, err := companion.MarshalResponse(companion.EncodedResponse{Payload: reply})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(stored, reply) {
		t.Fatalf("stored CLI reply = % X, want % X", stored, reply)
	}
}
