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

	// The reference's own answer for this verb, verbatim: sprintf(reply,
	// "> %s", node_name). Other verbs answer other shapes.
	if got := answer(t, "get name"); got != "> FR94VIN Étienne" {
		t.Errorf("get name = %q", got)
	}
	// Leading spaces are skipped before the words, as the reference does.
	if got := answer(t, "   get name"); got != "> FR94VIN Étienne" {
		t.Errorf("indented get name = %q", got)
	}
	// The pairing prefix comes off the line and back onto the answer, so
	// an application can match a reply to what it asked.
	if got := answer(t, "a1|get name"); got != "a1|> FR94VIN Étienne" {
		t.Errorf("tagged get name = %q", got)
	}
	// A line this station does not run is answered, not refused: the
	// reference sends the sentence and the application prints it.
	if got := answer(t, "set freq 869.525"); got != cliUnknown {
		t.Errorf("unknown line = %q", got)
	}
	// And an unknown line keeps its pairing prefix. handleCmdFrame says
	// so where it appends the sentence: "reply_buf may have cmd prefix
	// from 'text'".
	if got := answer(t, "a1|frobnicate"); got != "a1|"+cliUnknown {
		t.Errorf("tagged unknown line = %q", got)
	}
	// ver and board answer bare, unlike get name: the reference gives
	// each verb its own shape.
	if got := answer(t, "ver"); !strings.Contains(got, "(Build: ") || strings.HasPrefix(got, "> ") {
		t.Errorf("ver = %q", got)
	}
	if got := answer(t, "board"); got != "Lotor Virtual Station" {
		t.Errorf("board = %q", got)
	}
	// set name answers a receipt, and the rename is what get name then
	// reports.
	if got := answer(t, "set name Larsen"); got != "OK" {
		t.Errorf("set name = %q", got)
	}
	if got := answer(t, "get name"); got != "> Larsen" {
		t.Errorf("get name after set name = %q", got)
	}
	// The seven characters an advert name may not carry are refused, in
	// the reference's own words.
	if got := answer(t, "set name bad,name"); got != "Error, bad chars" {
		t.Errorf("set name with a comma = %q", got)
	}
	if got := answer(t, "get name"); got != "> Larsen" {
		t.Errorf("a refused rename changed the name: %q", got)
	}
	// A name longer than the station's field is truncated, as the
	// reference truncates into its own.
	long := strings.Repeat("z", maxStationName+10)
	if got := answer(t, "set name "+long); got != "OK" {
		t.Errorf("set name long = %q", got)
	}
	if got := answer(t, "get name"); got != "> "+long[:maxStationName] {
		t.Errorf("truncated name = %q", got)
	}
	// The name follows what the typed companion command set, since both
	// doors write one field.
	svc.handle(t.Context(), companion.SetAdvertName{Name: "Pouet"})
	if got := answer(t, "get name"); got != "> Pouet" {
		t.Errorf("get name after the typed rename = %q", got)
	}
}

func TestCLIReplyIsAResponseNotAPush(t *testing.T) {
	// A station answers the CLI on the synchronous path, so its reply
	// must survive a durable mailbox round trip like any response.
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
