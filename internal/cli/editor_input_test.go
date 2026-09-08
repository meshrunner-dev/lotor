package cli

import (
	"slices"
	"strings"
	"testing"
	"unicode/utf8"
)

func decodeEditorInput(text string) []editorInput {
	var decoder inputDecoder
	var got []editorInput
	keep := func(input editorInput) {
		if input.kind != inputNone {
			got = append(got, input)
		}
	}
	for i := range len(text) {
		first, next := decoder.decode(text[i])
		keep(first)
		keep(next)
	}
	keep(decoder.finish())
	return got
}

func TestEditorInputDecodesUTF8BeforeControls(t *testing.T) {
	for _, text := range []string{"\xe9\x03\r", "\xf0\x9f\x03\r", "\xe9\x04\r"} {
		control := rune(0x03)
		if strings.ContainsRune(text, 0x04) {
			control = 0x04
		}
		want := []editorInput{{kind: inputRune, key: utf8.RuneError}, {kind: inputControl, key: control}, {kind: inputControl, key: '\r'}}
		if got := decodeEditorInput(text); !slices.Equal(got, want) {
			t.Fatalf("input=%q events=%v; want %v", text, got, want)
		}
	}
	want := []editorInput{{kind: inputRune, key: 'é'}, {kind: inputRune, key: '🦝'}, {kind: inputControl, key: '\r'}}
	if got := decodeEditorInput("é🦝\r"); !slices.Equal(got, want) {
		t.Fatalf("fragmented UTF-8 events=%v; want %v", got, want)
	}
}

func TestEditorInputFlushesIncompleteRuneOnce(t *testing.T) {
	var decoder inputDecoder
	for _, c := range []byte{0xf0, 0x9f} {
		first, next := decoder.decode(c)
		if first.kind != inputNone || next.kind != inputNone {
			t.Fatal("unfinished rune emitted early")
		}
	}
	if input := decoder.finish(); input.kind != inputRune || input.key != utf8.RuneError {
		t.Fatalf("EOF event=%v", input)
	}
	if input := decoder.finish(); input.kind != inputNone {
		t.Fatalf("second EOF repeated event=%v", input)
	}
}

func TestEditorInputSeparatesReportsFromEditingKeys(t *testing.T) {
	text := "\x1b[30;1R\x1b[<0;33;21M\x1b[" + strings.Repeat("0", 10000) + "R\x1b[D\x1bOP\x1b[3~\x1b!"
	want := []editorInput{{kind: inputArrow, key: 'D'}, {kind: inputHelp}, {kind: inputDelete}, {kind: inputEscape}, {kind: inputRune, key: '!'}}
	if got := decodeEditorInput(text); !slices.Equal(got, want) {
		t.Fatalf("events=%v; want %v", got, want)
	}
}

func TestEditorInputBracketedPasteKeepsPayloadAndControls(t *testing.T) {
	want := []editorInput{{kind: inputRune, key: 'é'}, {kind: inputRune, key: '?'}, {kind: inputControl, key: 0x03}, {kind: inputControl, key: '\r'}}
	if got := decodeEditorInput("\x1b[200~é?\x03\x1b[201~\r"); !slices.Equal(got, want) {
		t.Fatalf("paste events=%v; want %v", got, want)
	}
}
