package sx126x

import (
	"reflect"
	"strings"
	"testing"

	"meshrunner.dev/lotor/internal/schema"
)

func TestSchemaCoversEverySetting(t *testing.T) {
	declared := map[string]bool{}
	for _, a := range Schema() {
		if declared[a.Name] {
			t.Errorf("schema names %q twice", a.Name)
		}
		if a.Doc == "" {
			t.Errorf("schema attr %q carries no doc line", a.Name)
		}
		declared[a.Name] = true
	}
	for f := range reflect.TypeFor[Settings]().Fields() {
		key, _, _ := strings.Cut(f.Tag.Get("yaml"), ",")
		if key == "" || key == "-" {
			t.Fatalf("field %s has no yaml key", f.Name)
		}
		if !declared[key] {
			t.Errorf("Settings field %q is not in the schema", key)
		}
		delete(declared, key)
	}
	for name := range declared {
		t.Errorf("schema attr %q matches no Settings field", name)
	}
}

func TestTCXOCandidatesAgreeWithTheDriver(t *testing.T) {
	a, ok := schema.Find(Schema(), attrTCXO)
	if !ok {
		t.Fatal("TCXO is absent from the driver schema")
	}
	for _, text := range []string{"", "1.6", "1.8", "3.3", "2.5", "auto"} {
		parsed, parseErr := schema.Parse(a, text)
		_, driverErr := tcxoFrom(text)
		if (parseErr == nil) != (driverErr == nil) {
			t.Errorf("TCXO %q: schema %v, driver %v", text, parseErr, driverErr)
		}
		if parseErr == nil && parsed != text {
			t.Errorf("TCXO %q parsed as %#v, want its string spelling", text, parsed)
		}
	}
	for _, candidate := range a.Candidates() {
		if _, err := tcxoFrom(candidate); err != nil {
			t.Errorf("candidate %q is not a driver voltage: %v", candidate, err)
		}
	}
}
