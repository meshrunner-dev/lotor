package sx126x

import (
	"reflect"
	"strings"
	"testing"
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
