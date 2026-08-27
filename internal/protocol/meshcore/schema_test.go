package meshcore

import (
	"reflect"
	"strings"
	"testing"
)

// yamlKeys walks a struct's yaml tags, embedded inlines included.
func yamlKeys(t *testing.T, typ reflect.Type) []string {
	t.Helper()
	var out []string
	for f := range typ.Fields() {
		tag, _, _ := strings.Cut(f.Tag.Get("yaml"), ",")
		if tag == "" && f.Anonymous {
			out = append(out, yamlKeys(t, f.Type)...)
			continue
		}
		if tag == "" || tag == "-" {
			t.Fatalf("field %s has no yaml key", f.Name)
		}
		out = append(out, tag)
	}
	return out
}

func TestSchemaCoversEveryParam(t *testing.T) {
	// The schema and the struct describe the same attribute set, or
	// the console's help lies about what the engine accepts.
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
	fields := yamlKeys(t, reflect.TypeFor[params]())
	for _, key := range fields {
		if !declared[key] {
			t.Errorf("params field %q is not in the schema", key)
		}
		delete(declared, key)
	}
	for name := range declared {
		t.Errorf("schema attr %q matches no params field", name)
	}
}
