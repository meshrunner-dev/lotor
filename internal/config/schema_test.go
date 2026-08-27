package config

import (
	"reflect"
	"strings"
	"testing"

	"meshrunner.dev/lotor/internal/schema"
)

// covered checks a schema against a struct's yaml keys, with the
// fields the schema deliberately does not describe named out loud.
func covered(t *testing.T, attrs []schema.Attr, typ reflect.Type, skip map[string]string) {
	t.Helper()
	declared := map[string]bool{}
	for _, a := range attrs {
		if a.Doc == "" {
			t.Errorf("attr %q carries no doc line", a.Name)
		}
		declared[a.Name] = true
	}
	walk(t, typ, "", declared, skip)
	for name := range declared {
		t.Errorf("attr %q matches no struct field", name)
	}
}

func walk(t *testing.T, typ reflect.Type, prefix string, declared map[string]bool, skip map[string]string) {
	t.Helper()
	for f := range typ.Fields() {
		full := f.Tag.Get("yaml")
		tag, _, _ := strings.Cut(full, ",")
		if tag == "" && (f.Anonymous || strings.Contains(full, ",inline")) {
			walk(t, f.Type, prefix, declared, skip)
			continue
		}
		if _, skipped := skip[prefix+tag]; skipped {
			continue
		}
		ft := f.Type
		for ft.Kind() == reflect.Pointer {
			ft = ft.Elem()
		}
		if ft.Kind() == reflect.Struct && ft.String() != "time.Duration" && !isLeaf(ft) {
			walk(t, ft, prefix+tag+".", declared, skip)
			continue
		}
		if !declared[prefix+tag] {
			t.Errorf("struct field %q is not in the schema", prefix+tag)
		}
		delete(declared, prefix+tag)
	}
}

// isLeaf marks struct-shaped types the schema treats as one value.
func isLeaf(typ reflect.Type) bool { return typ.NumField() == 0 }

func TestStructuralSchemasCoverTheirStructs(t *testing.T) {
	covered(t, RelayAttrs(), reflect.TypeFor[Relay](), map[string]string{
		// The profile/overrides pair is the storage of the contributed
		// attributes, not an attribute itself; profile alone is.
		"overrides": "the attr storage, not an attr",
	})
	covered(t, RadioAttrs(), reflect.TypeFor[Radio](), map[string]string{
		"overrides": "the attr storage, not an attr",
	})
	covered(t, SentinelAttrs(), reflect.TypeFor[Sentinel](), nil)
	covered(t, SystemAttrs(), reflect.TypeFor[System](), nil)
	covered(t, CLIAttrs(), reflect.TypeFor[CLI](), nil)
}
