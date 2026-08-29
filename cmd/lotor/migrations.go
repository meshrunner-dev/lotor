package main

// The store's shape history, one migration per break, in the order
// they happened. The discipline: a commit that renames or removes a
// stored key ships its entry here, so a store written by any past
// binary heals on the way up — and the console's orphan unset stays
// what it is, a repair kit, not the plan.

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"

	"meshrunner.dev/lotor/internal/confdb"
)

// storeShape is the shape this binary writes. Shape 1 is where
// versioning began; every store from before reads as 1.
func storeMigrations() []confdb.Migration {
	return []confdb.Migration{{
		To: 2,
		Doc: "observer keys settle: neighbours_interval absorbs neighbors, " +
			"status_interval absorbs status, iata speaks uppercase",
		Run: migrateObserverKeys,
	}, {
		To: 3,
		Doc: "observers stored typed — url and friends at the top level, " +
			"from before the layering — move into an override scope",
		Run: migrateTypedObservers,
	}, {
		To: 4,
		Doc: "revisions recorded secrets in the clear before the mask asked " +
			"the schema — the history aligns with the policy",
		Run: scrubRevisionSecrets,
	}, {
		To:  5,
		Doc: "the session table gains granted, for permissions that outlive idle",
		Run: addACLGranted,
	}}
}

// addACLGranted adds the granted column to a store whose acl table
// predates it. A base that never had the table gets it whole from the
// schema, with the column; this heals the ones created in between.
func addACLGranted(ctx context.Context, tx *sql.Tx) error {
	var count int
	if err := tx.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM pragma_table_info('acl') WHERE name = 'granted'").Scan(&count); err != nil {
		return err
	}
	if count > 0 {
		return nil // already present — a base born with the column
	}
	// A table absent entirely also reads zero; guard on the table too.
	if err := tx.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='acl'").Scan(&count); err != nil {
		return err
	}
	if count == 0 {
		return nil
	}
	_, err := tx.ExecContext(ctx,
		"ALTER TABLE acl ADD COLUMN granted INTEGER NOT NULL DEFAULT 0")
	return err
}

// secretKeys is every attribute name a revision may have recorded in
// the clear before the schema-driven mask existed — wherever it sits,
// including inside the whole-object shape a remove keeps.
var secretKeys = map[string]bool{
	"identity": true, "guest_password": true, "password": true, "token": true,
}

// scrubRevisionSecrets masks those values across the whole journal.
// The mask is what the current code would have written; this aligns
// what was recorded with what the policy records.
func scrubRevisionSecrets(ctx context.Context, tx *sql.Tx) error {
	rows, err := tx.QueryContext(ctx, "SELECT id, change FROM revisions")
	if err != nil {
		return err
	}
	type patch struct {
		id     int64
		change string
	}
	var patches []patch
	func() {
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			var id int64
			var change string
			if err = rows.Scan(&id, &change); err != nil {
				return
			}
			var c any
			if err = json.Unmarshal([]byte(change), &c); err != nil {
				return
			}
			if !scrubSecrets(c) {
				continue
			}
			var raw []byte
			if raw, err = json.Marshal(c); err != nil {
				return
			}
			patches = append(patches, patch{id, string(raw)})
		}
		err = rows.Err()
	}()
	if err != nil {
		return err
	}
	for _, p := range patches {
		if _, err := tx.ExecContext(ctx,
			"UPDATE revisions SET change = ? WHERE id = ?", p.change, p.id); err != nil {
			return err
		}
	}
	return nil
}

// scrubSecrets walks a decoded change and masks every secret value in
// place, however deep the shape nested it. A secret key's value may
// be the string itself (an object shape) or the {old, new} pair a
// revision records — everything under the key is masked either way,
// because under-masking is the failure that matters here.
func scrubSecrets(node any) bool {
	dirty := false
	switch node := node.(type) {
	case map[string]any:
		for key, v := range node {
			if secretKeys[key] {
				if maskUnder(node, key, v) {
					dirty = true
				}
				continue
			}
			if scrubSecrets(v) {
				dirty = true
			}
		}
	case []any:
		for _, v := range node {
			if scrubSecrets(v) {
				dirty = true
			}
		}
	}
	return dirty
}

// maskUnder masks every string beneath one secret key, in place.
func maskUnder(parent map[string]any, key string, v any) bool {
	switch v := v.(type) {
	case string:
		if v == "" || v == maskedChange {
			return false
		}
		parent[key] = maskedChange
		return true
	case map[string]any:
		dirty := false
		for k2, v2 := range v {
			if maskUnder(v, k2, v2) {
				dirty = true
			}
		}
		return dirty
	}
	return false
}

// migrateObserverKeys heals the observer overrides the 2026-08-28
// parameter settlement renamed and collapsed by hand on the lab.
func migrateObserverKeys(ctx context.Context, tx *sql.Tx) error {
	patches, err := collectPatches(ctx, tx, healObserverAttrs)
	if err != nil {
		return err
	}
	return applyPatches(ctx, tx, patches)
}

// applyPatches writes the healed rows back.
func applyPatches(ctx context.Context, tx *sql.Tx, patches []observerPatch) error {
	for _, p := range patches {
		if _, err := tx.ExecContext(ctx,
			"UPDATE objects SET attrs = ? WHERE kind = 'mqtt' AND name = ?",
			p.attrs, p.name); err != nil {
			return err
		}
	}
	return nil
}

// observerPatch is one row's healed replacement.
type observerPatch struct{ name, attrs string }

// collectPatches reads every observer row through one healer, the
// writes left for the caller so the reader closes first.
func collectPatches(ctx context.Context, tx *sql.Tx,
	heal func(string) (string, bool, error),
) ([]observerPatch, error) {
	rows, err := tx.QueryContext(ctx,
		"SELECT name, attrs FROM objects WHERE kind = 'mqtt'")
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var patches []observerPatch
	for rows.Next() {
		var name, attrs string
		if err := rows.Scan(&name, &attrs); err != nil {
			return nil, err
		}
		healed, changed, err := heal(attrs)
		if err != nil {
			return nil, err
		}
		if changed {
			patches = append(patches, observerPatch{name, healed})
		}
	}
	return patches, rows.Err()
}

// migrateTypedObservers lifts the observers stored before the
// layering: their keys sat at the top level of the object, where the
// strict decode of the layered shape refuses them — a daemon that
// met one at load would not boot. Values move into the custom
// override scope, zero values dropped, then the key settlement of
// the previous rung applies to what moved.
func migrateTypedObservers(ctx context.Context, tx *sql.Tx) error {
	patches, err := collectPatches(ctx, tx, healTypedObserver)
	if err != nil {
		return err
	}
	return applyPatches(ctx, tx, patches)
}

// healTypedObserver rewrites one pre-layering observer; a row that
// already speaks overrides passes untouched.
func healTypedObserver(attrs string) (string, bool, error) {
	var o map[string]any
	if err := json.Unmarshal([]byte(attrs), &o); err != nil {
		return "", false, err
	}
	if _, layered := o["overrides"]; layered {
		return attrs, false, nil
	}
	scope := map[string]any{}
	for key, v := range o {
		switch v := v.(type) {
		case nil:
			continue
		case string:
			if v == "" {
				continue
			}
		case bool:
			if !v && key != "status" {
				continue // false is the default everywhere but the old status switch
			}
		case []any:
			if len(v) == 0 {
				continue
			}
		}
		scope[key] = v
	}
	next := map[string]any{
		"profile":   "",
		"disabled":  false,
		"overrides": map[string]any{"custom": scope},
	}
	raw, err := json.Marshal(next)
	if err != nil {
		return "", false, err
	}
	// The previous rung's settlement applies to what moved: the old
	// status switch collapses, the region code speaks uppercase.
	healed, _, err := healObserverAttrs(string(raw))
	return healed, true, err
}

// healObserverAttrs rewrites one observer's stored shape into the
// settled vocabulary; changed is false when there was nothing to say.
func healObserverAttrs(attrs string) (string, bool, error) {
	var o map[string]any
	if err := json.Unmarshal([]byte(attrs), &o); err != nil {
		return "", false, err
	}
	overrides, _ := o["overrides"].(map[string]any)
	changed := false
	for _, scoped := range overrides {
		kv, ok := scoped.(map[string]any)
		if !ok {
			continue
		}
		if v, held := kv["neighbors_interval"]; held {
			kv["neighbours_interval"] = v
			delete(kv, "neighbors_interval")
			changed = true
		}
		if v, held := kv["neighbors"]; held {
			if on, _ := v.(bool); on {
				if _, has := kv["neighbours_interval"]; !has {
					// The old default cadence: consent carries over.
					kv["neighbours_interval"] = "24h"
				}
			}
			delete(kv, "neighbors")
			changed = true
		}
		if v, held := kv["status"]; held {
			if on, _ := v.(bool); !on {
				// The interval is the whole switch now; zero is off.
				kv["status_interval"] = "0s"
			}
			delete(kv, "status")
			changed = true
		}
		if v, _ := kv["iata"].(string); v != "" && v != strings.ToUpper(v) {
			kv["iata"] = strings.ToUpper(v)
			changed = true
		}
	}
	if !changed {
		return attrs, false, nil
	}
	healed, err := json.Marshal(o)
	return string(healed), changed, err
}
