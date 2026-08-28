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
	}}
}

// migrateObserverKeys heals the observer overrides the 2026-08-28
// parameter settlement renamed and collapsed by hand on the lab.
func migrateObserverKeys(ctx context.Context, tx *sql.Tx) error {
	patches, err := collectObserverPatches(ctx, tx)
	if err != nil {
		return err
	}
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

// collectObserverPatches reads every observer row and heals what
// needs it, the writes left for the caller so the reader closes
// first.
func collectObserverPatches(ctx context.Context, tx *sql.Tx) ([]observerPatch, error) {
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
		healed, changed, err := healObserverAttrs(attrs)
		if err != nil {
			return nil, err
		}
		if changed {
			patches = append(patches, observerPatch{name, healed})
		}
	}
	return patches, rows.Err()
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
