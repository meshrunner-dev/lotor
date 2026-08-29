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
	"fmt"
	"strings"

	"meshrunner.dev/lotor/internal/confdb"
	"meshrunner.dev/lotor/internal/config"
	"meshrunner.dev/lotor/internal/protocol"
	"meshrunner.dev/lotor/internal/radio"
	"meshrunner.dev/lotor/internal/schema"
	"meshrunner.dev/pkg/meshcore"
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
	}, {
		To: 6,
		Doc: "the transport agreement moves into the region table: accept_scopes " +
			"become region rows, default_scope the default designation, " +
			"accept_unscoped the wildcard's flood flag",
		Run: migrateScopesToRegions,
	}, {
		To: 7,
		Doc: "the region tables gain their range CHECKs — a store from before " +
			"them is rebuilt through the constrained shape",
		Run: migrateRegionBounds,
	}, {
		To: 8,
		Doc: "the shape-4 scrub missed admin_password — the journal is " +
			"rescrubbed with the completed list",
		Run: scrubRevisionSecrets,
	}}
}

// revisionMasker builds the journal's secrets mask from every schema
// the daemon knows: structural attributes, what every registered
// protocol and driver contributes, and the observers'. Derived, never
// listed by hand — a secret a future schema declares is masked the
// day it exists.
func revisionMasker(kinds []schema.Kind) func(string) string {
	secret := map[string]bool{}
	take := func(attrs []schema.Attr) {
		for _, a := range attrs {
			if a.Secret {
				secret[a.Name] = true
			}
		}
	}
	for _, k := range kinds {
		take(k.Attrs)
	}
	for _, n := range protocol.Registered() {
		if b, err := protocol.Lookup(n); err == nil {
			take(b.Schema)
		}
	}
	for _, n := range radio.Registered() {
		if d, err := radio.Lookup(n); err == nil {
			take(d.Schema)
		}
	}
	return func(raw string) string {
		var v any
		if err := json.Unmarshal([]byte(raw), &v); err != nil {
			return raw
		}
		scrubSecretsIn(v, secret)
		masked, err := json.Marshal(v)
		if err != nil {
			return raw
		}
		return string(masked)
	}
}

// migrateRegionBounds rebuilds the two region tables through the
// CHECK-constrained shape fresh stores are born with: existing rows
// copy across, and a row outside the wire's ranges fails the copy —
// fail-closed, with the pre-shape backup as the operator's rollback.
func migrateRegionBounds(ctx context.Context, tx *sql.Tx) error {
	steps := []string{
		`ALTER TABLE regions RENAME TO regions_pre7`,
		`CREATE TABLE regions(
		   relay  TEXT NOT NULL,
		   id     INTEGER NOT NULL CHECK(id BETWEEN 1 AND 65535),
		   parent INTEGER NOT NULL CHECK(parent BETWEEN 0 AND 65535),
		   name   TEXT NOT NULL,
		   flags  INTEGER NOT NULL CHECK(flags BETWEEN 0 AND 255),
		   seq    INTEGER NOT NULL CHECK(seq >= 0),
		   PRIMARY KEY(relay, id)
		 )`,
		`INSERT INTO regions SELECT relay, id, parent, name, flags, seq FROM regions_pre7`,
		`DROP TABLE regions_pre7`,
		`ALTER TABLE regions_meta RENAME TO regions_meta_pre7`,
		`CREATE TABLE regions_meta(
		   relay          TEXT PRIMARY KEY,
		   next_id        INTEGER NOT NULL CHECK(next_id BETWEEN 0 AND 65535),
		   home_id        INTEGER NOT NULL CHECK(home_id BETWEEN 0 AND 65535),
		   default_id     INTEGER NOT NULL CHECK(default_id BETWEEN 0 AND 65535),
		   wildcard_flags INTEGER NOT NULL CHECK(wildcard_flags BETWEEN 0 AND 255)
		 )`,
		`INSERT INTO regions_meta SELECT relay, next_id, home_id, default_id, wildcard_flags FROM regions_meta_pre7`,
		`DROP TABLE regions_meta_pre7`,
	}
	for _, stmt := range steps {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

// migrateScopesToRegions lifts the three scope attributes out of every
// relay's stored overrides and into the region tables the engine now
// mutates over the air. Accepted scopes become flood-allowed regions,
// flat under the wildcard; the default scope becomes the default
// designation, created if the old list somehow missed it — the
// reference auto-creates on designation, and the migration follows;
// accept_unscoped false becomes the wildcard's deny-flood flag. A
// relay that never spoke of scopes gets no meta row at all: absent
// means "never configured", which the engine seeds as the defaults,
// while an empty table with a meta row is a map someone emptied.
func migrateScopesToRegions(ctx context.Context, tx *sql.Tx) error {
	rows, err := tx.QueryContext(ctx,
		"SELECT name, attrs FROM objects WHERE kind = 'relay'")
	if err != nil {
		return err
	}
	type lift struct {
		relay, attrs string
		entries      []confdb.RegionRow
		meta         *confdb.RegionsMeta
	}
	var lifts []lift
	collect := func() error {
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			var relay, attrs string
			if err := rows.Scan(&relay, &attrs); err != nil {
				return err
			}
			healed, entries, meta, err := liftScopeAttrs(attrs)
			if err != nil {
				return err
			}
			if healed == attrs && meta == nil {
				continue // never spoke of scopes, nothing to heal
			}
			lifts = append(lifts, lift{relay, healed, entries, meta})
		}
		return rows.Err()
	}
	if err := collect(); err != nil {
		return err
	}
	for _, l := range lifts {
		if err := writeLift(ctx, tx, l.relay, l.attrs, l.entries, l.meta); err != nil {
			return err
		}
	}
	return nil
}

// writeLift lands one relay's healed attrs and, when an active policy
// translated, its region rows and meta.
func writeLift(ctx context.Context, tx *sql.Tx, relay, attrs string,
	entries []confdb.RegionRow, meta *confdb.RegionsMeta,
) error {
	if _, err := tx.ExecContext(ctx,
		"UPDATE objects SET attrs = ? WHERE kind = 'relay' AND name = ?",
		attrs, relay); err != nil {
		return err
	}
	if meta == nil {
		return nil // stripped obsolete keys, no active policy to translate
	}
	for i, r := range entries {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO regions(relay, id, parent, name, flags, seq)
			   VALUES(?, ?, ?, ?, ?, ?)`,
			relay, int64(r.ID), int64(r.Parent), r.Name, int64(r.Flags), int64(i)); err != nil {
			return err
		}
	}
	_, err := tx.ExecContext(ctx,
		`INSERT INTO regions_meta(relay, next_id, home_id, default_id, wildcard_flags)
		   VALUES(?, ?, ?, ?, ?)`,
		relay, int64(meta.NextID), int64(meta.HomeID),
		int64(meta.DefaultID), int64(meta.WildcardFlags))
	return err
}

// liftScopeAttrs strips the three scope keys from one relay's override
// scopes and translates them; meta is nil when the relay never carried
// any of them.
func liftScopeAttrs(attrs string) (string, []confdb.RegionRow, *confdb.RegionsMeta, error) {
	var o map[string]any
	if err := json.Unmarshal([]byte(attrs), &o); err != nil {
		return "", nil, nil, err
	}
	accept, defaultScope, unscoped, active, stripped := takeScopeAttrs(o)
	if !active && !stripped {
		return attrs, nil, nil, nil
	}
	if !active {
		// Only inactive profiles carried the keys: the running policy
		// was "never configured", so no region table — but the healed
		// attrs must still be written, or the strict config door
		// refuses the relay at its next load.
		healed, err := json.Marshal(o)
		return string(healed), nil, nil, err
	}

	seen := map[string]uint16{}
	var entries []confdb.RegionRow
	nextID := uint16(1)
	add := func(name string) uint16 {
		bare := strings.TrimPrefix(name, "#")
		// A private '$' scope could never match anything — the keystore
		// it promises was never implemented, and the old validation
		// refused the syntax — so a stray one in stored data migrates
		// to nothing rather than to a table the engine refuses whole.
		if bare == "" || strings.HasPrefix(bare, "$") {
			return 0
		}
		if id, held := seen[bare]; held {
			return id
		}
		id := nextID
		nextID++
		seen[bare] = id
		entries = append(entries, confdb.RegionRow{ID: id, Name: bare})
		return id
	}
	for _, n := range accept {
		add(n)
	}
	meta := &confdb.RegionsMeta{}
	if defaultScope != "" {
		meta.DefaultID = add(defaultScope)
	}
	meta.NextID = nextID
	// The region table holds 32 entries; a list past that would
	// migrate into a store the engine then refuses to attach. Failing
	// here, before anything is written, leaves the pre-shape backup as
	// the operator's rollback and names the problem.
	if len(entries) > 32 {
		return "", nil, nil, fmt.Errorf(
			"%d accepted scopes — the region table holds 32; trim accept_scopes before migrating",
			len(entries))
	}
	if unscoped != nil && !*unscoped {
		meta.WildcardFlags = uint8(meshcore.RegionDenyFlood)
	}
	healed, err := json.Marshal(o)
	return string(healed), entries, meta, err
}

// takeScopeAttrs reads the three scope keys from the SELECTED
// profile's override scope alone — the only scope Resolve ever
// applied, so the only policy that was actually in force — and strips
// them from every scope, where they are all obsolete. An inactive
// profile's scopes must not leak into the active policy: that is the
// layering's whole contract, and a migration is not above it. found
// active says whether the SELECTED scope carried any of the keys — a
// relay whose only scope keys sat in inactive profiles migrates as
// never configured, which is what its running policy was — and
// stripped whether any scope at all did, because obsolete keys left
// in place would fail the strict config door at the next load.
func takeScopeAttrs(o map[string]any) (accept []string, defaultScope string,
	unscoped *bool, active, stripped bool,
) {
	profile, _ := o["profile"].(string)
	if profile == "" {
		profile = config.CustomProfile
	}
	overrides, _ := o["overrides"].(map[string]any)
	accept, defaultScope, unscoped, active = readScopeAttrs(overrides[profile])
	for _, scoped := range overrides {
		kv, ok := scoped.(map[string]any)
		if !ok {
			continue
		}
		for _, gone := range []string{"accept_scopes", "default_scope", "accept_unscoped"} {
			if _, held := kv[gone]; held {
				delete(kv, gone)
				stripped = true
			}
		}
	}
	return accept, defaultScope, unscoped, active, stripped
}

// readScopeAttrs reads the three keys out of one override scope.
func readScopeAttrs(scoped any) (accept []string, defaultScope string, unscoped *bool, found bool) {
	kv, ok := scoped.(map[string]any)
	if !ok {
		return nil, "", nil, false
	}
	if v, held := kv["accept_scopes"]; held {
		accept = scopeValueNames(v)
		found = true
	}
	if v, held := kv["default_scope"]; held {
		if s, ok := v.(string); ok && s != "" {
			defaultScope = s
		}
		found = true
	}
	if v, held := kv["accept_unscoped"]; held {
		if b, ok := v.(bool); ok {
			unscoped = &b
		}
		found = true
	}
	return accept, defaultScope, unscoped, found
}

// scopeValueNames reads an accept_scopes value in either stored shape:
// the list the config wrote, or the comma line an operator set.
func scopeValueNames(v any) []string {
	switch v := v.(type) {
	case []any:
		out := make([]string, 0, len(v))
		for _, e := range v {
			if s, ok := e.(string); ok && s != "" {
				out = append(out, s)
			}
		}
		return out
	case string:
		return meshcore.ScopeNames(v)
	}
	return nil
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
	// admin_password was missing from the shape-4 scrub; shape 8
	// reruns the scrub with it so history aligns at last.
	"admin_password": true,
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
			if !scrubSecretsIn(c, secretKeys) {
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

// scrubSecretsIn walks a decoded change and masks every secret value
// in place, however deep the shape nested it. A secret key's value
// may be the string itself (an object shape) or the {old, new} pair a
// revision records — everything under the key is masked either way,
// because under-masking is the failure that matters here.
func scrubSecretsIn(node any, keys map[string]bool) bool {
	dirty := false
	switch node := node.(type) {
	case map[string]any:
		for key, v := range node {
			if keys[key] {
				if maskUnder(node, key, v) {
					dirty = true
				}
				continue
			}
			if scrubSecretsIn(v, keys) {
				dirty = true
			}
		}
	case []any:
		for _, v := range node {
			if scrubSecretsIn(v, keys) {
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
