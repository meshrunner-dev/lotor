// Package confdb is the configuration's home: one SQLite file whose
// copy is a backup of the whole relay — node identity included, which
// is why the file is created unreadable to anyone but its owner. The
// journal is a different database on purpose: it churns, gets pruned,
// and may live in RAM; this one is small, durable, and the thing an
// operator saves.
//
// Every mutation lands with a revision — who, when, what, and the
// value it replaced — so the audit trail travels inside the backup
// rather than beside it.
package confdb

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
	_ "modernc.org/sqlite" // the driver under database/sql

	"meshrunner.dev/lotor/internal/config"
)

// DefaultPath is where the configuration lives unless told otherwise —
// the daemon's state directory, beside the journal it is not.
const DefaultPath = "/var/lib/lotor/config.db"

// Memory keeps the store in RAM — the test rig's path.
const Memory = ":memory:"

// The object kinds this store understands. Radios and relays are
// named instances; the sentinel and the CLI are singletons stored
// under an empty name.
const (
	KindRadio    = "radio"
	KindRelay    = "relay"
	KindSentinel = "sentinel"
	KindCLI      = "cli"
)

const schemaDDL = `
CREATE TABLE IF NOT EXISTS meta(
  key   TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS objects(
  kind  TEXT NOT NULL,
  name  TEXT NOT NULL,
  attrs TEXT NOT NULL,
  PRIMARY KEY(kind, name)
);
CREATE TABLE IF NOT EXISTS revisions(
  id        INTEGER PRIMARY KEY AUTOINCREMENT,
  at        TEXT NOT NULL,
  principal TEXT NOT NULL,
  kind      TEXT NOT NULL,
  name      TEXT NOT NULL,
  op        TEXT NOT NULL,
  change    TEXT NOT NULL
);
INSERT INTO meta(key, value) VALUES('schema_version', '1')
  ON CONFLICT(key) DO NOTHING;
`

// Store is the open configuration database.
type Store struct {
	db   *sql.DB
	path string
}

// Open creates or opens the store and tightens its permissions: the
// file carries the node's private key, so whoever reads it is the
// node.
func Open(ctx context.Context, path string) (*Store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if path != Memory {
		// DELETE journaling, not WAL: WAL leaves -wal/-shm companions
		// beside the file, and "back up the relay" must stay "copy one
		// file". Config writes are rare; durability wins over speed.
		if _, err := db.ExecContext(ctx,
			"PRAGMA journal_mode=DELETE; PRAGMA synchronous=FULL; PRAGMA busy_timeout=5000;"); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("config pragmas: %w", err)
		}
	}
	if _, err := db.ExecContext(ctx, schemaDDL); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("config schema: %w", err)
	}
	if path != Memory {
		if err := os.Chmod(path, 0o600); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("config permissions: %w", err)
		}
	}
	return &Store{db: db, path: path}, nil
}

// Close releases the database.
func (s *Store) Close() error { return s.db.Close() }

// Path reports where the store lives.
func (s *Store) Path() string { return s.path }

// Empty reports whether the store holds any configuration at all.
func (s *Store) Empty(ctx context.Context) (bool, error) {
	var n int
	if err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM objects").Scan(&n); err != nil {
		return false, err
	}
	return n == 0, nil
}

// Load assembles the whole configuration. An empty store is a valid
// one — a daemon with nothing configured comes up with its console
// and waits — but what is present must cross-validate, exactly as a
// file had to.
func (s *Store) Load(ctx context.Context) (*config.File, error) {
	rows, err := s.db.QueryContext(ctx, "SELECT kind, name, attrs FROM objects")
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	f := &config.File{
		Radios: map[string]config.Radio{},
		Relays: map[string]config.Relay{},
	}
	for rows.Next() {
		var kind, name, attrs string
		if err := rows.Scan(&kind, &name, &attrs); err != nil {
			return nil, err
		}
		if err := assign(f, kind, name, []byte(attrs)); err != nil {
			return nil, fmt.Errorf("config %s %q: %w", kind, name, err)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := f.Validate(false); err != nil {
		return nil, err
	}
	return f, nil
}

// assign decodes one object row into its place in the file shape.
func assign(f *config.File, kind, name string, attrs []byte) error {
	switch kind {
	case KindRadio:
		r, err := fromAttrs[config.Radio](attrs)
		if err != nil {
			return err
		}
		f.Radios[name] = r
	case KindRelay:
		r, err := fromAttrs[config.Relay](attrs)
		if err != nil {
			return err
		}
		f.Relays[name] = r
	case KindSentinel:
		sen, err := fromAttrs[config.Sentinel](attrs)
		if err != nil {
			return err
		}
		f.Sentinel = &sen
	case KindCLI:
		c, err := fromAttrs[config.CLI](attrs)
		if err != nil {
			return err
		}
		f.CLI = &c
	default:
		return fmt.Errorf("unknown object kind %q", kind)
	}
	return nil
}

// ImportFile replaces the store's whole content with a configuration
// assembled elsewhere — the migration door, and the restore door. One
// transaction: the store never holds half of two configurations.
func (s *Store) ImportFile(ctx context.Context, f *config.File, principal string) error {
	type object struct {
		kind, name string
		section    any
	}
	objects := make([]object, 0, len(f.Radios)+len(f.Relays)+2)
	for name, r := range f.Radios {
		objects = append(objects, object{KindRadio, name, r})
	}
	for name, r := range f.Relays {
		objects = append(objects, object{KindRelay, name, r})
	}
	if f.Sentinel != nil {
		objects = append(objects, object{KindSentinel, "", *f.Sentinel})
	}
	if f.CLI != nil {
		objects = append(objects, object{KindCLI, "", *f.CLI})
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, "DELETE FROM objects"); err != nil {
		return err
	}
	now := time.Now().UTC().Format(time.RFC3339)
	for _, o := range objects {
		attrs, err := toAttrs(o.section)
		if err != nil {
			return fmt.Errorf("config %s %q: %w", o.kind, o.name, err)
		}
		if _, err := tx.ExecContext(ctx,
			"INSERT INTO objects(kind, name, attrs) VALUES(?, ?, ?)",
			o.kind, o.name, attrs); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx,
			"INSERT INTO revisions(at, principal, kind, name, op, change) VALUES(?, ?, ?, ?, 'import', ?)",
			now, principal, o.kind, o.name, attrs); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// Change is one attribute's before and after inside a revision. A nil
// Old is an attribute that did not exist; a nil New is one removed.
type Change struct {
	Old any `json:"old,omitempty"`
	New any `json:"new,omitempty"`
}

// Replace stores one object whole and records what changed in it, in
// one transaction — the store never holds an object without the
// revision that explains it.
func (s *Store) Replace(ctx context.Context, kind, name string, section any,
	principal, op string, change map[string]Change,
) error {
	attrs, err := toAttrs(section)
	if err != nil {
		return fmt.Errorf("config %s %q: %w", kind, name, err)
	}
	diff, err := json.Marshal(change)
	if err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx,
		"INSERT INTO objects(kind, name, attrs) VALUES(?, ?, ?) "+
			"ON CONFLICT(kind, name) DO UPDATE SET attrs = excluded.attrs",
		kind, name, attrs); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx,
		"INSERT INTO revisions(at, principal, kind, name, op, change) VALUES(?, ?, ?, ?, ?, ?)",
		time.Now().UTC().Format(time.RFC3339), principal, kind, name, op, string(diff)); err != nil {
		return err
	}
	return tx.Commit()
}

// LastMutation returns the newest revision an undo may invert — a set
// or an unset, not an import, whose inverse would be a wipe nobody
// asked for.
func (s *Store) LastMutation(ctx context.Context) (*Revision, error) {
	revs, err := s.Revisions(ctx, 50)
	if err != nil {
		return nil, err
	}
	for i := range revs {
		switch revs[i].Op {
		case "set", "unset", "undo":
			return &revs[i], nil
		case "import":
			return nil, errors.New("nothing to undo — the last change is an import")
		}
	}
	return nil, errors.New("nothing to undo")
}

// Changes decodes what a revision recorded.
func (r *Revision) Changes() (map[string]Change, error) {
	out := map[string]Change{}
	if err := json.Unmarshal([]byte(r.Change), &out); err != nil {
		return nil, err
	}
	return out, nil
}

// Revision is one recorded mutation, oldest first when listed.
type Revision struct {
	ID        int64
	At        time.Time
	Principal string
	Kind      string
	Name      string
	Op        string
	Change    string
}

// Revisions lists the most recent mutations, newest first.
func (s *Store) Revisions(ctx context.Context, limit int) ([]Revision, error) {
	rows, err := s.db.QueryContext(ctx,
		"SELECT id, at, principal, kind, name, op, change FROM revisions ORDER BY id DESC LIMIT ?",
		limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []Revision
	for rows.Next() {
		var r Revision
		var at string
		if err := rows.Scan(&r.ID, &at, &r.Principal, &r.Kind, &r.Name, &r.Op, &r.Change); err != nil {
			return nil, err
		}
		if r.At, err = time.Parse(time.RFC3339, at); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// toAttrs canonicalises one section for storage: through YAML first,
// because that is the dialect the config structs speak — durations as
// "720h", strict field names — then into JSON for the database.
// Nulls are dropped: an absent block and a block that is null must
// not read differently.
func toAttrs(section any) (string, error) {
	raw, err := yaml.Marshal(section)
	if err != nil {
		return "", err
	}
	var m any
	if err := yaml.Unmarshal(raw, &m); err != nil {
		return "", err
	}
	j, err := json.Marshal(stripNulls(m))
	if err != nil {
		return "", err
	}
	return string(j), nil
}

// fromAttrs is toAttrs backwards, strict: an attribute the section
// does not know is an error, never a silently ignored setting.
func fromAttrs[T any](attrs []byte) (T, error) {
	var zero T
	var m map[string]any
	if err := json.Unmarshal(attrs, &m); err != nil {
		return zero, err
	}
	return config.Decode[T](m)
}

// stripNulls drops nil values wherever they nest.
func stripNulls(v any) any {
	m, ok := v.(map[string]any)
	if !ok {
		return v
	}
	for k, val := range m {
		if val == nil {
			delete(m, k)
			continue
		}
		m[k] = stripNulls(val)
	}
	return m
}

// ErrNotEmpty refuses an import over a store that already holds a
// configuration; replacing one is a decision, not a default.
var ErrNotEmpty = errors.New("the configuration database is not empty")
