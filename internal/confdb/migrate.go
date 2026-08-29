package confdb

// The store's shape is coupled to the binary that wrote it: a renamed
// override key or a restructured section strands objects — or the
// whole daemon — on every machine that self-updates past the change.
// The discipline this file exists for: every commit that breaks the
// stored shape ships the migration that heals it, registered by the
// daemon and run here, in order, before the first load.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
)

// shapeKey is the meta row that says which shape the store holds.
// Absent reads as 1: every store written before versioning existed
// carries the shape the versioning began at.
const shapeKey = "shape"

// A Migration lifts the store one shape forward, inside the
// transaction it is handed — all of it lands or none of it does.
type Migration struct {
	// To is the shape this migration produces; migrations run in
	// ascending order, each from exactly To-1.
	To  int
	Doc string
	Run func(ctx context.Context, tx *sql.Tx) error
}

// CopyTo writes a consistent snapshot of the store to a fresh file —
// the probe copy a selfcheck migrates and reads so the live store is
// never touched from outside the daemon that owns it. The snapshot
// carries everything the store does, identity included, so it is
// born under a dotted name, tightened to 0600, and only then renamed
// into place: a copy that sat readable for even a moment under the
// usual umask handed the node's key to every local account.
func (s *Store) CopyTo(ctx context.Context, path string) error {
	tmp := filepath.Join(filepath.Dir(path), "."+filepath.Base(path)+".tmp")
	_ = os.Remove(tmp)
	if _, err := s.db.ExecContext(ctx, "VACUUM INTO ?", tmp); err != nil {
		return err
	}
	if err := os.Chmod(tmp, 0o600); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, path)
}

// Shape reads the store's current shape.
func (s *Store) Shape(ctx context.Context) (int, error) {
	var text string
	err := s.db.QueryRowContext(ctx,
		"SELECT value FROM meta WHERE key = ?", shapeKey).Scan(&text)
	if err == sql.ErrNoRows {
		return 1, nil
	}
	if err != nil {
		return 0, err
	}
	return strconv.Atoi(text)
}

// Migrate lifts the store to the newest registered shape, one
// migration per transaction, the shape stamp riding in the same
// transaction as the change it certifies. Before anything moves, the
// store is copied beside itself — a rollback to an older binary meets
// a shape it cannot read, and the copy is what an operator restores.
//
// The ceiling is the binary's, never the base's: a store stamped past
// what this binary supports is one a NEWER binary wrote, and touching
// it would mutate invariants this code has never heard of — the very
// thing the versioning promises cannot happen. It refuses by name
// instead. And a failed attempt is resumable: a backup left by the
// last try is the recovery point, kept and reused, never a reason the
// retry cannot start.
func (s *Store) Migrate(ctx context.Context, migrations []Migration) error {
	supported := 1
	for _, m := range migrations {
		if m.To != supported+1 {
			return fmt.Errorf(
				"migration list is broken at shape %d: after %d comes %d — ordered, no gaps, no doubles",
				m.To, supported, supported+1)
		}
		supported = m.To
	}
	shape, err := s.Shape(ctx)
	if err != nil {
		return err
	}
	if shape > supported {
		return fmt.Errorf(
			"the store is shape %d and this binary speaks at most %d — "+
				"a newer lotor wrote it; refusing to touch what it means", shape, supported)
	}
	if s.path != Memory {
		// History is corrected on every start, lifted or not: backups
		// written before snapshots were born 0600 hold the identity.
		s.tightenBackups()
	}
	if shape == supported {
		return nil
	}
	if s.path != Memory {
		backup := fmt.Sprintf("%s.pre-shape%d", s.path, supported)
		if _, err := os.Stat(backup); errors.Is(err, os.ErrNotExist) {
			if err := s.CopyTo(ctx, backup); err != nil {
				return fmt.Errorf("shape backup: %w", err)
			}
		}
		// Already there: the recovery point of an interrupted attempt,
		// worth strictly more than a fresh copy of a half-lifted store.
	}
	for _, m := range migrations {
		if m.To <= shape {
			continue
		}
		if err := s.runMigration(ctx, m); err != nil {
			return fmt.Errorf("shape %d → %d (%s): %w", shape, m.To, m.Doc, err)
		}
		shape = m.To
	}
	return nil
}

// tightenBackups closes the door on history: pre-shape copies written
// before snapshots were born 0600 carry the same identity the main
// file guards, and an upgrade is the moment to correct them.
func (s *Store) tightenBackups() {
	matches, err := filepath.Glob(s.path + ".pre-shape*")
	if err != nil {
		return
	}
	for _, m := range matches {
		_ = os.Chmod(m, 0o600)
	}
}

// runMigration is one lift, transactional, stamp included.
func (s *Store) runMigration(ctx context.Context, m Migration) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if err := m.Run(ctx, tx); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx,
		"INSERT INTO meta(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
		shapeKey, strconv.Itoa(m.To)); err != nil {
		return err
	}
	return tx.Commit()
}
