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
	"fmt"
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
func (s *Store) Migrate(ctx context.Context, migrations []Migration) error {
	shape, err := s.Shape(ctx)
	if err != nil {
		return err
	}
	target := shape
	for _, m := range migrations {
		if m.To > target {
			target = m.To
		}
	}
	if target == shape {
		return nil
	}
	if s.path != Memory {
		backup := fmt.Sprintf("%s.pre-shape%d", s.path, target)
		if _, err := s.db.ExecContext(ctx, "VACUUM INTO ?", backup); err != nil {
			return fmt.Errorf("shape backup: %w", err)
		}
	}
	for _, m := range migrations {
		if m.To <= shape {
			continue
		}
		if m.To != shape+1 {
			return fmt.Errorf("shape %d cannot reach %d — a migration is missing", shape, m.To)
		}
		if err := s.runMigration(ctx, m); err != nil {
			return fmt.Errorf("shape %d → %d (%s): %w", shape, m.To, m.Doc, err)
		}
		shape = m.To
	}
	return nil
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
