package confdb

// The region table: one relay's whole region map — entries, the
// wildcard's flags, the id allocator and the two designations — kept
// as the mesh-facing state it is. It lives beside the acl table for
// the same reason sessions do: it mutates over the air, at the
// engine's pace, not the configuration's.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
)

// regionFlagsMask is every flag bit the model defines: deny-flood,
// and the deny-direct bit the reference reserves. Anything above is
// no policy at all, and a store carrying one fails closed rather
// than running a policy nobody wrote.
const regionFlagsMask = 0x03

// RegionRow is one persisted region entry. Seq is the insertion
// order, which is load-bearing on the wire: every export and every
// match walks it.
type RegionRow struct {
	ID     uint16
	Parent uint16
	Flags  uint8
	Name   string
	Seq    int
}

// RegionsMeta is the per-relay remainder of a region map: the id
// allocator's cursor, the home and default designations, and the
// wildcard's own flags.
type RegionsMeta struct {
	NextID        uint16
	HomeID        uint16
	DefaultID     uint16
	WildcardFlags uint8
}

// LoadRegions reads one relay's region map, entries in stored order.
// A relay never written reports ok false — the caller seeds from its
// configuration or starts empty; an empty table with a meta row is a
// map someone emptied, which is different.
func (s *Store) LoadRegions(ctx context.Context, relay string,
) ([]RegionRow, RegionsMeta, bool, error) {
	var meta RegionsMeta
	var next, home, def, wild int64
	err := s.db.QueryRowContext(ctx,
		`SELECT next_id, home_id, default_id, wildcard_flags
		   FROM regions_meta WHERE relay = ?`, relay).Scan(&next, &home, &def, &wild)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, meta, false, nil
	}
	if err != nil {
		return nil, meta, false, err
	}
	// Ranges are judged on the values as stored, BEFORE any narrowing:
	// a conversion first would turn corruption into a different, valid
	// policy — wildcard_flags 256 read back as 0 is "carry every plain
	// flood", where the promise is fail-closed on a table that does
	// not restore.
	if err := within16(next, home, def); err != nil {
		return nil, meta, false, fmt.Errorf("regions meta for %q: %w", relay, err)
	}
	if wild&^regionFlagsMask != 0 {
		return nil, meta, false, fmt.Errorf(
			"regions meta for %q: wildcard_flags %d carries bits no policy defines", relay, wild)
	}
	meta = RegionsMeta{
		NextID: uint16(next), HomeID: uint16(home),
		DefaultID: uint16(def), WildcardFlags: uint8(wild),
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, parent, name, flags, seq
		   FROM regions WHERE relay = ? ORDER BY seq`, relay)
	if err != nil {
		return nil, meta, false, err
	}
	defer func() { _ = rows.Close() }()
	var out []RegionRow
	seqs := map[int64]bool{}
	for rows.Next() {
		var r RegionRow
		var id, parent, flags, seq int64
		if err := rows.Scan(&id, &parent, &r.Name, &flags, &seq); err != nil {
			return nil, meta, false, err
		}
		if err := within16(id, parent); err != nil {
			return nil, meta, false, fmt.Errorf("region row %q of %q: %w", r.Name, relay, err)
		}
		// Flags outside the defined policy bits, a sequence past what a
		// 32-bit int holds (the ARM artefact narrows there), and two
		// rows sharing a sequence all fail closed: the insertion order
		// is a functional identity — matching and every export walk it
		// — and a silent reinterpretation is a different policy.
		if flags&^regionFlagsMask != 0 {
			return nil, meta, false, fmt.Errorf(
				"region row %q of %q: flags %d carries bits no policy defines", r.Name, relay, flags)
		}
		if seq < 0 || seq > math.MaxInt32 {
			return nil, meta, false, fmt.Errorf(
				"region row %q of %q: seq %d is out of range", r.Name, relay, seq)
		}
		if seqs[seq] {
			return nil, meta, false, fmt.Errorf(
				"region rows of %q share seq %d — the wire order is ambiguous", relay, seq)
		}
		seqs[seq] = true
		r.ID, r.Parent = uint16(id), uint16(parent)
		r.Flags, r.Seq = uint8(flags), int(seq)
		out = append(out, r)
	}
	return out, meta, true, rows.Err()
}

// ReplaceRegions writes one relay's whole map in a single
// transaction. The table holds at most 32 entries, so writing the
// state entire buys the same guarantee a transaction buys the ACL: a
// crash never leaves the store holding half of two maps, and every
// mutation the engine persists is the map it is about to install.
func (s *Store) ReplaceRegions(ctx context.Context, relay string,
	entries []RegionRow, meta RegionsMeta,
) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx,
		"DELETE FROM regions WHERE relay = ?", relay); err != nil {
		return err
	}
	for i, r := range entries {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO regions(relay, id, parent, name, flags, seq)
			   VALUES(?, ?, ?, ?, ?, ?)`,
			relay, int64(r.ID), int64(r.Parent), r.Name, int64(r.Flags), int64(i)); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO regions_meta(relay, next_id, home_id, default_id, wildcard_flags)
		   VALUES(?, ?, ?, ?, ?)
		 ON CONFLICT(relay) DO UPDATE SET
		   next_id = excluded.next_id, home_id = excluded.home_id,
		   default_id = excluded.default_id, wildcard_flags = excluded.wildcard_flags`,
		relay, int64(meta.NextID), int64(meta.HomeID),
		int64(meta.DefaultID), int64(meta.WildcardFlags)); err != nil {
		return err
	}
	return tx.Commit()
}

// within16 refuses any value a uint16 cannot hold as it stands.
func within16(vs ...int64) error {
	for _, v := range vs {
		if v < 0 || v > 65535 {
			return fmt.Errorf("value %d does not fit the wire's 16 bits", v)
		}
	}
	return nil
}
