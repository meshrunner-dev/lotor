package confdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// LoadStationState returns the opaque protocol-owned state for one station.
// The returned slice never aliases the SQLite driver's buffer.
func (s *Store) LoadStationState(ctx context.Context, station string) ([]byte, bool, error) {
	var state []byte
	err := s.db.QueryRowContext(ctx,
		"SELECT state FROM station_state WHERE station = ?", station).Scan(&state)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("load station %q state: %w", station, err)
	}
	return append([]byte(nil), state...), true, nil
}

// SaveStationState atomically replaces one station's opaque protocol state.
func (s *Store) SaveStationState(ctx context.Context, station string, state []byte) error {
	if len(state) == 0 {
		return errors.New("station state is empty")
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO station_state(station, state, updated_at) VALUES(?, ?, ?)
		ON CONFLICT(station) DO UPDATE SET state = excluded.state, updated_at = excluded.updated_at`,
		station, state, time.Now().UTC().Format(time.RFC3339Nano))
	if err != nil {
		return fmt.Errorf("save station %q state: %w", station, err)
	}
	return nil
}
