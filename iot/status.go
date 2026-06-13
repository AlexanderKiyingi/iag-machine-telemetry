package iot

import (
	"context"
	"fmt"
)

// ApplyMachineHotState mirrors the newest reading onto the operational
// `machines` row so the production/MES dashboards can show current state,
// last-seen, and cumulative counts without scanning the timeseries.
//
// Best-effort: the production service owns the machines table and its hot-state
// columns. Callers treat a returned error as a warning (telemetry is already
// persisted) so an out-of-sync or older registry schema never blocks ingest.
// Returns the number of rows updated (0 when the machine_id is unknown to the
// registry).
func (s *Store) ApplyMachineHotState(ctx context.Context, r Reading) (int64, error) {
	if r.MachineID == "" {
		return 0, nil
	}
	const q = `
        UPDATE machines SET
            last_state        = $2,
            last_seen_at      = $3,
            last_good_count   = COALESCE($4, last_good_count),
            last_reject_count = COALESCE($5, last_reject_count),
            updated_at        = NOW()
        WHERE id = $1`
	tag, err := s.op().Exec(ctx, q,
		r.MachineID, NormalizeState(r.State), r.TS, r.GoodCount, r.RejectCount)
	if err != nil {
		return 0, fmt.Errorf("apply machine hot-state: %w", err)
	}
	return tag.RowsAffected(), nil
}
