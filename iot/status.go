package iot

import (
	"context"
	"fmt"
)

// assetStatusForState maps a telemetry machine state onto the mes_assets.status
// domain (CHECK: running|idle|down|pm|maint). Returns "" for states that don't
// map (unknown), meaning "leave the asset status unchanged".
func assetStatusForState(state string) string {
	switch NormalizeState(state) {
	case StateRunning:
		return "running"
	case StateIdle:
		return "idle"
	case StateDown, StateBlocked:
		return "down"
	case StateSetup:
		return "maint"
	default:
		return ""
	}
}

// ApplyAssetHotState mirrors the newest reading onto the canonical machine
// master (mes_assets) and the latest-metric store (mes_asset_telemetry_latest)
// so production and the CMMS see current state and live metrics without
// scanning the timeseries.
//
// Best-effort: writes against the shared asset model and treats a returned
// error as a warning (telemetry is already persisted). Returns the number of
// mes_assets rows updated (0 when the asset_tag is unknown to the registry).
func (s *Store) ApplyAssetHotState(ctx context.Context, r Reading) (int64, error) {
	if r.AssetTag == "" {
		return 0, nil
	}
	var updated int64
	if status := assetStatusForState(r.State); status != "" {
		q := fmt.Sprintf(`UPDATE %s SET status = $2, updated_at = NOW() WHERE tag = $1`, AssetsTable)
		tag, err := s.op().Exec(ctx, q, r.AssetTag, status)
		if err != nil {
			return 0, fmt.Errorf("apply asset status: %w", err)
		}
		updated = tag.RowsAffected()
	}
	if err := s.upsertLatest(ctx, r); err != nil {
		return updated, fmt.Errorf("apply latest metrics: %w", err)
	}
	return updated, nil
}

// upsertLatest writes each present metric into mes_asset_telemetry_latest
// (asset_tag, metric) so the maintenance dashboard reads live values.
func (s *Store) upsertLatest(ctx context.Context, r Reading) error {
	type metric struct {
		name string
		val  *float64
		unit string
	}
	var f64 = func(p *int64) *float64 {
		if p == nil {
			return nil
		}
		v := float64(*p)
		return &v
	}
	metrics := []metric{
		{"spindle_rpm", r.SpindleRPM, "rpm"},
		{"feed_rate", r.FeedRate, ""},
		{"temperature_c", r.TemperatureC, "C"},
		{"vibration_mm_s", r.VibrationMmS, "mm/s"},
		{"pressure_bar", r.PressureBar, "bar"},
		{"power_kw", r.PowerKW, "kW"},
		{"good_count", f64(r.GoodCount), "ea"},
		{"reject_count", f64(r.RejectCount), "ea"},
		{"cycle_count", f64(r.CycleCount), "ea"},
	}
	q := fmt.Sprintf(`
        INSERT INTO %s (asset_tag, metric, value, unit, recorded_at)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (asset_tag, metric) DO UPDATE SET
            value = EXCLUDED.value, unit = EXCLUDED.unit, recorded_at = EXCLUDED.recorded_at`, LatestTable)
	for _, m := range metrics {
		if m.val == nil {
			continue
		}
		if _, err := s.op().Exec(ctx, q, r.AssetTag, m.name, *m.val, m.unit, r.TS); err != nil {
			return err
		}
	}
	return nil
}
