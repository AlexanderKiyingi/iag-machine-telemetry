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
	// One statement for every present metric, not one per metric.
	//
	// This loop used to issue up to nine separate round trips per reading, with
	// no transaction around them — so a failure halfway left the latest-value
	// table describing a machine state that never existed. Unnesting arrays
	// makes it a single multi-row upsert: one trip, and atomic by construction.
	names := make([]string, 0, len(metrics))
	values := make([]float64, 0, len(metrics))
	units := make([]string, 0, len(metrics))
	for _, m := range metrics {
		if m.val == nil {
			continue
		}
		names = append(names, m.name)
		values = append(values, *m.val)
		units = append(units, m.unit)
	}
	if len(names) == 0 {
		return nil
	}

	q := fmt.Sprintf(`
        INSERT INTO %s (asset_tag, metric, value, unit, recorded_at)
        SELECT $1, m.metric, m.value, m.unit, $5
          FROM unnest($2::text[], $3::double precision[], $4::text[])
               AS m(metric, value, unit)
        ON CONFLICT (asset_tag, metric) DO UPDATE SET
            value = EXCLUDED.value, unit = EXCLUDED.unit, recorded_at = EXCLUDED.recorded_at`, LatestTable)
	_, err := s.op().Exec(ctx, q, r.AssetTag, names, values, units, r.TS)
	return err
}
