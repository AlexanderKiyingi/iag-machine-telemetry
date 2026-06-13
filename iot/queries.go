package iot

import "fmt"

// SQL fragments for mes_machine_telemetry (Timescale hypertable). Built
// from ReadingsTable so a rename stays in one place.
var (
	sqlFromReadings = fmt.Sprintf("FROM %s", ReadingsTable)

	sqlReadingCols = "asset_tag, device_id, ts, state, spindle_rpm, feed_rate, " +
		"temperature_c, vibration_mm_s, pressure_bar, power_kw, " +
		"good_count, reject_count, cycle_count, fault_code, raw"

	sqlInsertReading = fmt.Sprintf(`
        INSERT INTO %s (
            asset_tag, device_id, ts, state, spindle_rpm, feed_rate,
            temperature_c, vibration_mm_s, pressure_bar, power_kw,
            good_count, reject_count, cycle_count, fault_code, raw
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
        ON CONFLICT (asset_tag, ts) DO NOTHING`, ReadingsTable)

	sqlLatestReading = fmt.Sprintf(`
        SELECT %s
        %s WHERE asset_tag = $1
        ORDER BY ts DESC LIMIT 1`, sqlReadingCols, sqlFromReadings)

	sqlTrackReadings = fmt.Sprintf(`
        SELECT %s
        %s
        WHERE asset_tag = $1 AND ts BETWEEN $2 AND $3
        ORDER BY ts ASC LIMIT $4`, sqlReadingCols, sqlFromReadings)

	sqlTrackReadingsAfter = fmt.Sprintf(`
        SELECT %s
        %s
        WHERE asset_tag = $1 AND ts > $2 AND ts BETWEEN $3 AND $4
        ORDER BY ts ASC LIMIT $5`, sqlReadingCols, sqlFromReadings)

	// ReadingsForDay loads only the columns the aggregator differences/sums.
	sqlReadingsForDay = fmt.Sprintf(`
        SELECT asset_tag, device_id, ts, state, spindle_rpm, feed_rate,
               temperature_c, vibration_mm_s, pressure_bar, power_kw,
               good_count, reject_count, cycle_count, fault_code, raw
        %s
        WHERE asset_tag = $1 AND ts >= $2 AND ts < $3
        ORDER BY ts`, sqlFromReadings)

	sqlDistinctAssetDays = fmt.Sprintf(`
        SELECT DISTINCT asset_tag, (ts AT TIME ZONE 'UTC')::date AS day
        %s
        WHERE ts >= $1 AND ts < $2
        ORDER BY asset_tag, day`, sqlFromReadings)

	sqlPurgeBefore = fmt.Sprintf("DELETE FROM %s WHERE ts < $1", ReadingsTable)

	sqlListDaily = fmt.Sprintf(`
        SELECT asset_tag, day, reading_count, running_minutes, idle_minutes,
               down_minutes, setup_minutes, good_total, reject_total, cycles_total,
               availability, performance, quality, oee,
               max_temperature_c, avg_spindle_rpm, first_reading, last_reading
        FROM %s
        WHERE asset_tag = $1 AND day >= $2::date AND day <= $3::date
        ORDER BY day ASC`, DailyTable)

	sqlUpsertDaily = fmt.Sprintf(`
        INSERT INTO %s (
            asset_tag, day, reading_count, running_minutes, idle_minutes,
            down_minutes, setup_minutes, good_total, reject_total, cycles_total,
            availability, performance, quality, oee,
            max_temperature_c, avg_spindle_rpm, first_reading, last_reading)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)
        ON CONFLICT (asset_tag, day) DO UPDATE SET
            reading_count     = EXCLUDED.reading_count,
            running_minutes   = EXCLUDED.running_minutes,
            idle_minutes      = EXCLUDED.idle_minutes,
            down_minutes      = EXCLUDED.down_minutes,
            setup_minutes     = EXCLUDED.setup_minutes,
            good_total        = EXCLUDED.good_total,
            reject_total      = EXCLUDED.reject_total,
            cycles_total      = EXCLUDED.cycles_total,
            availability      = EXCLUDED.availability,
            performance       = EXCLUDED.performance,
            quality           = EXCLUDED.quality,
            oee               = EXCLUDED.oee,
            max_temperature_c = EXCLUDED.max_temperature_c,
            avg_spindle_rpm   = EXCLUDED.avg_spindle_rpm,
            first_reading     = EXCLUDED.first_reading,
            last_reading      = EXCLUDED.last_reading`, DailyTable)

	// Auto-detected downtime is written into the canonical CMMS log with
	// category='auto'; duration/confidence/fault ride in attrs. The partial
	// unique index (category='auto') makes aggregator re-runs idempotent.
	sqlInsertDowntime = fmt.Sprintf(`
        INSERT INTO %s (asset_tag, category, reason, started_at, ended_at, attrs)
        VALUES ($1, 'auto', $2, $3, $4, $5::jsonb)
        ON CONFLICT (asset_tag, started_at) WHERE category = 'auto' DO NOTHING`, DowntimeTable)
)
