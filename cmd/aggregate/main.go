// Command aggregate rolls raw readings into the daily OEE table and detected
// downtime events. Intended to run as a scheduled job (cron / Railway job).
//
//	DATABASE_URL=... go run ./cmd/aggregate                 # rolls up yesterday + today
//	DATABASE_URL=... AGG_FROM=2026-06-01 AGG_TO=2026-06-13 go run ./cmd/aggregate
//	DATABASE_URL=... PURGE_DAYS=180 go run ./cmd/aggregate  # also drop readings older than 180d
package main

import (
	"context"
	"log/slog"
	"os"
	"strconv"
	"time"

	"github.com/iag/machine-telemetry/iot"
	"github.com/iag/machine-telemetry/pg"
)

func main() {
	configureLogger()

	connectCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	registryPool, telemetryPool, err := pg.ConnectSplit(connectCtx)
	cancel()
	if err != nil {
		slog.Error("connect Postgres", "err", err)
		os.Exit(1)
	}
	defer registryPool.Close()
	if telemetryPool != registryPool {
		defer telemetryPool.Close()
	}
	store := iot.NewSplitStore(registryPool, telemetryPool)

	ctx := context.Background()
	from, to := dateWindow()
	params := iot.AggregateParams{IdealCyclesPerMin: floatEnv("IDEAL_CYCLES_PER_MIN", 0)}

	pairs, err := store.DistinctAssetDays(ctx, from, to)
	if err != nil {
		slog.Error("distinct machine-days", "err", err)
		os.Exit(1)
	}
	slog.Info("aggregating", "from", from.Format("2006-01-02"), "to", to.Format("2006-01-02"), "machineDays", len(pairs))

	var rolled, downtimeWritten int
	for _, md := range pairs {
		readings, err := store.ReadingsForDay(ctx, md.AssetTag, md.Day)
		if err != nil {
			slog.Error("load readings", "assetTag", md.AssetTag, "day", md.Day, "err", err)
			continue
		}
		res := iot.AggregateDay(md.AssetTag, md.Day, readings, params)
		if err := store.UpsertDaily(ctx, res.Summary); err != nil {
			slog.Error("upsert daily", "assetTag", md.AssetTag, "day", md.Day, "err", err)
			continue
		}
		rolled++
		if n, err := store.InsertDowntimeEvents(ctx, res.DowntimeEvents); err != nil {
			slog.Warn("insert downtime", "assetTag", md.AssetTag, "day", md.Day, "err", err)
		} else {
			downtimeWritten += n
		}
	}
	slog.Info("aggregation complete", "dailyRows", rolled, "downtimeEvents", downtimeWritten)

	// Retention defaults to the policy the platform already declares rather
	// than to nothing.
	//
	// iag-mes migration 008 asks for compress-after-7-days and
	// retain-after-180-days on mes_machine_telemetry, but it is a TimescaleDB
	// policy and TimescaleDB is not installed on the production database — the
	// migration raises a notice and returns, so nothing enforces it. With
	// PURGE_DAYS defaulting to 0 nothing enforced it here either, and a table
	// taking a row per machine per reading grows without bound: one machine at
	// a reading a second is ~86k rows a day.
	//
	// 180 is not a new decision; it is 008's number, applied where it can
	// actually run. Set PURGE_DAYS=0 to keep everything.
	if days := intEnv("PURGE_DAYS", 180); days > 0 {
		cutoff := time.Now().UTC().AddDate(0, 0, -days)
		n, err := store.PurgeBefore(ctx, cutoff)
		if err != nil {
			slog.Error("purge", "err", err)
		} else {
			slog.Info("purged old readings", "before", cutoff.Format("2006-01-02"), "rows", n)
		}
	}
}

// dateWindow returns [from, to) as UTC midnights. Defaults to yesterday..tomorrow
// (covering yesterday + today) unless AGG_FROM/AGG_TO (YYYY-MM-DD) are set.
func dateWindow() (time.Time, time.Time) {
	now := time.Now().UTC().Truncate(24 * time.Hour)
	from := now.AddDate(0, 0, -1)
	to := now.AddDate(0, 0, 1)
	if v := os.Getenv("AGG_FROM"); v != "" {
		if d, err := time.Parse("2006-01-02", v); err == nil {
			from = d.UTC()
		}
	}
	if v := os.Getenv("AGG_TO"); v != "" {
		if d, err := time.Parse("2006-01-02", v); err == nil {
			to = d.UTC().AddDate(0, 0, 1) // inclusive end day
		}
	}
	return from, to
}

func intEnv(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

func floatEnv(key string, fallback float64) float64 {
	if v := os.Getenv(key); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return fallback
}

func configureLogger() {
	var h slog.Handler
	if os.Getenv("LOG_FORMAT") == "json" {
		h = slog.NewJSONHandler(os.Stderr, nil)
	} else {
		h = slog.NewTextHandler(os.Stderr, nil)
	}
	slog.SetDefault(slog.New(h))
}
