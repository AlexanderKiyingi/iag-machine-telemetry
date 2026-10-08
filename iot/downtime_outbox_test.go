package iot

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// A detected stoppage has to reach the rest of the platform.
//
// This service writes mes_downtime_events directly. iag-mes publishes
// mes.downtime.* only from its own CreateDowntimeEvent, via the outbox, and
// the table has no trigger — so a stoppage detected here showed in the MES
// downtime list and Pareto and never reached iag-production: no time-log
// interval, no shift data, no KPI. The announcement now rides in the same
// transaction as the row.

func outboxTestStore(t *testing.T) (*Store, context.Context) {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("set TEST_DATABASE_URL to run the downtime/outbox tests")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	t.Cleanup(pool.Close)
	// Just enough of the iag-mes schema for this behaviour, shaped as the
	// real tables are: the partial unique index is what makes a re-run a
	// no-op, and the 'open' default is what used to leave finished stoppages
	// looking ongoing.
	for _, ddl := range []string{
		`CREATE TABLE IF NOT EXISTS mes_downtime_events (
			id BIGSERIAL PRIMARY KEY, asset_tag TEXT NOT NULL, category TEXT NOT NULL,
			reason TEXT, state TEXT NOT NULL DEFAULT 'open',
			started_at TIMESTAMPTZ NOT NULL, ended_at TIMESTAMPTZ,
			attrs JSONB NOT NULL DEFAULT '{}')`,
		`CREATE UNIQUE INDEX IF NOT EXISTS mes_downtime_auto_idx
			ON mes_downtime_events (asset_tag, started_at) WHERE category = 'auto'`,
		`CREATE TABLE IF NOT EXISTS mes_event_outbox (
			id BIGSERIAL PRIMARY KEY, kafka_topic TEXT NOT NULL, event_type TEXT NOT NULL,
			event_key TEXT, payload JSONB NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			available_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), dispatched_at TIMESTAMPTZ,
			attempts INT NOT NULL DEFAULT 0, last_error TEXT)`,
	} {
		if _, err := pool.Exec(ctx, ddl); err != nil {
			t.Fatalf("ddl: %v", err)
		}
	}
	return NewStore(pool), ctx
}

func countEvents(t *testing.T, s *Store, ctx context.Context, tag string) (started, ended int) {
	t.Helper()
	if err := s.op().QueryRow(ctx, `
		SELECT
		  count(*) FILTER (WHERE event_type = $2),
		  count(*) FILTER (WHERE event_type = $3)
		FROM mes_event_outbox WHERE event_key = $1`,
		tag, EventDowntimeStarted, EventDowntimeEnded).Scan(&started, &ended); err != nil {
		t.Fatal(err)
	}
	return
}

func TestADetectedStoppageIsAnnounced(t *testing.T) {
	s, ctx := outboxTestStore(t)
	tag := "MCH-" + time.Now().Format("150405.000000")
	start := time.Now().UTC().Add(-2 * time.Hour).Truncate(time.Second)
	end := start.Add(30 * time.Minute)

	n, err := s.InsertDowntimeEvents(ctx, []DowntimeEvent{{
		AssetTag: tag, StartedAt: start, EndedAt: &end, DurationMin: 30,
		Reason: "unplanned stop", Confidence: "high",
	}})
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	if n != 1 {
		t.Fatalf("wrote %d rows, want 1", n)
	}

	started, ended := countEvents(t, s, ctx, tag)
	if started != 1 || ended != 1 {
		t.Errorf("outbox has %d started / %d ended, want 1 and 1 — production reconstructs the interval from the pair", started, ended)
	}

	// A finished stoppage must not sit in the live log as ongoing.
	var state string
	if err := s.op().QueryRow(ctx,
		`SELECT state FROM mes_downtime_events WHERE asset_tag = $1`, tag).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != "closed" {
		t.Errorf("state = %q, want closed — the column defaults to 'open' and the machine would look permanently down", state)
	}

	// The payload has to be the shape iag-mes publishes, or production's
	// consumer needs a special case for device-detected stops.
	var assetTag, category, reason, ts string
	if err := s.op().QueryRow(ctx, `
		SELECT payload->>'asset_tag', payload->>'category', payload->>'reason', payload->>'timestamp'
		  FROM mes_event_outbox WHERE event_key = $1 AND event_type = $2`,
		tag, EventDowntimeStarted).Scan(&assetTag, &category, &reason, &ts); err != nil {
		t.Fatal(err)
	}
	if assetTag != tag || category != "auto" || reason == "" {
		t.Errorf("payload = %s/%s/%s", assetTag, category, reason)
	}
	if _, err := time.Parse(time.RFC3339, ts); err != nil {
		t.Errorf("timestamp %q is not RFC3339: %v", ts, err)
	}
	var topic string
	if err := s.op().QueryRow(ctx,
		`SELECT kafka_topic FROM mes_event_outbox WHERE event_key = $1 LIMIT 1`, tag).Scan(&topic); err != nil {
		t.Fatal(err)
	}
	if topic != TopicOperations {
		t.Errorf("topic = %q, want %q — iag-production subscribes to operations", topic, TopicOperations)
	}
}

// The aggregator re-runs over a trailing window, so the same stoppage is
// offered repeatedly. Re-announcing it would reopen and reclose an interval
// in iag-production every time.
func TestReRunningTheAggregatorAnnouncesNothingTwice(t *testing.T) {
	s, ctx := outboxTestStore(t)
	tag := "MCH-" + time.Now().Format("150405.000000") + "r"
	start := time.Now().UTC().Add(-3 * time.Hour).Truncate(time.Second)
	end := start.Add(10 * time.Minute)
	ev := []DowntimeEvent{{AssetTag: tag, StartedAt: start, EndedAt: &end, Reason: "unplanned stop"}}

	if _, err := s.InsertDowntimeEvents(ctx, ev); err != nil {
		t.Fatal(err)
	}
	n, err := s.InsertDowntimeEvents(ctx, ev)
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("the second run wrote %d rows, want 0", n)
	}
	started, ended := countEvents(t, s, ctx, tag)
	if started != 1 || ended != 1 {
		t.Errorf("after two runs the outbox has %d started / %d ended, want 1 and 1", started, ended)
	}
}

// A stoppage still in progress is announced as a start and nothing else, and
// stays open in the log.
func TestAnOngoingStoppageIsAnnouncedOnce(t *testing.T) {
	s, ctx := outboxTestStore(t)
	tag := "MCH-" + time.Now().Format("150405.000000") + "o"
	start := time.Now().UTC().Add(-20 * time.Minute).Truncate(time.Second)

	if _, err := s.InsertDowntimeEvents(ctx, []DowntimeEvent{{
		AssetTag: tag, StartedAt: start, Reason: "unplanned stop",
	}}); err != nil {
		t.Fatal(err)
	}
	started, ended := countEvents(t, s, ctx, tag)
	if started != 1 || ended != 0 {
		t.Errorf("outbox has %d started / %d ended, want 1 and 0", started, ended)
	}
	var state string
	if err := s.op().QueryRow(ctx,
		`SELECT state FROM mes_downtime_events WHERE asset_tag = $1`, tag).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != "open" {
		t.Errorf("state = %q, want open — the stop has not finished", state)
	}
}
