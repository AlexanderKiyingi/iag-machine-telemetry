package iot

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrDeviceNotFound = errors.New("device not found")
	ErrInvalidAPIKey  = errors.New("invalid device api key")
	ErrInactiveDevice = errors.New("device inactive")
)

// Store holds Postgres pools for the machine telemetry hypertable and the
// operational device registry. When telemetry is nil, all queries use
// operational (single-DB dev mode).
type Store struct {
	operational *pgxpool.Pool
	telemetry   *pgxpool.Pool

	// authCache holds api_key_hash → cachedDevice for deviceAuthTTL. Ingest
	// authenticates on every request, so without it each POST spends a round
	// trip resolving a key to a row that essentially never changes.
	authCache sync.Map
}

// NewStore wires one pool for both operational and telemetry tables (local dev).
func NewStore(pool *pgxpool.Pool) *Store {
	return NewSplitStore(pool, nil)
}

// NewSplitStore separates registry/hot-state (operational) from time-series (telemetry).
func NewSplitStore(operational, telemetry *pgxpool.Pool) *Store {
	if operational == nil {
		operational = telemetry
	}
	return &Store{operational: operational, telemetry: telemetry}
}

func (s *Store) op() *pgxpool.Pool { return s.operational }

func (s *Store) tel() *pgxpool.Pool {
	if s.telemetry != nil {
		return s.telemetry
	}
	return s.operational
}

// ─────────────────────────────── Devices ────────────────────────────────

type CreateDeviceInput struct {
	Serial   string
	Label    string
	AssetTag string
	IssueKey bool // when true, a fresh API key is generated; the plaintext is returned once
}

type CreatedDevice struct {
	Device
	APIKeyPlaintext string `json:"apiKey,omitempty"`
}

func (s *Store) CreateDevice(ctx context.Context, in CreateDeviceInput) (*CreatedDevice, error) {
	var keyHash, plaintext string
	if in.IssueKey {
		buf := make([]byte, 32)
		if _, err := rand.Read(buf); err != nil {
			return nil, err
		}
		plaintext = base64.RawURLEncoding.EncodeToString(buf)
		keyHash = hashAPIKey(plaintext)
	}
	q := fmt.Sprintf(`
        INSERT INTO %s (serial, label, asset_tag, api_key_hash)
        VALUES ($1, NULLIF($2, ''), NULLIF($3, ''), NULLIF($4, ''))
        RETURNING id, serial, COALESCE(label,''), COALESCE(asset_tag,''),
                  api_key_hash IS NOT NULL, is_active, last_seen, COALESCE(last_ip,''), created_at`, DevicesTable)
	var d Device
	err := s.op().QueryRow(ctx, q, in.Serial, in.Label, in.AssetTag, keyHash).Scan(
		&d.ID, &d.Serial, &d.Label, &d.AssetTag,
		&d.HasAPIKey, &d.IsActive, &d.LastSeen, &d.LastIP, &d.CreatedAt,
	)
	if err != nil {
		return nil, err
	}
	return &CreatedDevice{Device: d, APIKeyPlaintext: plaintext}, nil
}

func (s *Store) ListDevices(ctx context.Context) ([]Device, error) {
	q := fmt.Sprintf(`
        SELECT id, serial, COALESCE(label,''), COALESCE(asset_tag,''),
               api_key_hash IS NOT NULL, is_active, last_seen, COALESCE(last_ip,''), created_at
        FROM %s ORDER BY created_at DESC`, DevicesTable)
	rows, err := s.op().Query(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Device
	for rows.Next() {
		var d Device
		if err := rows.Scan(
			&d.ID, &d.Serial, &d.Label, &d.AssetTag,
			&d.HasAPIKey, &d.IsActive, &d.LastSeen, &d.LastIP, &d.CreatedAt,
		); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// FindBySerial authenticates an incoming TCP connection by device serial.
// Returns ErrDeviceNotFound for an unknown serial, ErrInactiveDevice if the
// device is registered but disabled.
func (s *Store) FindBySerial(ctx context.Context, serial string) (*Device, error) {
	q := fmt.Sprintf(`
        SELECT id, serial, COALESCE(label,''), COALESCE(asset_tag,''),
               api_key_hash IS NOT NULL, is_active, last_seen, COALESCE(last_ip,''), created_at
        FROM %s WHERE serial = $1`, DevicesTable)
	var d Device
	err := s.op().QueryRow(ctx, q, serial).Scan(
		&d.ID, &d.Serial, &d.Label, &d.AssetTag,
		&d.HasAPIKey, &d.IsActive, &d.LastSeen, &d.LastIP, &d.CreatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrDeviceNotFound
	}
	if err != nil {
		return nil, err
	}
	if !d.IsActive {
		return nil, ErrInactiveDevice
	}
	return &d, nil
}

// AuthenticateAPIKey resolves a device by the plaintext API key from the HTTP
// Authorization header. The supplied key is hashed and looked up directly on
// the indexed api_key_hash column.
// deviceAuthTTL is how long a successful key lookup is reused.
//
// Every ingest request authenticates, so this was one database round trip per
// POST purely to turn a key into a device row that changes approximately never.
// Thirty seconds bounds how long a revoked key or a deactivated device stays
// usable, which is the only thing the cache can get wrong.
//
// Failures are deliberately NOT cached: a wrong key must stay cheap to reject
// but must not be able to pin a negative result, and a device that was just
// activated should start working immediately rather than after a TTL.
const deviceAuthTTL = 30 * time.Second

type cachedDevice struct {
	device Device
	at     time.Time
}

// InvalidateDeviceAuth drops a cached key→device entry. Called after rotating
// or deactivating a device so the change takes effect now rather than at TTL.
func (s *Store) InvalidateDeviceAuth() {
	s.authCache.Range(func(k, _ any) bool {
		s.authCache.Delete(k)
		return true
	})
}

func (s *Store) AuthenticateAPIKey(ctx context.Context, plaintext string) (*Device, error) {
	if plaintext == "" {
		return nil, ErrInvalidAPIKey
	}
	digest := hashAPIKey(plaintext)

	if v, ok := s.authCache.Load(digest); ok {
		if hit, isEntry := v.(cachedDevice); isEntry && time.Since(hit.at) < deviceAuthTTL {
			d := hit.device
			return &d, nil
		}
		s.authCache.Delete(digest)
	}

	q := fmt.Sprintf(`
        SELECT id, serial, COALESCE(label,''), COALESCE(asset_tag,''),
               api_key_hash IS NOT NULL, is_active, last_seen, COALESCE(last_ip,''), created_at
        FROM %s WHERE api_key_hash = $1`, DevicesTable)
	var d Device
	err := s.op().QueryRow(ctx, q, digest).Scan(
		&d.ID, &d.Serial, &d.Label, &d.AssetTag,
		&d.HasAPIKey, &d.IsActive, &d.LastSeen, &d.LastIP, &d.CreatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrInvalidAPIKey
	}
	if err != nil {
		return nil, err
	}
	if !d.IsActive {
		// Not cached: an inactive device that gets reactivated should start
		// working on its next request, not at the end of a TTL.
		return nil, ErrInactiveDevice
	}
	s.authCache.Store(digest, cachedDevice{device: d, at: time.Now()})
	return &d, nil
}

// RotateAPIKey issues a fresh plaintext key and updates the stored digest.
// The plaintext is shown to the caller exactly once.
func (s *Store) RotateAPIKey(ctx context.Context, id int64) (string, error) {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	plaintext := base64.RawURLEncoding.EncodeToString(buf)
	tag, err := s.op().Exec(ctx,
		fmt.Sprintf(`UPDATE %s SET api_key_hash = $1 WHERE id = $2`, DevicesTable),
		hashAPIKey(plaintext), id)
	if err != nil {
		return "", err
	}
	if tag.RowsAffected() == 0 {
		return "", ErrDeviceNotFound
	}
	// The old key must stop working now, not at the end of deviceAuthTTL —
	// rotation is usually a response to a leak, and a 30-second grace period
	// for the leaked key is exactly what rotation exists to prevent.
	s.InvalidateDeviceAuth()
	return plaintext, nil
}

// MarkSeenInterval is how stale a device's last_seen is allowed to get.
//
// This UPDATE used to run on every ingest request. A relay posting every few
// seconds rewrote its own registry row constantly, for a column whose only job
// is answering "when did we last hear from this unit" — dead-tuple production
// on a small, frequently-read table, on the shared platform Postgres.
const MarkSeenInterval = time.Minute

// MarkSeen records that a device is alive, at most once per MarkSeenInterval.
// The throttle is in SQL rather than in process memory so it holds across
// restarts and across replicas.
func (s *Store) MarkSeen(ctx context.Context, deviceID int64, ip string) error {
	_, err := s.op().Exec(ctx, fmt.Sprintf(`
		UPDATE %s
		   SET last_seen = NOW(), last_ip = NULLIF($2, '')
		 WHERE id = $1
		   AND (last_seen IS NULL
		        OR last_seen < NOW() - $3::interval
		        OR last_ip IS DISTINCT FROM NULLIF($2, ''))`, DevicesTable),
		deviceID, ip, MarkSeenInterval.String(),
	)
	return err
}

// ─────────────────────────────── Readings ───────────────────────────────

// InsertReadings persists a batch. Duplicate (asset_tag, ts) rows are skipped
// by the ON CONFLICT clause.
func (s *Store) InsertReadings(ctx context.Context, readings []Reading) (int, error) {
	if len(readings) == 0 {
		return 0, nil
	}
	batch := &pgx.Batch{}
	for _, r := range readings {
		raw := r.Raw
		if len(raw) == 0 {
			raw = json.RawMessage(`{}`)
		}
		state := NormalizeState(r.State)
		batch.Queue(sqlInsertReading,
			r.AssetTag, r.DeviceID, r.TS, state, r.SpindleRPM, r.FeedRate,
			r.TemperatureC, r.VibrationMmS, r.PressureBar, r.PowerKW,
			r.GoodCount, r.RejectCount, r.CycleCount, r.FaultCode, raw,
		)
	}
	br := s.tel().SendBatch(ctx, batch)
	defer br.Close()
	inserted := 0
	for range readings {
		tag, err := br.Exec()
		if err != nil {
			return inserted, fmt.Errorf("insert reading: %w", err)
		}
		inserted += int(tag.RowsAffected())
	}
	return inserted, nil
}

// LatestReading returns the most recent reading for a machine, or nil if none.
func (s *Store) LatestReading(ctx context.Context, assetTag string) (*Reading, error) {
	row := s.tel().QueryRow(ctx, sqlLatestReading, assetTag)
	r, err := scanReading(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return r, err
}

// Track returns readings for a machine in [from, to], oldest first, capped at
// limit (default 5000). When after is non-nil, only readings with ts > after
// are returned (cursor pagination).
func (s *Store) Track(ctx context.Context, assetTag string, from, to time.Time, limit int, after *time.Time) ([]Reading, error) {
	maxRows := MaxTrackRowLimit()
	if limit <= 0 {
		limit = 5000
	}
	if limit > maxRows {
		limit = maxRows
	}
	var rows pgx.Rows
	var err error
	if after != nil && !after.IsZero() {
		rows, err = s.tel().Query(ctx, sqlTrackReadingsAfter, assetTag, *after, from, to, limit)
	} else {
		rows, err = s.tel().Query(ctx, sqlTrackReadings, assetTag, from, to, limit)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Reading
	for rows.Next() {
		r, err := scanReading(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *r)
	}
	return out, rows.Err()
}

// ReadingsForDay loads every reading for one (machine, day) UTC, oldest first.
// Used by cmd/aggregate.
func (s *Store) ReadingsForDay(ctx context.Context, assetTag string, day time.Time) ([]Reading, error) {
	start := day.UTC().Truncate(24 * time.Hour)
	end := start.Add(24 * time.Hour)
	rows, err := s.tel().Query(ctx, sqlReadingsForDay, assetTag, start, end)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Reading
	for rows.Next() {
		r, err := scanReading(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *r)
	}
	return out, rows.Err()
}

// AssetDay is a (asset_tag, day) pair with at least one reading.
type AssetDay struct {
	AssetTag string
	Day      time.Time
}

// DistinctAssetDays returns every (asset_tag, day) pair that has at least
// one reading in [from, to). Days are returned at UTC midnight. Drives the
// aggregator without scanning machines that have no telemetry.
func (s *Store) DistinctAssetDays(ctx context.Context, from, to time.Time) ([]AssetDay, error) {
	rows, err := s.tel().Query(ctx, sqlDistinctAssetDays, from, to)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []AssetDay
	for rows.Next() {
		var v AssetDay
		if err := rows.Scan(&v.AssetTag, &v.Day); err != nil {
			return nil, err
		}
		v.Day = v.Day.UTC()
		out = append(out, v)
	}
	return out, rows.Err()
}

// UpsertDaily inserts or replaces one mes_machine_oee_daily row. Idempotent.
func (s *Store) UpsertDaily(ctx context.Context, sum DailySummary) error {
	_, err := s.tel().Exec(ctx, sqlUpsertDaily,
		sum.AssetTag, sum.Day, sum.ReadingCount, sum.RunningMinutes, sum.IdleMinutes,
		sum.DownMinutes, sum.SetupMinutes, sum.GoodTotal, sum.RejectTotal, sum.CyclesTotal,
		sum.Availability, sum.Performance, sum.Quality, sum.OEE,
		sum.MaxTemperatureC, sum.AvgSpindleRPM, sum.FirstReading, sum.LastReading,
	)
	return err
}

// ListDailySummaries returns mes_machine_oee_daily rows for one machine in
// [from, to] (UTC dates).
func (s *Store) ListDailySummaries(ctx context.Context, assetTag string, from, to time.Time) ([]DailySummary, error) {
	rows, err := s.tel().Query(ctx, sqlListDaily, assetTag, from.UTC(), to.UTC())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []DailySummary
	for rows.Next() {
		var sum DailySummary
		if err := rows.Scan(
			&sum.AssetTag, &sum.Day, &sum.ReadingCount, &sum.RunningMinutes, &sum.IdleMinutes,
			&sum.DownMinutes, &sum.SetupMinutes, &sum.GoodTotal, &sum.RejectTotal, &sum.CyclesTotal,
			&sum.Availability, &sum.Performance, &sum.Quality, &sum.OEE,
			&sum.MaxTemperatureC, &sum.AvgSpindleRPM, &sum.FirstReading, &sum.LastReading,
		); err != nil {
			return nil, err
		}
		out = append(out, sum)
	}
	return out, rows.Err()
}

// InsertDowntimeEvents writes auto-detected stoppages into the canonical CMMS
// log (mes_downtime_events, category='auto'). The partial unique index makes
// aggregator re-runs idempotent. duration/confidence/fault ride in attrs.
// Targets the operational pool — downtime is CMMS data, not timeseries.
func (s *Store) InsertDowntimeEvents(ctx context.Context, events []DowntimeEvent) (int, error) {
	if len(events) == 0 {
		return 0, nil
	}
	tx, err := s.op().Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)

	written := 0
	for _, ev := range events {
		attrs, _ := json.Marshal(map[string]any{
			"duration_min": ev.DurationMin,
			"confidence":   ev.Confidence,
			"fault_code":   ev.FaultCode,
			"source":       "machine-telemetry",
		})
		reason := ev.Reason
		if reason == "" {
			reason = "unplanned stop"
		}
		tag, err := tx.Exec(ctx, sqlInsertDowntime,
			ev.AssetTag, reason, ev.StartedAt, ev.EndedAt, attrs,
		)
		if err != nil {
			return written, err
		}
		if tag.RowsAffected() == 0 {
			// ON CONFLICT DO NOTHING: the aggregator has seen this stoppage
			// before. Re-announcing it would reopen and reclose an interval
			// in iag-production on every re-run, so a row that was not
			// written gets no event.
			continue
		}
		written++

		// Announce the stoppage as a start and, when it is already over, an
		// end. The aggregator detects stoppages after the fact, so most
		// carry both: iag-production's MirrorAssetDowntime opens the interval
		// at one timestamp and closes it at the other, reproducing the stop
		// rather than leaving a machine down for ever.
		if err := enqueueDowntime(ctx, tx, EventDowntimeStarted, ev.AssetTag, reason, ev.StartedAt); err != nil {
			return written, err
		}
		if ev.EndedAt != nil {
			if err := enqueueDowntime(ctx, tx, EventDowntimeEnded, ev.AssetTag, reason, *ev.EndedAt); err != nil {
				return written, err
			}
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return written, err
	}
	return written, nil
}

// enqueueDowntime writes one mes.downtime.* event to the MES outbox, in the
// caller's transaction, with the payload shape iag-mes itself publishes —
// asset_tag, category, reason and an RFC3339 timestamp — so iag-production's
// consumer needs no special case for events that came from a device.
func enqueueDowntime(ctx context.Context, tx pgx.Tx, eventType, assetTag, reason string, at time.Time) error {
	payload, err := json.Marshal(map[string]any{
		"asset_tag": assetTag,
		"category":  "auto",
		"reason":    reason,
		"timestamp": at.UTC().Format(time.RFC3339),
	})
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, sqlEnqueueEvent, TopicOperations, eventType, assetTag, payload)
	return err
}

// PurgeBefore drops readings older than the cutoff. Called by cmd/aggregate -purge.
func (s *Store) PurgeBefore(ctx context.Context, cutoff time.Time) (int64, error) {
	tag, err := s.tel().Exec(ctx, sqlPurgeBefore, cutoff)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

// ──────────────────────────────── helpers ────────────────────────────────

func hashAPIKey(plaintext string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(plaintext)))
	return hex.EncodeToString(sum[:])
}

type rowScanner interface {
	Scan(...any) error
}

func scanReading(row rowScanner) (*Reading, error) {
	var r Reading
	var raw []byte
	err := row.Scan(
		&r.AssetTag, &r.DeviceID, &r.TS, &r.State, &r.SpindleRPM, &r.FeedRate,
		&r.TemperatureC, &r.VibrationMmS, &r.PressureBar, &r.PowerKW,
		&r.GoodCount, &r.RejectCount, &r.CycleCount, &r.FaultCode, &raw,
	)
	if err != nil {
		return nil, err
	}
	r.Raw = json.RawMessage(raw)
	return &r, nil
}
