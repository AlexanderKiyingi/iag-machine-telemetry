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
	Serial    string
	Label     string
	MachineID string
	IssueKey  bool // when true, a fresh API key is generated; the plaintext is returned once
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
        INSERT INTO %s (serial, label, machine_id, api_key_hash)
        VALUES ($1, NULLIF($2, ''), NULLIF($3, ''), NULLIF($4, ''))
        RETURNING id, serial, COALESCE(label,''), COALESCE(machine_id,''),
                  api_key_hash IS NOT NULL, is_active, last_seen, COALESCE(last_ip,''), created_at`, DevicesTable)
	var d Device
	err := s.op().QueryRow(ctx, q, in.Serial, in.Label, in.MachineID, keyHash).Scan(
		&d.ID, &d.Serial, &d.Label, &d.MachineID,
		&d.HasAPIKey, &d.IsActive, &d.LastSeen, &d.LastIP, &d.CreatedAt,
	)
	if err != nil {
		return nil, err
	}
	return &CreatedDevice{Device: d, APIKeyPlaintext: plaintext}, nil
}

func (s *Store) ListDevices(ctx context.Context) ([]Device, error) {
	q := fmt.Sprintf(`
        SELECT id, serial, COALESCE(label,''), COALESCE(machine_id,''),
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
			&d.ID, &d.Serial, &d.Label, &d.MachineID,
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
        SELECT id, serial, COALESCE(label,''), COALESCE(machine_id,''),
               api_key_hash IS NOT NULL, is_active, last_seen, COALESCE(last_ip,''), created_at
        FROM %s WHERE serial = $1`, DevicesTable)
	var d Device
	err := s.op().QueryRow(ctx, q, serial).Scan(
		&d.ID, &d.Serial, &d.Label, &d.MachineID,
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
func (s *Store) AuthenticateAPIKey(ctx context.Context, plaintext string) (*Device, error) {
	if plaintext == "" {
		return nil, ErrInvalidAPIKey
	}
	digest := hashAPIKey(plaintext)
	q := fmt.Sprintf(`
        SELECT id, serial, COALESCE(label,''), COALESCE(machine_id,''),
               api_key_hash IS NOT NULL, is_active, last_seen, COALESCE(last_ip,''), created_at
        FROM %s WHERE api_key_hash = $1`, DevicesTable)
	var d Device
	err := s.op().QueryRow(ctx, q, digest).Scan(
		&d.ID, &d.Serial, &d.Label, &d.MachineID,
		&d.HasAPIKey, &d.IsActive, &d.LastSeen, &d.LastIP, &d.CreatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrInvalidAPIKey
	}
	if err != nil {
		return nil, err
	}
	if !d.IsActive {
		return nil, ErrInactiveDevice
	}
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
	return plaintext, nil
}

func (s *Store) MarkSeen(ctx context.Context, deviceID int64, ip string) error {
	_, err := s.op().Exec(ctx,
		fmt.Sprintf(`UPDATE %s SET last_seen = NOW(), last_ip = NULLIF($2, '') WHERE id = $1`, DevicesTable),
		deviceID, ip,
	)
	return err
}

// ─────────────────────────────── Readings ───────────────────────────────

// InsertReadings persists a batch. Duplicate (machine_id, ts) rows are skipped
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
			r.MachineID, r.DeviceID, r.TS, state, r.SpindleRPM, r.FeedRate,
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
func (s *Store) LatestReading(ctx context.Context, machineID string) (*Reading, error) {
	row := s.tel().QueryRow(ctx, sqlLatestReading, machineID)
	r, err := scanReading(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return r, err
}

// Track returns readings for a machine in [from, to], oldest first, capped at
// limit (default 5000). When after is non-nil, only readings with ts > after
// are returned (cursor pagination).
func (s *Store) Track(ctx context.Context, machineID string, from, to time.Time, limit int, after *time.Time) ([]Reading, error) {
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
		rows, err = s.tel().Query(ctx, sqlTrackReadingsAfter, machineID, *after, from, to, limit)
	} else {
		rows, err = s.tel().Query(ctx, sqlTrackReadings, machineID, from, to, limit)
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
func (s *Store) ReadingsForDay(ctx context.Context, machineID string, day time.Time) ([]Reading, error) {
	start := day.UTC().Truncate(24 * time.Hour)
	end := start.Add(24 * time.Hour)
	rows, err := s.tel().Query(ctx, sqlReadingsForDay, machineID, start, end)
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

// MachineDay is a (machine_id, day) pair with at least one reading.
type MachineDay struct {
	MachineID string
	Day       time.Time
}

// DistinctMachineDays returns every (machine_id, day) pair that has at least
// one reading in [from, to). Days are returned at UTC midnight. Drives the
// aggregator without scanning machines that have no telemetry.
func (s *Store) DistinctMachineDays(ctx context.Context, from, to time.Time) ([]MachineDay, error) {
	rows, err := s.tel().Query(ctx, sqlDistinctMachineDays, from, to)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []MachineDay
	for rows.Next() {
		var v MachineDay
		if err := rows.Scan(&v.MachineID, &v.Day); err != nil {
			return nil, err
		}
		v.Day = v.Day.UTC()
		out = append(out, v)
	}
	return out, rows.Err()
}

// UpsertDaily inserts or replaces one machine_telemetry_daily row. Idempotent.
func (s *Store) UpsertDaily(ctx context.Context, sum DailySummary) error {
	_, err := s.tel().Exec(ctx, sqlUpsertDaily,
		sum.MachineID, sum.Day, sum.ReadingCount, sum.RunningMinutes, sum.IdleMinutes,
		sum.DownMinutes, sum.SetupMinutes, sum.GoodTotal, sum.RejectTotal, sum.CyclesTotal,
		sum.Availability, sum.Performance, sum.Quality, sum.OEE,
		sum.MaxTemperatureC, sum.AvgSpindleRPM, sum.FirstReading, sum.LastReading,
	)
	return err
}

// ListDailySummaries returns machine_telemetry_daily rows for one machine in
// [from, to] (UTC dates).
func (s *Store) ListDailySummaries(ctx context.Context, machineID string, from, to time.Time) ([]DailySummary, error) {
	rows, err := s.tel().Query(ctx, sqlListDaily, machineID, from.UTC(), to.UTC())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []DailySummary
	for rows.Next() {
		var sum DailySummary
		if err := rows.Scan(
			&sum.MachineID, &sum.Day, &sum.ReadingCount, &sum.RunningMinutes, &sum.IdleMinutes,
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

// InsertDowntimeEvents persists a batch. Conflicts on (machine_id, started_at)
// are ignored so re-running the aggregator over the same day is idempotent.
func (s *Store) InsertDowntimeEvents(ctx context.Context, events []DowntimeEvent) (int, error) {
	if len(events) == 0 {
		return 0, nil
	}
	tx, err := s.tel().Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)

	written := 0
	for _, ev := range events {
		tag, err := tx.Exec(ctx, sqlInsertDowntime,
			ev.MachineID, ev.StartedAt, ev.EndedAt, ev.DurationMin,
			ev.FaultCode, ev.Reason, ev.Confidence, ev.Notes,
		)
		if err != nil {
			return written, err
		}
		written += int(tag.RowsAffected())
	}
	if err := tx.Commit(ctx); err != nil {
		return written, err
	}
	return written, nil
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
		&r.MachineID, &r.DeviceID, &r.TS, &r.State, &r.SpindleRPM, &r.FeedRate,
		&r.TemperatureC, &r.VibrationMmS, &r.PressureBar, &r.PowerKW,
		&r.GoodCount, &r.RejectCount, &r.CycleCount, &r.FaultCode, &raw,
	)
	if err != nil {
		return nil, err
	}
	r.Raw = json.RawMessage(raw)
	return &r, nil
}
