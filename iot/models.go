// Package iot owns machine-telemetry ingestion, rollups, and live streaming.
//
// Readings arrive two ways:
//   - HTTP bulk: POST /v1/readings (alias /api/iot/readings), authenticated via
//     the device's shared API key (Authorization: Bearer <plaintext>). The
//     plaintext is hashed (sha256) on creation and only the digest is persisted.
//   - MTP binary TCP: cmd/gateway accepts the Machine Telemetry Protocol
//     (see mtp.go), with the device identified by serial matching
//     mes_iot_devices.serial.
//
// This service owns high-throughput ingest into the mes_machine_telemetry
// TimescaleDB hypertable plus the daily OEE rollup and downtime detection. The
// iag-production and iag-mes domain services own business APIs, the device
// registry UI, schema migrations, and reads.
//
// Live updates fan out via an in-memory pubsub broker (one subscriber channel
// per active SSE client), or Redis pub/sub when REDIS_URL is set, so HTTP ingest
// and the TCP gateway can share a live feed across API replicas.
package iot

import (
	"encoding/json"
	"time"
)

// Machine states reported by a device. Stored as text in the timeseries so the
// production/MES dashboards can group on it directly.
const (
	StateUnknown = "unknown"
	StateRunning = "running"
	StateIdle    = "idle"
	StateDown    = "down"
	StateSetup   = "setup"
	StateBlocked = "blocked"
)

// stateByCode maps the MTP wire byte to a state string (see mtp.go).
var stateByCode = map[uint8]string{
	0: StateUnknown,
	1: StateRunning,
	2: StateIdle,
	3: StateDown,
	4: StateSetup,
	5: StateBlocked,
}

// StateForCode resolves an MTP state byte, defaulting to "unknown".
func StateForCode(code uint8) string {
	if s, ok := stateByCode[code]; ok {
		return s
	}
	return StateUnknown
}

// NormalizeState lower-cases and validates a state string, defaulting to
// "unknown" for anything unrecognized.
func NormalizeState(s string) string {
	switch s {
	case StateRunning, StateIdle, StateDown, StateSetup, StateBlocked, StateUnknown:
		return s
	default:
		return StateUnknown
	}
}

// Device is an IoT sensor/edge box attached to a production machine.
type Device struct {
	ID        int64      `json:"id"`
	Serial    string     `json:"serial"`
	Label     string     `json:"label,omitempty"`
	AssetTag  string     `json:"assetTag,omitempty"`
	HasAPIKey bool       `json:"hasApiKey"`
	IsActive  bool       `json:"isActive"`
	LastSeen  *time.Time `json:"lastSeen,omitempty"`
	LastIP    string     `json:"lastIp,omitempty"`
	CreatedAt time.Time  `json:"createdAt"`
}

// Reading is one timestamped sample from a machine, persisted to
// mes_machine_telemetry (Timescale hypertable). AssetTag and TS are
// required; every metric is best-effort and may be nil depending on the
// machine's sensor map. GoodCount/RejectCount/CycleCount are cumulative
// lifetime counters — the aggregator differences them to get per-day totals.
type Reading struct {
	AssetTag     string          `json:"assetTag"`
	DeviceID     *int64          `json:"deviceId,omitempty"`
	TS           time.Time       `json:"ts"`
	State        string          `json:"state"`
	SpindleRPM   *float64        `json:"spindleRpm,omitempty"`
	FeedRate     *float64        `json:"feedRate,omitempty"`
	TemperatureC *float64        `json:"temperatureC,omitempty"`
	VibrationMmS *float64        `json:"vibrationMmS,omitempty"`
	PressureBar  *float64        `json:"pressureBar,omitempty"`
	PowerKW      *float64        `json:"powerKw,omitempty"`
	GoodCount    *int64          `json:"goodCount,omitempty"`
	RejectCount  *int64          `json:"rejectCount,omitempty"`
	CycleCount   *int64          `json:"cycleCount,omitempty"`
	FaultCode    *string         `json:"faultCode,omitempty"`
	Raw          json.RawMessage `json:"raw,omitempty"`
}

// DowntimeEvent is an auto-detected stoppage, derived from a contiguous run of
// "down" readings, persisted to mes_downtime_events. Surfaced to MES as
// unplanned-downtime / fault-pareto input.
type DowntimeEvent struct {
	ID          int64      `json:"id,omitempty"`
	AssetTag    string     `json:"assetTag"`
	StartedAt   time.Time  `json:"startedAt"`
	EndedAt     *time.Time `json:"endedAt,omitempty"`
	DurationMin float64    `json:"durationMin"`
	FaultCode   string     `json:"faultCode,omitempty"`
	Reason      string     `json:"reason,omitempty"`
	Confidence  string     `json:"confidence"` // high | medium | low
	Notes       string     `json:"notes,omitempty"`
}

// DailyResult bundles what AggregateDay produces: the OEE summary row plus any
// downtime events detected over the day's readings.
type DailyResult struct {
	Summary        DailySummary    `json:"summary"`
	DowntimeEvents []DowntimeEvent `json:"downtimeEvents,omitempty"`
}

// DailySummary is one row of mes_machine_oee_daily — the per-machine,
// per-UTC-day OEE rollup.
type DailySummary struct {
	AssetTag        string     `json:"assetTag"`
	Day             time.Time  `json:"day"`
	ReadingCount    int        `json:"readingCount"`
	RunningMinutes  int        `json:"runningMinutes"`
	IdleMinutes     int        `json:"idleMinutes"`
	DownMinutes     int        `json:"downMinutes"`
	SetupMinutes    int        `json:"setupMinutes"`
	GoodTotal       int64      `json:"goodTotal"`
	RejectTotal     int64      `json:"rejectTotal"`
	CyclesTotal     int64      `json:"cyclesTotal"`
	Availability    *float64   `json:"availability,omitempty"` // 0..1
	Performance     *float64   `json:"performance,omitempty"`  // 0..1
	Quality         *float64   `json:"quality,omitempty"`      // 0..1
	OEE             *float64   `json:"oee,omitempty"`          // 0..1
	MaxTemperatureC *float64   `json:"maxTemperatureC,omitempty"`
	AvgSpindleRPM   *float64   `json:"avgSpindleRpm,omitempty"`
	FirstReading    *time.Time `json:"firstReading,omitempty"`
	LastReading     *time.Time `json:"lastReading,omitempty"`
}
