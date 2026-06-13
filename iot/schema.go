package iot

// ReadingsTable is the TimescaleDB hypertable for raw machine telemetry
// (state, OEE counters, process metrics). Partitioned on ts; no synthetic id
// column. Owned/migrated by the production + MES domain services; this service
// only writes to and reads from it.
const ReadingsTable = "machine_telemetry_timeseries"

// DailyTable is the per-machine, per-day OEE rollup produced by cmd/aggregate.
const DailyTable = "machine_telemetry_daily"

// DowntimeTable holds auto-detected stoppages.
const DowntimeTable = "machine_downtime_events"

// DevicesTable is the operational device registry (serial → machine binding,
// hashed API key). Lives on the registry/operational DB, not the timeseries DB.
const DevicesTable = "machine_iot_devices"
