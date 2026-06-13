package iot

// Tables are owned/migrated by MES (mes schema, 006_machine_telemetry.sql) and
// keyed on asset_tag — the platform-wide machine identity shared by mes_assets
// (canonical master), production, and the CMMS. This service only reads/writes.

// ReadingsTable is the high-throughput wide TimescaleDB hypertable for raw
// machine telemetry (state, OEE counters, process metrics).
const ReadingsTable = "mes_machine_telemetry"

// DailyTable is the per-asset, per-day OEE rollup produced by cmd/aggregate.
const DailyTable = "mes_machine_oee_daily"

// DowntimeTable is the canonical CMMS downtime log. Auto-detected stoppages are
// written here with category='auto' so maintenance sees them alongside manual
// downtime.
const DowntimeTable = "mes_downtime_events"

// DevicesTable is the operational IoT device registry (serial → asset binding,
// hashed API key).
const DevicesTable = "mes_iot_devices"

// AssetsTable is the canonical machine/asset master (hot-state target).
const AssetsTable = "mes_assets"

// LatestTable is the per-asset latest-metric store (EAV), updated on ingest.
const LatestTable = "mes_asset_telemetry_latest"
