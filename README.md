# machine-telemetry

Production-machine IoT ingest for the IAG platform. **iag-production** and
**iag-mes** own business APIs, the device-registry UI, schema migrations, and
reads; **machine-telemetry** owns high-throughput ingest into the
**`machine_telemetry_timeseries`** TimescaleDB hypertable, the daily OEE rollup,
and downtime detection.

Mirrors the split that `Fleet_IoT` uses for vehicles — this is the
manufacturing-floor equivalent (state, OEE counters, process metrics instead of
GPS/fuel).

## Architecture

```text
Machines / PLC relays
    ├─ TCP MTP (:5037)   → cmd/gateway   (binary, length-prefixed + CRC)
    └─ HTTP JSON (:4090) → cmd/ingest    (Bearer device API key)
              │
              ▼
    machine_telemetry_timeseries  (TimescaleDB hypertable on ts)
              │
              ├─ cmd/aggregate → machine_telemetry_daily (OEE) + machine_downtime_events
              └─ Redis pub/sub (optional) → production/MES SSE live floor view
```

| Component | Repo | Responsibility |
|-----------|------|----------------|
| **machine-telemetry** | this repo (`edge/machine-telemetry`) | Ingest, OEE rollup, downtime detection |
| **iag-production** | `services/operations/production` | Production runs/orders, reads, device admin, migrations |
| **iag-mes** | `services/operations/mes` | CMMS/maintenance, downtime + fault pareto reads |

Go module path: `github.com/iag/machine-telemetry`. GitHub / folder name: **machine-telemetry**.

## Binaries

| Binary | Default | Description |
|--------|---------|-------------|
| `/app/ingest` | `:4090` | `POST /v1/readings`, `POST /api/iot/readings` |
| `/app/gateway` | `:5037` | MTP (Machine Telemetry Protocol) binary TCP |
| `/app/aggregate` | job | Daily OEE rollup + downtime detection (+ optional purge) |

## Reading shape (HTTP)

```json
POST /v1/readings   Authorization: Bearer <device-api-key>
{
  "machineId": "MCH-001",
  "ts": "2026-06-13T08:30:00Z",
  "state": "running",
  "spindleRpm": 12000,
  "temperatureC": 64.5,
  "vibrationMmS": 2.75,
  "powerKw": 7.4,
  "goodCount": 980,
  "rejectCount": 20,
  "cycleCount": 1000,
  "faultCode": null
}
```
A single object or an array (batch ≤ 1000) is accepted. `goodCount`/`rejectCount`/
`cycleCount` are **cumulative lifetime counters** — the aggregator differences
them per day (and tolerates counter resets).

## MTP — Machine Telemetry Protocol

Compact big-endian binary framing for relays that can't speak HTTP, modeled on
the Teltonika Codec 8 wire shape (length-prefixed serial handshake,
preamble+length+CRC-16/IBM data packet, int32 ACK). One fixed 39-byte record
carries ts, state, fault code, spindle RPM, good/reject/cycle counters, and
scaled temperature/vibration/power. See [`iot/mtp.go`](iot/mtp.go).

## OEE & downtime

`cmd/aggregate` computes, per machine per UTC day:
- **Availability** = running ÷ (running + down + setup)
- **Quality** = good ÷ (good + reject)
- **Performance** = cycles ÷ (`IDEAL_CYCLES_PER_MIN` × running min) — only when a
  nameplate rate is configured; otherwise Performance and OEE are left null
  rather than invented.
- **OEE** = Availability × Performance × Quality (when all three are present).

Contiguous `down` reading runs become `machine_downtime_events` for MES's
unplanned-downtime / fault-pareto views.

## Schema ownership

This service runs **no migrations**. Its tables are created by **MES** migration
`006_machine_telemetry.sql` (schema `mes`) as a **parallel subsystem** keyed on
`machine_id` — intentionally separate from MES's existing `mes_assets` /
`mes_asset_telemetry_latest` / `mes_downtime_events` (keyed on `asset_tag`),
**to be reconciled later**. `deploy/schema.sql` mirrors that DDL for reference.
The service DSN must put `mes` on the `search_path` (see `config/.env.example`).
Requires the TimescaleDB extension on the hypertable.

## Monorepo wiring

```go
// services/operations/production/go.mod
require github.com/iag/machine-telemetry v0.0.0
replace github.com/iag/machine-telemetry => ../../../edge/machine-telemetry
```

## Environment

See [`config/.env.example`](config/.env.example). Key vars: `DATABASE_URL`
(timeseries), `REGISTRY_DATABASE_URL` (split-DB registry/hot-state), `ADDR` /
`IOT_ADDR`, `REDIS_URL`, `IDEAL_CYCLES_PER_MIN`, `PURGE_DAYS`.

## Standalone remote

`https://github.com/AlexanderKiyingi/iag-machine-telemetry.git` — publish this
directory and depend on a tagged `github.com/iag/machine-telemetry` release from
production/MES. Registered in [`subrepos.json`](../../subrepos.json).
