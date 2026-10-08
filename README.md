# machine-telemetry

Production-machine IoT ingest for the IAG platform, keyed on **`asset_tag`** —
the platform-wide machine identity shared by **`mes_assets`** (the canonical
machine/asset master, owned by MES), production (`prod_*.asset_tag`), and the
CMMS. **iag-mes** and **iag-production** own business APIs, the device-registry
UI, schema migrations, and reads; **machine-telemetry** owns high-throughput
ingest into the **`mes_machine_telemetry`** TimescaleDB hypertable, the daily OEE
rollup, and downtime detection.

It does **not** create its own machine registry — hot-state folds into the
canonical `mes_assets` + `mes_asset_telemetry_latest`, and detected stoppages
into the existing `mes_downtime_events`. Mirrors the split that `Fleet_IoT` uses
for vehicles — the manufacturing-floor equivalent (state, OEE counters, process
metrics instead of GPS/fuel).

## Architecture

```text
Machines / PLC relays
    ├─ TCP MTP (:5037)   → cmd/gateway   (binary, length-prefixed + CRC)
    └─ HTTP JSON (:4090) → cmd/ingest    (Bearer device API key)
              │
              ▼
    mes_machine_telemetry  (TimescaleDB hypertable on ts, keyed on asset_tag)
              │
              ├─ ingest hot-state  → mes_assets.status + mes_asset_telemetry_latest
              ├─ cmd/aggregate     → mes_machine_oee_daily (OEE) + mes_downtime_events (category='auto')
              └─ Redis pub/sub (optional) → production/MES SSE live floor view
```

| Component | Repo | Responsibility |
|-----------|------|----------------|
| **machine-telemetry** | this repo (`edge/machine-telemetry`) | Ingest, OEE rollup, downtime detection |
| **iag-production** | `services/operations/production` | Production runs/orders, reads, device admin, migrations |
| **iag-mes** | `services/operations/mes` | CMMS/maintenance, downtime + fault pareto reads |

Go module path: `github.com/iag/machine-telemetry`. GitHub / folder name: **machine-telemetry**.

## Retention

`mes_machine_telemetry` is declared as a TimescaleDB hypertable and iag-mes
migration 008 asks for compression after 7 days and retention after 180.
**TimescaleDB is not installed on the production database**, so that migration
raises a notice and returns and neither policy exists. `cmd/aggregate` applies
the same 180 days itself (`PURGE_DAYS`, set to 0 to keep everything), which is
the only retention in force until the extension is installed.

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
  "assetTag": "MCH-001",
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

Contiguous `down` reading runs are written into the canonical
`mes_downtime_events` with `category='auto'` (duration/confidence/fault in
`attrs`), so they appear in MES's unplanned-downtime / fault-pareto views
alongside manually logged downtime. A partial unique index makes re-runs
idempotent.

## Schema ownership

This service runs **no migrations**. It speaks the shared `asset_tag` machine
identity and writes against the MES-owned schema:

| Table | Role | Owner |
|-------|------|-------|
| `mes_assets` | canonical machine/asset master (hot-state target) | MES (existing) |
| `mes_asset_telemetry_latest` | live per-metric values | MES (existing) |
| `mes_downtime_events` | CMMS downtime log (auto + manual) | MES (existing) |
| `mes_machine_telemetry` | high-throughput readings hypertable | **added** by `006_machine_telemetry.sql` |
| `mes_machine_oee_daily` | daily OEE rollup | **added** |
| `mes_iot_devices` | device registry (serial → asset binding) | **added** |

The added tables come from **MES** migration `006_machine_telemetry.sql` (schema
`mes`). The service DSN must put `mes` on the `search_path` (see
`config/.env.example`). Requires the TimescaleDB extension on the hypertable.

## Monorepo wiring

```go
// services/operations/mes/go.mod
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
