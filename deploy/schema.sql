-- Reference DDL for the machine-telemetry tables. This service does NOT run
-- migrations — the iag-production (and/or iag-mes) domain service owns schema
-- ownership. Copy these statements into a production/MES migration so reads,
-- the device registry UI, and the timeseries stay in one migration history.
--
-- Requires the TimescaleDB extension for the readings hypertable.

-- ── Operational / registry DB ────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS machine_iot_devices (
    id           BIGGENERATED_PLACEHOLDER,
    serial       TEXT NOT NULL UNIQUE,
    label        TEXT,
    machine_id   TEXT,
    api_key_hash TEXT UNIQUE,
    is_active    BOOLEAN NOT NULL DEFAULT TRUE,
    last_seen    TIMESTAMPTZ,
    last_ip      TEXT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
-- NB: replace BIGGENERATED_PLACEHOLDER with `BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY`
--     (kept as a placeholder so this file is documentation, not executed verbatim).
CREATE INDEX IF NOT EXISTS machine_iot_devices_machine_idx ON machine_iot_devices (machine_id);

-- Hot-state columns expected on the existing `machines` table (ApplyMachineHotState):
ALTER TABLE machines ADD COLUMN IF NOT EXISTS last_state        TEXT;
ALTER TABLE machines ADD COLUMN IF NOT EXISTS last_seen_at      TIMESTAMPTZ;
ALTER TABLE machines ADD COLUMN IF NOT EXISTS last_good_count   BIGINT;
ALTER TABLE machines ADD COLUMN IF NOT EXISTS last_reject_count BIGINT;

-- ── Timeseries DB (TimescaleDB) ──────────────────────────────────────────

CREATE TABLE IF NOT EXISTS machine_telemetry_timeseries (
    machine_id     TEXT        NOT NULL,
    device_id      BIGINT,
    ts             TIMESTAMPTZ NOT NULL,
    state          TEXT        NOT NULL DEFAULT 'unknown',
    spindle_rpm    DOUBLE PRECISION,
    feed_rate      DOUBLE PRECISION,
    temperature_c  DOUBLE PRECISION,
    vibration_mm_s DOUBLE PRECISION,
    pressure_bar   DOUBLE PRECISION,
    power_kw       DOUBLE PRECISION,
    good_count     BIGINT,
    reject_count   BIGINT,
    cycle_count    BIGINT,
    fault_code     TEXT,
    raw            JSONB NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (machine_id, ts)
);
SELECT create_hypertable('machine_telemetry_timeseries', 'ts', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS machine_ts_state_idx ON machine_telemetry_timeseries (machine_id, state, ts DESC);

CREATE TABLE IF NOT EXISTS machine_telemetry_daily (
    machine_id        TEXT NOT NULL,
    day               DATE NOT NULL,
    reading_count     INT  NOT NULL DEFAULT 0,
    running_minutes   INT  NOT NULL DEFAULT 0,
    idle_minutes      INT  NOT NULL DEFAULT 0,
    down_minutes      INT  NOT NULL DEFAULT 0,
    setup_minutes     INT  NOT NULL DEFAULT 0,
    good_total        BIGINT NOT NULL DEFAULT 0,
    reject_total      BIGINT NOT NULL DEFAULT 0,
    cycles_total      BIGINT NOT NULL DEFAULT 0,
    availability      DOUBLE PRECISION,
    performance       DOUBLE PRECISION,
    quality           DOUBLE PRECISION,
    oee               DOUBLE PRECISION,
    max_temperature_c DOUBLE PRECISION,
    avg_spindle_rpm   DOUBLE PRECISION,
    first_reading     TIMESTAMPTZ,
    last_reading      TIMESTAMPTZ,
    PRIMARY KEY (machine_id, day)
);

CREATE TABLE IF NOT EXISTS machine_downtime_events (
    id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    machine_id   TEXT NOT NULL,
    started_at   TIMESTAMPTZ NOT NULL,
    ended_at     TIMESTAMPTZ,
    duration_min DOUBLE PRECISION NOT NULL DEFAULT 0,
    fault_code   TEXT,
    reason       TEXT,
    confidence   TEXT NOT NULL DEFAULT 'low',
    notes        TEXT,
    UNIQUE (machine_id, started_at)
);
