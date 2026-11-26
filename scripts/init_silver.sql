-- DDL of the Silver schema (Postgres 5434, database streaming).
-- Idempotent: `make silver` can run as many times as needed and never deletes data.
-- ATTENTION: `IF NOT EXISTS` does NOT alter an existing table. A column change is `make silver-reset`;
-- after the restart test the schema is frozen (the test depends on the data).
--
-- Two kinds of time, on purpose (see docs/notes.md):
--   * event_time / pickup_at / dropoff_at  -> TIMESTAMP (naive, New York local time as TLC publishes)
--   * emit_time / ingested_at / updated_at -> TIMESTAMPTZ (wall-clock instant of producer/consumer)
-- lag_seconds = ingested_at - emit_time (pipeline latency), never ingested_at - event_time
-- (the latter would give ~2 years, because the replay uses 2024 data).
--
-- Everything in one transaction: if something fails halfway, no half-built schema is left behind.
BEGIN;

CREATE SCHEMA IF NOT EXISTS silver;

-- 1 row per trip_id. The PK is where the real dedup happens.
-- An invalid row ENTERS here with is_valid_trip = false (Silver policy, see docs/notes.md).
CREATE TABLE IF NOT EXISTS silver.trip_events (
    trip_id          TEXT PRIMARY KEY,
    pickup_at        TIMESTAMP      NOT NULL,
    dropoff_at       TIMESTAMP,
    pickup_zone_id   INTEGER,
    dropoff_zone_id  INTEGER,
    fare_amount      NUMERIC(10, 2),
    tip_amount       NUMERIC(10, 2),
    total_amount     NUMERIC(10, 2),
    payment_type     SMALLINT,
    is_valid_trip    BOOLEAN        NOT NULL DEFAULT TRUE,
    event_time       TIMESTAMP      NOT NULL,
    emit_time        TIMESTAMPTZ    NOT NULL,
    ingested_at      TIMESTAMPTZ    NOT NULL,
    lag_seconds      DOUBLE PRECISION
);

-- End-to-end reconciliation and the zone window both scan by pickup_at.
CREATE INDEX IF NOT EXISTS trip_events_pickup_at_idx
    ON silver.trip_events (pickup_at);
CREATE INDEX IF NOT EXISTS trip_events_zone_pickup_idx
    ON silver.trip_events (pickup_zone_id, pickup_at);

-- Zone x 5-minute tumbling window. Composite PK = the window upsert key.
-- The aggregate is recomputed from trip_events for the windows a batch touched (streaming/silver.py):
-- a replayed batch rewrites the same numbers instead of counting the events twice.
CREATE TABLE IF NOT EXISTS silver.zone_demand_5min (
    pickup_zone_id   INTEGER       NOT NULL,
    window_start     TIMESTAMP     NOT NULL,
    window_end       TIMESTAMP     NOT NULL,
    trips            INTEGER       NOT NULL DEFAULT 0,
    revenue          NUMERIC(12, 2) NOT NULL DEFAULT 0,
    avg_duration_min DOUBLE PRECISION,
    updated_at       TIMESTAMPTZ   NOT NULL DEFAULT now(),
    PRIMARY KEY (pickup_zone_id, window_start),
    CONSTRAINT zone_demand_5min_window_5min CHECK (window_end = window_start + interval '5 minutes')
);

-- Single row: watermark = the previous batch's watermark, the base of the late filter.
-- A NULL watermark means "first batch, accept everything" (NOT "discard everything").
CREATE TABLE IF NOT EXISTS silver.stream_state (
    id         SMALLINT    PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    watermark  TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Seed the single row so the job can always run UPDATE ... WHERE id = 1.
INSERT INTO silver.stream_state (id) VALUES (1)
ON CONFLICT (id) DO NOTHING;

-- Micro-batch ledger. The PK is the batch identity (offset ranges per partition), which repeats when
-- the checkpoint re-executes the batch: the INSERT ... ON CONFLICT DO NOTHING keeps the repetition
-- from counting twice. `data/state/counters.json` is the sum of this table, not of the process.
CREATE TABLE IF NOT EXISTS silver.batch_runs (
    batch_key          TEXT PRIMARY KEY,
    consumed           INTEGER     NOT NULL,
    valid_stored       INTEGER     NOT NULL,
    invalid            INTEGER     NOT NULL,
    late_dropped       INTEGER     NOT NULL,
    duplicates_dropped INTEGER     NOT NULL,
    rejected           INTEGER     NOT NULL,
    committed_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMIT;
