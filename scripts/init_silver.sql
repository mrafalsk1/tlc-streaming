-- DDL do schema Silver (Postgres 5434, banco streaming).
-- Idempotente: `make silver` pode rodar quantas vezes quiser, não apaga dado.
-- ATENÇÃO: `IF NOT EXISTS` NÃO altera tabela existente. Mudança de coluna é `make silver-reset`;
-- depois do restart test o schema é congelado (o teste depende do dado).
--
-- Dois tipos de tempo, de propósito (ver docs/notes.md):
--   * event_time / pickup_at / dropoff_at  -> TIMESTAMP (naive, hora local de NY como o TLC publica)
--   * emit_time / ingested_at / updated_at -> TIMESTAMPTZ (instante de parede do producer/consumer)
-- lag_seconds = ingested_at - emit_time (latência do pipeline), nunca ingested_at - event_time
-- (este último daria ~2 anos, porque o replay usa dados de 2024).
--
-- Tudo em uma transação: se algo falhar no meio, não fica schema pela metade.
BEGIN;

CREATE SCHEMA IF NOT EXISTS silver;

-- 1 linha por trip_id. A PK é onde a dedup de verdade acontece.
-- Linha inválida ENTRA aqui com is_valid_trip = false (política do Silver, ver docs/notes.md).
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

-- Consultas de reconciliação ponta a ponta e da janela por zona varrem por pickup_at.
CREATE INDEX IF NOT EXISTS trip_events_pickup_at_idx
    ON silver.trip_events (pickup_at);
CREATE INDEX IF NOT EXISTS trip_events_zone_pickup_idx
    ON silver.trip_events (pickup_zone_id, pickup_at);

-- Zona x janela tumbling de 5 min. PK composta = chave do upsert da janela.
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

-- Uma linha só: watermark = max(event_time) do lote anterior, base do filtro de atraso próprio.
-- watermark NULL = "primeiro lote, aceita tudo" (NÃO é "descarta tudo").
CREATE TABLE IF NOT EXISTS silver.stream_state (
    id         SMALLINT    PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    watermark  TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Semeia a linha única para o job poder sempre fazer UPDATE ... WHERE id = 1.
INSERT INTO silver.stream_state (id) VALUES (1)
ON CONFLICT (id) DO NOTHING;

COMMIT;
