-- Sink schema for the Berlin transit delay pipeline.
--
-- Every table the streaming job writes is keyed so that writing the same
-- logical row twice is a no-op in value terms. That is what makes replay
-- from offset 0 produce byte-identical output: the job computes the same
-- aggregate for the same window key and upserts it over itself.

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ---------------------------------------------------------------------------
-- Dimension tables, loaded from GTFS Schedule by tools/gtfs_loader.py.
-- Only stops.txt and routes.txt are imported. stop_times.txt for the whole
-- VBB network is tens of millions of rows and does not belong on a laptop;
-- see docs/architecture.md "Why delay comes from the feed".
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS gtfs_stops (
    stop_id        TEXT PRIMARY KEY,
    stop_name      TEXT NOT NULL,
    parent_station TEXT,
    stop_lat       DOUBLE PRECISION,
    stop_lon       DOUBLE PRECISION,
    loaded_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS gtfs_routes (
    route_id         TEXT PRIMARY KEY,
    route_short_name TEXT NOT NULL,
    route_long_name  TEXT,
    route_type       INTEGER,
    loaded_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------------
-- Tumbling 1 hour aggregate: delay by route, stop and hour on event time.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS delay_tumbling_1h (
    window_start      TIMESTAMPTZ NOT NULL,
    window_end        TIMESTAMPTZ NOT NULL,
    route_id          TEXT        NOT NULL,
    stop_id           TEXT        NOT NULL,
    route_short_name  TEXT        NOT NULL,
    stop_name         TEXT        NOT NULL,
    event_count       BIGINT      NOT NULL,
    sum_delay_seconds BIGINT      NOT NULL,
    min_delay_seconds INTEGER     NOT NULL,
    max_delay_seconds INTEGER     NOT NULL,
    -- Bucket counts instead of percentiles. An exact streaming p99 needs a
    -- sketch and a merge function; buckets are exact, cheap and deterministic.
    early_count       BIGINT      NOT NULL,  -- delay <= -60s
    on_time_count     BIGINT      NOT NULL,  -- -60s < delay <= 120s
    late_count        BIGINT      NOT NULL,  -- 120s < delay <= 300s
    very_late_count   BIGINT      NOT NULL,  -- delay > 300s
    avg_delay_seconds DOUBLE PRECISION
        GENERATED ALWAYS AS (sum_delay_seconds::DOUBLE PRECISION
                             / NULLIF(event_count, 0)) STORED,
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (window_start, route_id, stop_id)
);

SELECT create_hypertable('delay_tumbling_1h', 'window_start',
                         chunk_time_interval => INTERVAL '1 day',
                         if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS delay_tumbling_1h_route_idx
    ON delay_tumbling_1h (route_short_name, window_start DESC);

-- ---------------------------------------------------------------------------
-- Sliding 1 hour aggregate, 5 minute slide. Same key shape, same inputs.
-- Side by side with the tumbling table it shows what the window type changes:
-- 12 overlapping rows per hour instead of 1, each a smoother reading.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS delay_sliding_1h_5m (
    window_start      TIMESTAMPTZ NOT NULL,
    window_end        TIMESTAMPTZ NOT NULL,
    route_id          TEXT        NOT NULL,
    stop_id           TEXT        NOT NULL,
    route_short_name  TEXT        NOT NULL,
    stop_name         TEXT        NOT NULL,
    event_count       BIGINT      NOT NULL,
    sum_delay_seconds BIGINT      NOT NULL,
    min_delay_seconds INTEGER     NOT NULL,
    max_delay_seconds INTEGER     NOT NULL,
    early_count       BIGINT      NOT NULL,
    on_time_count     BIGINT      NOT NULL,
    late_count        BIGINT      NOT NULL,
    very_late_count   BIGINT      NOT NULL,
    avg_delay_seconds DOUBLE PRECISION
        GENERATED ALWAYS AS (sum_delay_seconds::DOUBLE PRECISION
                             / NULLIF(event_count, 0)) STORED,
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (window_start, route_id, stop_id)
);

SELECT create_hypertable('delay_sliding_1h_5m', 'window_start',
                         chunk_time_interval => INTERVAL '1 day',
                         if_not_exists => TRUE);

-- ---------------------------------------------------------------------------
-- Late records. Events that arrive after the watermark has passed their
-- window end are not dropped: the job routes them to a side output and they
-- land here, so the late rate is a measured number and not a guess.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS late_events (
    trip_id               TEXT        NOT NULL,
    start_date            TEXT        NOT NULL,
    stop_id               TEXT        NOT NULL,
    stop_sequence         INTEGER     NOT NULL,
    event_type            SMALLINT    NOT NULL,
    route_id              TEXT        NOT NULL,
    observed_time         TIMESTAMPTZ NOT NULL,
    delay_seconds         INTEGER,
    feed_timestamp        TIMESTAMPTZ NOT NULL,
    lateness_seconds      BIGINT      NOT NULL,
    recorded_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (observed_time, trip_id, start_date, stop_sequence, event_type)
);

SELECT create_hypertable('late_events', 'observed_time',
                         chunk_time_interval => INTERVAL '1 day',
                         if_not_exists => TRUE);

-- ---------------------------------------------------------------------------
-- Operational series. Written by the sink (latency) and by tools/lag_probe.py
-- (consumer lag). Grafana reads both from here, which keeps Prometheus out of
-- the compose file and ~300 MB of RAM free.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS e2e_latency_samples (
    sampled_at  TIMESTAMPTZ NOT NULL,
    latency_ms  BIGINT      NOT NULL,
    stage       TEXT        NOT NULL
);

SELECT create_hypertable('e2e_latency_samples', 'sampled_at',
                         chunk_time_interval => INTERVAL '1 hour',
                         if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS consumer_lag_samples (
    sampled_at    TIMESTAMPTZ NOT NULL,
    consumer_group TEXT       NOT NULL,
    topic         TEXT        NOT NULL,
    partition     INTEGER     NOT NULL,
    lag           BIGINT      NOT NULL,
    high_watermark BIGINT     NOT NULL
);

SELECT create_hypertable('consumer_lag_samples', 'sampled_at',
                         chunk_time_interval => INTERVAL '1 hour',
                         if_not_exists => TRUE);

-- ---------------------------------------------------------------------------
-- Replay verification. tools/verify_replay.py snapshots a content checksum of
-- the aggregate tables here before and after a replay and compares the two.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS replay_checksums (
    label       TEXT        NOT NULL,
    table_name  TEXT        NOT NULL,
    row_count   BIGINT      NOT NULL,
    checksum    TEXT        NOT NULL,
    taken_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (label, table_name)
);
