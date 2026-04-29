-- ClickHouse schema for cinema analytics
-- Note: The Kafka Engine table will connect to Kafka lazily (on first read),
-- so this script runs successfully even before Kafka is up.

-- Raw events table (MergeTree) — permanent storage
CREATE TABLE IF NOT EXISTS cinema.movie_events (
    event_id         String,
    user_id          String,
    movie_id         String,
    event_type       LowCardinality(String),
    event_timestamp  DateTime('UTC'),
    device_type      LowCardinality(String),
    session_id       String,
    progress_seconds Int32,
    _ingested_at     DateTime('UTC') DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_timestamp)
ORDER BY (event_type, user_id, event_timestamp)
SETTINGS index_granularity = 8192;

-- Kafka engine table — reads from topic (lazy connection)
CREATE TABLE IF NOT EXISTS cinema.movie_events_kafka (
    event_id         String,
    user_id          String,
    movie_id         String,
    event_type       String,
    event_timestamp  String,
    device_type      String,
    session_id       String,
    progress_seconds Int32
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka-1:29092,kafka-2:29093',
    kafka_topic_list           = 'movie-events',
    kafka_group_name           = 'clickhouse-consumer',
    kafka_format               = 'JSONEachRow',
    kafka_num_consumers        = 1,
    kafka_skip_broken_messages = 100;

-- Materialized view: Kafka -> MergeTree
CREATE MATERIALIZED VIEW IF NOT EXISTS cinema.movie_events_mv
TO cinema.movie_events AS
SELECT
    event_id,
    user_id,
    movie_id,
    event_type,
    parseDateTimeBestEffort(event_timestamp) AS event_timestamp,
    device_type,
    session_id,
    progress_seconds
FROM cinema.movie_events_kafka;

-- ──────────────────────────────────────────
-- Aggregation tables
-- ──────────────────────────────────────────

CREATE TABLE IF NOT EXISTS cinema.agg_dau (
    metric_date Date,
    dau         UInt64,
    computed_at DateTime('UTC') DEFAULT now()
)
ENGINE = ReplacingMergeTree(computed_at)
ORDER BY metric_date;

CREATE TABLE IF NOT EXISTS cinema.agg_avg_watch_time (
    metric_date          Date,
    avg_progress_seconds Float64,
    computed_at          DateTime('UTC') DEFAULT now()
)
ENGINE = ReplacingMergeTree(computed_at)
ORDER BY metric_date;

CREATE TABLE IF NOT EXISTS cinema.agg_top_movies (
    metric_date Date,
    movie_id    String,
    view_count  UInt64,
    computed_at DateTime('UTC') DEFAULT now()
)
ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (metric_date, movie_id);

CREATE TABLE IF NOT EXISTS cinema.agg_conversion (
    metric_date    Date,
    started_count  UInt64,
    finished_count UInt64,
    conversion_pct Float64,
    computed_at    DateTime('UTC') DEFAULT now()
)
ENGINE = ReplacingMergeTree(computed_at)
ORDER BY metric_date;

CREATE TABLE IF NOT EXISTS cinema.agg_retention (
    cohort_date   Date,
    day_number    UInt8,
    cohort_size   UInt64,
    retained      UInt64,
    retention_pct Float64,
    computed_at   DateTime('UTC') DEFAULT now()
)
ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (cohort_date, day_number);
