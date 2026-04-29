-- PostgreSQL schema for aggregated cinema metrics

CREATE TABLE IF NOT EXISTS daily_metrics (
    id          BIGSERIAL PRIMARY KEY,
    metric_date DATE        NOT NULL,
    metric_name VARCHAR(64) NOT NULL,
    metric_value DOUBLE PRECISION NOT NULL,
    computed_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (metric_date, metric_name)
);

-- Detailed top movies per day
CREATE TABLE IF NOT EXISTS top_movies (
    id           BIGSERIAL PRIMARY KEY,
    metric_date  DATE         NOT NULL,
    movie_id     VARCHAR(64)  NOT NULL,
    view_count   BIGINT       NOT NULL,
    computed_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (metric_date, movie_id)
);

-- Retention cohorts
CREATE TABLE IF NOT EXISTS retention_cohorts (
    id            BIGSERIAL PRIMARY KEY,
    cohort_date   DATE    NOT NULL,
    day_number    INTEGER NOT NULL,
    cohort_size   INTEGER NOT NULL,
    retained      INTEGER NOT NULL,
    retention_pct DOUBLE PRECISION NOT NULL,
    computed_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (cohort_date, day_number)
);

CREATE INDEX IF NOT EXISTS idx_daily_metrics_date ON daily_metrics(metric_date);
CREATE INDEX IF NOT EXISTS idx_top_movies_date ON top_movies(metric_date);
CREATE INDEX IF NOT EXISTS idx_retention_cohort_date ON retention_cohorts(cohort_date);
