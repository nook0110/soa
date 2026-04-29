"""
Business metrics aggregation:
  - DAU
  - Average watch time (VIEW_FINISHED)
  - Top movies by views
  - View conversion (VIEW_FINISHED / VIEW_STARTED)
  - Retention D1 / D7
"""

import logging
import time
from datetime import date, timedelta
from typing import List, Tuple

import psycopg2
import psycopg2.extras
from tenacity import retry, stop_after_attempt, wait_exponential, before_sleep_log

from .clickhouse_client import get_client
from .config import settings

logger = logging.getLogger(__name__)

# ─── PostgreSQL helpers ───────────────────────────────────────────────────────


def _pg_conn():
    return psycopg2.connect(settings.postgres_dsn)


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True,
)
def _upsert_daily_metric(conn, metric_date: date, name: str, value: float) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO daily_metrics (metric_date, metric_name, metric_value)
            VALUES (%s, %s, %s)
            ON CONFLICT (metric_date, metric_name) DO UPDATE
                SET metric_value = EXCLUDED.metric_value,
                    computed_at  = NOW()
            """,
            (metric_date, name, value),
        )


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True,
)
def _upsert_top_movies(conn, metric_date: date, rows: List[Tuple[str, int]]) -> None:
    with conn.cursor() as cur:
        for movie_id, count in rows:
            cur.execute(
                """
                INSERT INTO top_movies (metric_date, movie_id, view_count)
                VALUES (%s, %s, %s)
                ON CONFLICT (metric_date, movie_id) DO UPDATE
                    SET view_count  = EXCLUDED.view_count,
                        computed_at = NOW()
                """,
                (metric_date, movie_id, count),
            )


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True,
)
def _upsert_retention(
    conn, cohort_date: date, day_number: int, size: int, retained: int, pct: float
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO retention_cohorts (cohort_date, day_number, cohort_size, retained, retention_pct)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (cohort_date, day_number) DO UPDATE
                SET cohort_size   = EXCLUDED.cohort_size,
                    retained      = EXCLUDED.retained,
                    retention_pct = EXCLUDED.retention_pct,
                    computed_at   = NOW()
            """,
            (cohort_date, day_number, size, retained, pct),
        )


# ─── ClickHouse queries ───────────────────────────────────────────────────────


def _compute_dau(ch, target_date: date) -> int:
    result = ch.query(
        """
        SELECT uniq(user_id) AS dau
        FROM cinema.movie_events
        WHERE toDate(event_timestamp) = {d:Date}
        """,
        parameters={"d": target_date},
    )
    return int(result.first_row[0]) if result.first_row else 0


def _compute_avg_watch_time(ch, target_date: date) -> float:
    result = ch.query(
        """
        SELECT avg(progress_seconds) AS avg_watch
        FROM cinema.movie_events
        WHERE toDate(event_timestamp) = {d:Date}
          AND event_type = 'VIEW_FINISHED'
        """,
        parameters={"d": target_date},
    )
    val = result.first_row[0] if result.first_row else 0
    return float(val or 0)


def _compute_top_movies(
    ch, target_date: date, top_n: int = 10
) -> List[Tuple[str, int]]:
    result = ch.query(
        """
        SELECT movie_id, count() AS views
        FROM cinema.movie_events
        WHERE toDate(event_timestamp) = {d:Date}
          AND event_type IN ('VIEW_STARTED', 'VIEW_FINISHED')
        GROUP BY movie_id
        ORDER BY views DESC
        LIMIT {n:UInt8}
        """,
        parameters={"d": target_date, "n": top_n},
    )
    return [(row[0], int(row[1])) for row in result.result_rows]


def _compute_conversion(ch, target_date: date) -> Tuple[int, int, float]:
    result = ch.query(
        """
        SELECT
            countIf(event_type = 'VIEW_STARTED')  AS started,
            countIf(event_type = 'VIEW_FINISHED') AS finished
        FROM cinema.movie_events
        WHERE toDate(event_timestamp) = {d:Date}
        """,
        parameters={"d": target_date},
    )
    row = result.first_row
    started, finished = (int(row[0]), int(row[1])) if row else (0, 0)
    pct = (finished / started * 100) if started > 0 else 0.0
    return started, finished, pct


def _compute_retention(
    ch, cohort_date: date, max_days: int = 7
) -> List[Tuple[int, int, int, float]]:
    """Returns list of (day_number, cohort_size, retained, pct)."""
    result = ch.query(
        """
        WITH cohort AS (
            SELECT
                user_id,
                min(toDate(event_timestamp)) AS first_day
            FROM cinema.movie_events
            GROUP BY user_id
            HAVING first_day = {cd:Date}
        ),
        cohort_size AS (
            SELECT uniq(user_id) AS size FROM cohort
        )
        SELECT
            day_num,
            (SELECT size FROM cohort_size) AS cohort_size,
            retained,
            if((SELECT size FROM cohort_size) > 0,
               retained / (SELECT size FROM cohort_size) * 100, 0) AS retention_pct
        FROM (
            SELECT
                dateDiff('day', c.first_day, toDate(e.event_timestamp)) AS day_num,
                uniq(e.user_id) AS retained
            FROM cinema.movie_events e
            INNER JOIN cohort c ON e.user_id = c.user_id
            WHERE day_num BETWEEN 0 AND {md:UInt8}
            GROUP BY day_num
        )
        ORDER BY day_num
        """,
        parameters={"cd": cohort_date, "md": max_days},
    )
    rows = []
    cohort_size = 0
    # Determine cohort size from day 0
    for row in result.result_rows:
        day_num, cs, retained, pct = (
            int(row[0]),
            int(row[1]),
            int(row[2]),
            float(row[3]),
        )
        if day_num == 0:
            cohort_size = retained  # day 0 = everyone
        rows.append((day_num, cohort_size, retained, pct))
    return rows


def _upsert_ch_agg(
    ch,
    target_date: date,
    dau: int,
    avg_watch: float,
    started: int,
    finished: int,
    conv_pct: float,
) -> None:
    """Store aggregates back into ClickHouse aggregation tables."""
    ch.insert(
        "cinema.agg_dau",
        [[target_date, dau]],
        column_names=["metric_date", "dau"],
    )
    ch.insert(
        "cinema.agg_avg_watch_time",
        [[target_date, avg_watch]],
        column_names=["metric_date", "avg_progress_seconds"],
    )
    ch.insert(
        "cinema.agg_conversion",
        [[target_date, started, finished, conv_pct]],
        column_names=[
            "metric_date",
            "started_count",
            "finished_count",
            "conversion_pct",
        ],
    )


# ─── Public API ──────────────────────────────────────────────────────────────


def run_aggregation(target_date: date | None = None) -> dict:
    """Run full aggregation for target_date (defaults to yesterday)."""
    if target_date is None:
        from datetime import datetime, timezone

        target_date = datetime.now(timezone.utc).date() - timedelta(days=1)

    logger.info("Aggregation cycle started for date=%s", target_date)
    t0 = time.perf_counter()

    ch = get_client()
    pg = _pg_conn()

    try:
        # Compute metrics
        dau = _compute_dau(ch, target_date)
        avg_watch = _compute_avg_watch_time(ch, target_date)
        top_movies = _compute_top_movies(ch, target_date)
        started, finished, conv_pct = _compute_conversion(ch, target_date)
        retention_rows = _compute_retention(ch, target_date)

        # Write to ClickHouse aggregation tables
        _upsert_ch_agg(ch, target_date, dau, avg_watch, started, finished, conv_pct)
        if top_movies:
            ch.insert(
                "cinema.agg_top_movies",
                [[target_date, mid, cnt] for mid, cnt in top_movies],
                column_names=["metric_date", "movie_id", "view_count"],
            )
        if retention_rows:
            ch.insert(
                "cinema.agg_retention",
                [[target_date, dn, cs, rt, pt] for dn, cs, rt, pt in retention_rows],
                column_names=[
                    "cohort_date",
                    "day_number",
                    "cohort_size",
                    "retained",
                    "retention_pct",
                ],
            )

        # Write to PostgreSQL
        with pg:
            _upsert_daily_metric(pg, target_date, "dau", dau)
            _upsert_daily_metric(pg, target_date, "avg_watch_seconds", avg_watch)
            _upsert_daily_metric(pg, target_date, "conversion_pct", conv_pct)
            _upsert_daily_metric(pg, target_date, "view_started", started)
            _upsert_daily_metric(pg, target_date, "view_finished", finished)
            _upsert_top_movies(pg, target_date, top_movies)
            for day_num, cohort_size, retained, pct in retention_rows:
                _upsert_retention(pg, target_date, day_num, cohort_size, retained, pct)

        elapsed = time.perf_counter() - t0
        total_records = dau + len(top_movies) + len(retention_rows)
        logger.info(
            "Aggregation cycle completed: date=%s records=%d elapsed=%.2fs",
            target_date,
            total_records,
            elapsed,
        )
        return {
            "date": str(target_date),
            "dau": dau,
            "avg_watch_seconds": avg_watch,
            "conversion_pct": conv_pct,
            "top_movies_count": len(top_movies),
            "elapsed_seconds": elapsed,
        }

    except Exception as exc:
        logger.error("Aggregation failed for %s: %s", target_date, exc)
        raise
    finally:
        pg.close()
        ch.close()
