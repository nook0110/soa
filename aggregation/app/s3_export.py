"""
Export daily aggregates from PostgreSQL to MinIO (S3-compatible).

Files are stored at:
  s3://movie-analytics/daily/YYYY-MM-DD/aggregates.parquet

Re-running for the same date overwrites the file (idempotent).
"""

import io
import logging
from datetime import date

import boto3
import pandas as pd
import psycopg2
import psycopg2.extras
from botocore.exceptions import BotoCoreError, ClientError
from tenacity import retry, stop_after_attempt, wait_exponential, before_sleep_log

from .config import settings

logger = logging.getLogger(__name__)


def _get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=f"http://{settings.minio_endpoint}",
        aws_access_key_id=settings.minio_access_key,
        aws_secret_access_key=settings.minio_secret_key,
        region_name="us-east-1",
    )


def _fetch_aggregates(conn, target_date: date) -> pd.DataFrame:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            "SELECT metric_date, metric_name, metric_value, computed_at "
            "FROM daily_metrics WHERE metric_date = %s",
            (target_date,),
        )
        rows = cur.fetchall()
    return pd.DataFrame(rows)


def _fetch_top_movies(conn, target_date: date) -> pd.DataFrame:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            "SELECT metric_date, movie_id, view_count, computed_at "
            "FROM top_movies WHERE metric_date = %s ORDER BY view_count DESC",
            (target_date,),
        )
        rows = cur.fetchall()
    return pd.DataFrame(rows)


def _fetch_retention(conn, target_date: date) -> pd.DataFrame:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            "SELECT cohort_date, day_number, cohort_size, retained, retention_pct, computed_at "
            "FROM retention_cohorts WHERE cohort_date = %s ORDER BY day_number",
            (target_date,),
        )
        rows = cur.fetchall()
    return pd.DataFrame(rows)


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True,
)
def export_to_s3(target_date: date | None = None) -> dict:
    """Export all metrics for target_date to Parquet file in MinIO."""
    if target_date is None:
        from datetime import datetime, timezone, timedelta

        target_date = datetime.now(timezone.utc).date() - timedelta(days=1)

    logger.info("S3 export started for date=%s", target_date)

    try:
        conn = psycopg2.connect(settings.postgres_dsn)
    except Exception as exc:
        logger.error("PostgreSQL unavailable: %s", exc)
        raise

    try:
        df_metrics = _fetch_aggregates(conn, target_date)
        df_movies = _fetch_top_movies(conn, target_date)
        df_retention = _fetch_retention(conn, target_date)
    finally:
        conn.close()

    # Combine into a single Parquet with multiple sheets via different sections
    # We use separate Parquet files per table in a date-partitioned prefix
    s3 = _get_s3_client()
    date_prefix = f"daily/{target_date.isoformat()}"

    def _upload_df(df: pd.DataFrame, filename: str) -> None:
        if df.empty:
            logger.warning(
                "Empty dataframe for %s/%s — skipping", date_prefix, filename
            )
            return
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False, engine="pyarrow")
        buffer.seek(0)
        key = f"{date_prefix}/{filename}"
        s3.put_object(
            Bucket=settings.minio_bucket,
            Key=key,
            Body=buffer.getvalue(),
        )
        logger.info(
            "Uploaded s3://%s/%s (%d rows)", settings.minio_bucket, key, len(df)
        )

    try:
        _upload_df(df_metrics, "aggregates.parquet")
        _upload_df(df_movies, "top_movies.parquet")
        _upload_df(df_retention, "retention.parquet")
    except (BotoCoreError, ClientError) as exc:
        logger.error("S3 upload failed: %s", exc)
        raise

    logger.info("S3 export completed for date=%s", target_date)
    return {
        "date": str(target_date),
        "metrics_rows": len(df_metrics),
        "top_movies_rows": len(df_movies),
        "retention_rows": len(df_retention),
    }
