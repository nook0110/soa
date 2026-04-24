"""
Aggregation Service — FastAPI application.

Endpoints:
  GET  /health                — health check
  POST /aggregate             — trigger aggregation for a date
  POST /export                — trigger S3 export for a date

Background:
  - APScheduler runs aggregation every AGGREGATION_SCHEDULE_SECONDS
  - APScheduler runs S3 export every EXPORT_SCHEDULE_SECONDS
"""

import logging
from contextlib import asynccontextmanager
from datetime import date, datetime, timezone, timedelta
from typing import Optional

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI, HTTPException, Query

from .aggregator import run_aggregation
from .config import settings
from .s3_export import export_to_s3

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

scheduler = AsyncIOScheduler()


def _scheduled_aggregation():
    yesterday = datetime.now(timezone.utc).date() - timedelta(days=1)
    try:
        run_aggregation(yesterday)
    except Exception as exc:
        logger.error("Scheduled aggregation failed: %s", exc)


def _scheduled_export():
    yesterday = datetime.now(timezone.utc).date() - timedelta(days=1)
    try:
        export_to_s3(yesterday)
    except Exception as exc:
        logger.error("Scheduled S3 export failed: %s", exc)


@asynccontextmanager
async def lifespan(app: FastAPI):
    scheduler.add_job(
        _scheduled_aggregation,
        "interval",
        seconds=settings.aggregation_schedule_seconds,
        id="aggregation",
    )
    scheduler.add_job(
        _scheduled_export,
        "interval",
        seconds=settings.export_schedule_seconds,
        id="s3_export",
    )
    scheduler.start()
    logger.info(
        "Scheduler started: aggregation every %ds, export every %ds",
        settings.aggregation_schedule_seconds,
        settings.export_schedule_seconds,
    )
    yield
    scheduler.shutdown()


app = FastAPI(
    title="Cinema Aggregation Service",
    version="1.0.0",
    description="Computes business metrics from ClickHouse and stores in PostgreSQL / S3.",
    lifespan=lifespan,
)


@app.get("/health")
async def health():
    return {"status": "ok", "service": "aggregation"}


@app.post("/aggregate")
async def trigger_aggregation(
    target_date: Optional[date] = Query(
        default=None,
        description="Date to aggregate (YYYY-MM-DD). Defaults to yesterday.",
    ),
):
    """Manually trigger aggregation for a specific date."""
    try:
        result = run_aggregation(target_date)
        return {"status": "success", "result": result}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/export")
async def trigger_export(
    target_date: Optional[date] = Query(
        default=None,
        description="Date to export (YYYY-MM-DD). Defaults to yesterday.",
    ),
):
    """Manually trigger S3 export for a specific date."""
    try:
        result = export_to_s3(target_date)
        return {"status": "success", "result": result}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
