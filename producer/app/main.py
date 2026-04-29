"""
Movie Service Producer — FastAPI application.

Endpoints:
  POST /events        — publish a single event
  GET  /health        — health check
  POST /generator/start   — start synthetic generator (background)
  POST /generator/stop    — stop synthetic generator
"""

import asyncio
import logging
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Optional

from fastapi import FastAPI, HTTPException, status

from .config import settings
from .generator import run_generator
from .kafka_producer import publish_event, register_schema_if_needed
from .schemas import MovieEventRequest, MovieEventResponse

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

_generator_task: Optional[asyncio.Task] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    register_schema_if_needed()
    if settings.generator_enabled:
        global _generator_task
        _generator_task = asyncio.create_task(run_generator())
        logger.info("Background event generator started.")
    yield
    # Shutdown
    if _generator_task and not _generator_task.done():
        _generator_task.cancel()


app = FastAPI(
    title="Movie Event Producer",
    version="1.0.0",
    description="Publishes cinema events to Kafka topic `movie-events`.",
    lifespan=lifespan,
)


@app.get("/health")
async def health():
    return {"status": "ok", "service": "producer"}


@app.post(
    "/events", response_model=MovieEventResponse, status_code=status.HTTP_201_CREATED
)
async def publish(event_req: MovieEventRequest):
    """Validate and publish a movie event to Kafka."""
    event_id = str(event_req.event_id or uuid.uuid4())
    ts = event_req.timestamp or datetime.now(timezone.utc)

    event = {
        "event_id": event_id,
        "user_id": event_req.user_id,
        "movie_id": event_req.movie_id,
        "event_type": event_req.event_type.value,
        "event_timestamp": ts.isoformat(),
        "device_type": event_req.device_type.value,
        "session_id": event_req.session_id,
        "progress_seconds": event_req.progress_seconds,
    }

    try:
        publish_event(event)
    except Exception as exc:
        logger.error("Failed to publish event %s: %s", event_id, exc)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Could not publish event: {exc}",
        ) from exc

    return MovieEventResponse(event_id=event_id)


@app.post("/generator/start")
async def start_generator():
    global _generator_task
    if _generator_task and not _generator_task.done():
        return {"status": "already_running"}
    _generator_task = asyncio.create_task(run_generator())
    return {"status": "started"}


@app.post("/generator/stop")
async def stop_generator():
    global _generator_task
    if _generator_task and not _generator_task.done():
        _generator_task.cancel()
        return {"status": "stopped"}
    return {"status": "not_running"}
