"""
Synthetic event generator.

Simulates realistic user sessions:
  VIEW_STARTED → (VIEW_PAUSED → VIEW_RESUMED)* → VIEW_FINISHED
  sprinkled with LIKED and SEARCHED events.

progress_seconds always increases within a session.
"""

import asyncio
import logging
import random
import uuid
from datetime import datetime, timezone
from typing import Optional

from .config import settings
from .kafka_producer import publish_event

logger = logging.getLogger(__name__)

DEVICE_TYPES = ["MOBILE", "DESKTOP", "TV", "TABLET"]
EVENT_TYPES_STANDALONE = ["LIKED", "SEARCHED"]

# Pool of fake users and movies
USERS = [f"user_{i:04d}" for i in range(1, 201)]
MOVIES = [f"movie_{i:04d}" for i in range(1, 51)]


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _make_event(
    user_id: str,
    movie_id: str,
    event_type: str,
    session_id: str,
    device_type: str,
    progress_seconds: int,
) -> dict:
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": user_id,
        "movie_id": movie_id,
        "event_type": event_type,
        "event_timestamp": _now_iso(),
        "device_type": device_type,
        "session_id": session_id,
        "progress_seconds": progress_seconds,
    }


async def _simulate_session(user_id: str, movie_id: str, device_type: str) -> None:
    """Simulate one full viewing session for a user."""
    session_id = str(uuid.uuid4())
    progress = 0
    movie_duration = random.randint(3600, 7200)  # 1-2 hours

    # VIEW_STARTED
    publish_event(
        _make_event(
            user_id, movie_id, "VIEW_STARTED", session_id, device_type, progress
        )
    )
    await asyncio.sleep(random.uniform(0.1, 0.5))

    # Simulate watching with possible pauses
    while progress < movie_duration:
        chunk = random.randint(30, 300)
        progress = min(progress + chunk, movie_duration)

        # Random pause?
        if random.random() < 0.15:
            publish_event(
                _make_event(
                    user_id, movie_id, "VIEW_PAUSED", session_id, device_type, progress
                )
            )
            await asyncio.sleep(random.uniform(0.05, 0.2))
            publish_event(
                _make_event(
                    user_id, movie_id, "VIEW_RESUMED", session_id, device_type, progress
                )
            )
            await asyncio.sleep(random.uniform(0.05, 0.2))

        if random.random() < 0.5:
            break  # user gave up mid-watch

    # VIEW_FINISHED (only if watched most of the film)
    if progress >= movie_duration * 0.8:
        publish_event(
            _make_event(
                user_id, movie_id, "VIEW_FINISHED", session_id, device_type, progress
            )
        )

    # Maybe add a LIKE
    if random.random() < 0.3:
        await asyncio.sleep(random.uniform(0.05, 0.15))
        publish_event(
            _make_event(user_id, movie_id, "LIKED", session_id, device_type, progress)
        )


async def run_generator() -> None:
    """Continuously generate synthetic events at the configured interval."""
    logger.info(
        "Event generator started (interval=%.1fs)", settings.generator_interval_seconds
    )
    while True:
        try:
            user_id = random.choice(USERS)
            device_type = random.choice(DEVICE_TYPES)

            # Occasionally emit a SEARCHED event
            if random.random() < 0.2:
                publish_event(
                    _make_event(
                        user_id,
                        random.choice(MOVIES),
                        "SEARCHED",
                        str(uuid.uuid4()),
                        device_type,
                        0,
                    )
                )
            else:
                movie_id = random.choice(MOVIES)
                await _simulate_session(user_id, movie_id, device_type)

        except Exception as exc:  # noqa: BLE001
            logger.error("Generator error: %s", exc)

        await asyncio.sleep(settings.generator_interval_seconds)
