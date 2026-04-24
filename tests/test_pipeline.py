"""
Integration tests: Kafka → ClickHouse pipeline.

Test flow:
  1. Publish a uniquely identifiable event via HTTP API (POST /events).
  2. Wait up to 60 seconds for the event to land in ClickHouse.
  3. Verify all fields are correct.

Isolation: each test uses a unique event_id, so repeated runs don't collide.

Run:
  pip install -r tests/requirements.txt
  pytest tests/ -v
"""

import json
import time
import uuid
from datetime import datetime, timezone

import httpx
import pytest
from tenacity import retry, stop_after_delay, wait_fixed

PRODUCER_URL = "http://localhost:8000"
CH_POLL_TIMEOUT = 60  # seconds to wait for event to appear in ClickHouse


# ─── Helpers ─────────────────────────────────────────────────────────────────


def _publish_via_api(event_payload: dict) -> str:
    """Publish event through HTTP API, return event_id."""
    resp = httpx.post(f"{PRODUCER_URL}/events", json=event_payload, timeout=10)
    assert resp.status_code == 201, f"Unexpected status {resp.status_code}: {resp.text}"
    return resp.json()["event_id"]


def _wait_for_event_in_clickhouse(
    ch_client, event_id: str, timeout: int = CH_POLL_TIMEOUT
) -> dict:
    """Poll ClickHouse until event appears or timeout."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = ch_client.query(
            "SELECT event_id, user_id, movie_id, event_type, device_type, session_id, progress_seconds "
            "FROM cinema.movie_events WHERE event_id = {eid:String} LIMIT 1",
            parameters={"eid": event_id},
        )
        if result.result_rows:
            row = result.result_rows[0]
            return {
                "event_id": row[0],
                "user_id": row[1],
                "movie_id": row[2],
                "event_type": row[3],
                "device_type": row[4],
                "session_id": row[5],
                "progress_seconds": row[6],
            }
        time.sleep(2)
    pytest.fail(f"Event {event_id} did not appear in ClickHouse within {timeout}s")


# ─── Tests ───────────────────────────────────────────────────────────────────


class TestProducerHealth:
    def test_health_endpoint(self):
        resp = httpx.get(f"{PRODUCER_URL}/health", timeout=5)
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"


class TestEventPublishing:
    def test_publish_view_started_event(self, ch_client):
        """Publish VIEW_STARTED and verify it reaches ClickHouse with all fields."""
        event_id = str(uuid.uuid4())
        session_id = str(uuid.uuid4())
        user_id = f"test_user_{uuid.uuid4().hex[:8]}"
        movie_id = "movie_test_001"

        payload = {
            "event_id": event_id,
            "user_id": user_id,
            "movie_id": movie_id,
            "event_type": "VIEW_STARTED",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "device_type": "DESKTOP",
            "session_id": session_id,
            "progress_seconds": 0,
        }

        returned_id = _publish_via_api(payload)
        assert returned_id == event_id

        row = _wait_for_event_in_clickhouse(ch_client, event_id)
        assert row["event_id"] == event_id
        assert row["user_id"] == user_id
        assert row["movie_id"] == movie_id
        assert row["event_type"] == "VIEW_STARTED"
        assert row["device_type"] == "DESKTOP"
        assert row["session_id"] == session_id
        assert row["progress_seconds"] == 0

    def test_publish_view_finished_event(self, ch_client):
        """VIEW_FINISHED with positive progress_seconds lands in ClickHouse."""
        event_id = str(uuid.uuid4())
        session_id = str(uuid.uuid4())
        user_id = f"test_user_{uuid.uuid4().hex[:8]}"

        payload = {
            "event_id": event_id,
            "user_id": user_id,
            "movie_id": "movie_test_002",
            "event_type": "VIEW_FINISHED",
            "device_type": "TV",
            "session_id": session_id,
            "progress_seconds": 5400,
        }

        _publish_via_api(payload)
        row = _wait_for_event_in_clickhouse(ch_client, event_id)
        assert row["event_type"] == "VIEW_FINISHED"
        assert row["progress_seconds"] == 5400
        assert row["device_type"] == "TV"

    def test_publish_liked_event(self, ch_client):
        """LIKED event with zero progress_seconds."""
        event_id = str(uuid.uuid4())
        payload = {
            "event_id": event_id,
            "user_id": f"test_user_{uuid.uuid4().hex[:8]}",
            "movie_id": "movie_test_003",
            "event_type": "LIKED",
            "device_type": "MOBILE",
            "session_id": str(uuid.uuid4()),
            "progress_seconds": 0,
        }
        _publish_via_api(payload)
        row = _wait_for_event_in_clickhouse(ch_client, event_id)
        assert row["event_type"] == "LIKED"

    def test_invalid_event_rejected(self):
        """Missing required fields should return 422."""
        resp = httpx.post(
            f"{PRODUCER_URL}/events",
            json={"event_type": "VIEW_STARTED"},  # missing user_id, movie_id etc.
            timeout=5,
        )
        assert resp.status_code == 422

    def test_invalid_event_type_rejected(self):
        """Unknown event_type should return 422."""
        payload = {
            "user_id": "user_001",
            "movie_id": "movie_001",
            "event_type": "UNKNOWN_TYPE",
            "device_type": "DESKTOP",
            "session_id": str(uuid.uuid4()),
            "progress_seconds": 0,
        }
        resp = httpx.post(f"{PRODUCER_URL}/events", json=payload, timeout=5)
        assert resp.status_code == 422

    def test_multiple_events_same_user_ordered(self, ch_client):
        """Publish a full session (STARTED→PAUSED→RESUMED→FINISHED) for same user."""
        user_id = f"test_user_{uuid.uuid4().hex[:8]}"
        session_id = str(uuid.uuid4())
        movie_id = "movie_test_session"
        event_ids = {}

        for event_type, progress in [
            ("VIEW_STARTED", 0),
            ("VIEW_PAUSED", 120),
            ("VIEW_RESUMED", 120),
            ("VIEW_FINISHED", 5400),
        ]:
            eid = str(uuid.uuid4())
            event_ids[event_type] = eid
            _publish_via_api(
                {
                    "event_id": eid,
                    "user_id": user_id,
                    "movie_id": movie_id,
                    "event_type": event_type,
                    "device_type": "TABLET",
                    "session_id": session_id,
                    "progress_seconds": progress,
                }
            )

        # Verify all 4 events land in ClickHouse
        for event_type, eid in event_ids.items():
            row = _wait_for_event_in_clickhouse(ch_client, eid)
            assert row["event_type"] == event_type, f"Expected {event_type} for {eid}"
            assert row["user_id"] == user_id
            assert row["session_id"] == session_id
