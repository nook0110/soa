"""
Kafka producer with:
- JSON serialization (topic-compatible with ClickHouse Kafka Engine)
- Schema Registry registration (Protobuf descriptor)
- Partitioning by user_id
- acks=all, retries with exponential backoff
"""

import json
import logging
import time
from typing import Any, Dict

import requests
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)

from .config import settings

logger = logging.getLogger(__name__)

# ─── Schema Registry ──────────────────────────────────────────────────────────

PROTOBUF_SUBJECT = "movie-events-value"

# Simplified Protobuf file descriptor as raw schema string for SR
_PROTO_SCHEMA = """\
syntax = "proto3";
package cinema.events.v1;
import "google/protobuf/timestamp.proto";

enum EventType {
  EVENT_TYPE_UNSPECIFIED = 0;
  VIEW_STARTED = 1;
  VIEW_FINISHED = 2;
  VIEW_PAUSED = 3;
  VIEW_RESUMED = 4;
  LIKED = 5;
  SEARCHED = 6;
}

enum DeviceType {
  DEVICE_TYPE_UNSPECIFIED = 0;
  MOBILE = 1;
  DESKTOP = 2;
  TV = 3;
  TABLET = 4;
}

message MovieEvent {
  string event_id = 1;
  string user_id = 2;
  string movie_id = 3;
  EventType event_type = 4;
  google.protobuf.Timestamp timestamp = 5;
  DeviceType device_type = 6;
  string session_id = 7;
  int32 progress_seconds = 8;
}
"""


def register_schema_if_needed() -> None:
    """Register Protobuf schema in Schema Registry (idempotent)."""
    url = f"{settings.schema_registry_url}/subjects/{PROTOBUF_SUBJECT}/versions"
    payload = {"schemaType": "PROTOBUF", "schema": _PROTO_SCHEMA}
    for attempt in range(10):
        try:
            resp = requests.post(url, json=payload, timeout=5)
            if resp.status_code in (200, 409):
                logger.info(
                    "Schema registered/confirmed in Schema Registry (id=%s)",
                    resp.json().get("id", "existing"),
                )
                return
            logger.warning(
                "Schema Registry responded %s: %s", resp.status_code, resp.text
            )
        except requests.RequestException as exc:
            logger.warning(
                "Schema Registry not reachable (attempt %d): %s", attempt + 1, exc
            )
        time.sleep(3)
    logger.error("Could not register schema after 10 attempts — continuing anyway.")


# ─── Producer ─────────────────────────────────────────────────────────────────


def _build_producer() -> Producer:
    conf = {
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "acks": "all",
        "retries": 5,
        "retry.backoff.ms": 500,
        "enable.idempotence": True,
        "compression.type": "snappy",
    }
    return Producer(conf)


_producer: Producer | None = None


def get_producer() -> Producer:
    global _producer
    if _producer is None:
        _producer = _build_producer()
    return _producer


def _delivery_callback(err, msg) -> None:
    if err:
        logger.error("Delivery failed: %s", err)
    else:
        logger.debug(
            "Delivered to %s [partition %d] offset %d",
            msg.topic(),
            msg.partition(),
            msg.offset(),
        )


@retry(
    retry=retry_if_exception_type(Exception),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=1, max=30),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True,
)
def publish_event(event: Dict[str, Any]) -> None:
    """Publish a validated event dict as JSON to Kafka.

    Partitioned by user_id for ordered delivery per user.
    """
    producer = get_producer()
    payload = json.dumps(event, default=str).encode("utf-8")
    key = event["user_id"].encode("utf-8")

    logger.info(
        "Publishing event event_id=%s type=%s user=%s",
        event["event_id"],
        event["event_type"],
        event["user_id"],
    )

    producer.produce(
        topic=settings.kafka_topic,
        key=key,
        value=payload,
        on_delivery=_delivery_callback,
    )
    producer.poll(0)
    # Flush to get delivery confirmation
    producer.flush(timeout=10)
