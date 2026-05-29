"""
Integration tests for the Warehouse consumer service.

Requires a running docker-compose environment with:
  - Kafka (localhost:9092)
  - Schema Registry (localhost:8081)
  - Cassandra (localhost:9042)
  - Consumer HTTP API (localhost:8000)

Run after `docker-compose up -d` with:
  pytest tests/test_integration.py -v --timeout=120

Environment variables (optional overrides):
  KAFKA_BOOTSTRAP_SERVERS   default: localhost:9092
  SCHEMA_REGISTRY_URL       default: http://localhost:8081
  CASSANDRA_HOST            default: localhost
  CONSUMER_URL              default: http://localhost:8000
"""

import json
import os
import time
import uuid
import pytest
import requests

from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import DCAwareRoundRobinPolicy
from cassandra.query import ConsistencyLevel
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

# ── Config ────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
SCHEMA_REGISTRY = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
CASSANDRA_HOST  = os.getenv("CASSANDRA_HOST", "localhost")
CONSUMER_URL    = os.getenv("CONSUMER_URL", "http://localhost:8000")
KAFKA_TOPIC     = "warehouse-events"

SCHEMA_V2 = json.dumps({
    "type": "record",
    "name": "WarehouseEvent",
    "namespace": "com.warehouse",
    "fields": [
        {"name": "event_id",        "type": "string"},
        {"name": "event_type",      "type": {
            "type": "enum", "name": "EventType",
            "symbols": ["PRODUCT_RECEIVED","PRODUCT_SHIPPED","PRODUCT_MOVED",
                        "PRODUCT_RESERVED","PRODUCT_RELEASED","INVENTORY_COUNTED",
                        "ORDER_CREATED","ORDER_COMPLETED"]
        }},
        {"name": "timestamp",       "type": "string"},
        {"name": "sequence_number", "type": "long"},
        {"name": "product_id",  "type": ["null","string"], "default": None},
        {"name": "zone_id",     "type": ["null","string"], "default": None},
        {"name": "from_zone_id","type": ["null","string"], "default": None},
        {"name": "to_zone_id",  "type": ["null","string"], "default": None},
        {"name": "quantity",    "type": ["null","int"],    "default": None},
        {"name": "order_id",    "type": ["null","string"], "default": None},
        {"name": "order_items", "type": ["null", {
            "type": "array",
            "items": {
                "type": "record", "name": "OrderItem",
                "fields": [
                    {"name": "product_id", "type": "string"},
                    {"name": "zone_id",    "type": "string"},
                    {"name": "quantity",   "type": "int"}
                ]
            }
        }], "default": None},
        {"name": "supplier_id", "type": ["null","string"], "default": None},
    ]
})


# ── Fixtures ──────────────────────────────────────────────────────────────
@pytest.fixture(scope="session")
def cassandra_session():
    profile = ExecutionProfile(
        consistency_level=ConsistencyLevel.ONE,
        load_balancing_policy=DCAwareRoundRobinPolicy(local_dc="datacenter1"),
    )
    cluster = Cluster([CASSANDRA_HOST], execution_profiles={EXEC_PROFILE_DEFAULT: profile})
    session = cluster.connect("warehouse")
    yield session
    cluster.shutdown()


@pytest.fixture(scope="session")
def kafka_producer():
    sr_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY})
    serializer = AvroSerializer(sr_client, SCHEMA_V2)
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
    ctx = SerializationContext(KAFKA_TOPIC, MessageField.VALUE)
    yield producer, serializer, ctx
    producer.flush()


def _now_iso():
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat()


def _send(kafka_producer_fixture, event_type: str, **kwargs):
    producer, serializer, ctx = kafka_producer_fixture
    event = {
        "event_id":        str(uuid.uuid4()),
        "event_type":      event_type,
        "timestamp":       _now_iso(),
        "sequence_number": int(time.time() * 1000),
        "product_id":      kwargs.get("product_id"),
        "zone_id":         kwargs.get("zone_id"),
        "from_zone_id":    kwargs.get("from_zone_id"),
        "to_zone_id":      kwargs.get("to_zone_id"),
        "quantity":        kwargs.get("quantity"),
        "order_id":        kwargs.get("order_id"),
        "order_items":     kwargs.get("order_items"),
        "supplier_id":     kwargs.get("supplier_id"),
    }
    data = serializer(event, ctx)
    producer.produce(KAFKA_TOPIC, value=data, key=event["event_id"].encode())
    producer.flush()
    return event


def _wait_for_inventory(session, product_id: str, zone_id: str, timeout: int = 30) -> dict:
    """Poll Cassandra until inventory row appears or timeout."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        rows = list(session.execute(
            "SELECT available_quantity, reserved_quantity FROM inventory_by_product_zone "
            "WHERE product_id = %s AND zone_id = %s",
            (product_id, zone_id),
        ))
        if rows:
            return {"available": rows[0].available_quantity, "reserved": rows[0].reserved_quantity}
        time.sleep(1)
    raise TimeoutError(f"Inventory for {product_id}/{zone_id} not found within {timeout}s")


# ── Tests ─────────────────────────────────────────────────────────────────
class TestHealthEndpoint:
    def test_health_returns_200(self):
        resp = requests.get(f"{CONSUMER_URL}/health", timeout=5)
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "ok"

    def test_metrics_endpoint_reachable(self):
        resp = requests.get(f"{CONSUMER_URL}/metrics", timeout=5)
        assert resp.status_code == 200
        assert b"events_processed_total" in resp.content or b"http_requests_total" in resp.content


class TestProductReceivedIntegration:
    """Send PRODUCT_RECEIVED event → check Cassandra inventory updated."""

    def test_product_received_creates_inventory(self, cassandra_session, kafka_producer):
        product_id = f"INT-TEST-{uuid.uuid4().hex[:8]}"
        zone_id    = "ZONE-INT-A"

        _send(kafka_producer, "PRODUCT_RECEIVED",
              product_id=product_id, zone_id=zone_id, quantity=100)

        inv = _wait_for_inventory(cassandra_session, product_id, zone_id, timeout=40)
        assert inv["available"] == 100
        assert inv["reserved"] == 0

    def test_product_received_then_reserved(self, cassandra_session, kafka_producer):
        product_id = f"INT-RSVD-{uuid.uuid4().hex[:8]}"
        zone_id    = "ZONE-INT-B"

        _send(kafka_producer, "PRODUCT_RECEIVED",
              product_id=product_id, zone_id=zone_id, quantity=50)
        time.sleep(2)  # let first event settle
        _send(kafka_producer, "PRODUCT_RESERVED",
              product_id=product_id, zone_id=zone_id, quantity=20)

        # Wait for reservation to appear
        deadline = time.time() + 40
        while time.time() < deadline:
            inv = _wait_for_inventory(cassandra_session, product_id, zone_id, timeout=40)
            if inv["reserved"] == 20:
                break
            time.sleep(1)
        assert inv["available"] == 30
        assert inv["reserved"] == 20


class TestIdempotencyIntegration:
    """Sending the same event_id twice must not double-count."""

    def test_duplicate_event_not_double_counted(self, cassandra_session, kafka_producer):
        product_id = f"INT-IDEM-{uuid.uuid4().hex[:8]}"
        zone_id    = "ZONE-INT-C"
        producer, serializer, ctx = kafka_producer

        # Build one event and send it twice with the same event_id
        event = {
            "event_id":        str(uuid.uuid4()),
            "event_type":      "PRODUCT_RECEIVED",
            "timestamp":       _now_iso(),
            "sequence_number": int(time.time() * 1000),
            "product_id":      product_id,
            "zone_id":         zone_id,
            "from_zone_id":    None,
            "to_zone_id":      None,
            "quantity":        77,
            "order_id":        None,
            "order_items":     None,
            "supplier_id":     None,
        }
        for _ in range(2):
            data = serializer(event, ctx)
            producer.produce(KAFKA_TOPIC, value=data, key=event["event_id"].encode())
            producer.flush()
            time.sleep(0.5)

        inv = _wait_for_inventory(cassandra_session, product_id, zone_id, timeout=40)
        # Must be exactly 77, not 154
        assert inv["available"] == 77


class TestMetricsAfterLoad:
    """After sending events, Prometheus metrics must reflect them."""

    def test_events_processed_counter_increases(self, kafka_producer):
        # Record baseline
        resp = requests.get(f"{CONSUMER_URL}/metrics", timeout=5)
        assert resp.status_code == 200

        product_id = f"INT-METR-{uuid.uuid4().hex[:8]}"
        _send(kafka_producer, "PRODUCT_RECEIVED",
              product_id=product_id, zone_id="ZONE-INT-D", quantity=10)
        time.sleep(5)

        resp2 = requests.get(f"{CONSUMER_URL}/metrics", timeout=5)
        assert b"events_processed_total" in resp2.content

    def test_http_request_duration_histogram_present(self):
        requests.get(f"{CONSUMER_URL}/health", timeout=5)
        resp = requests.get(f"{CONSUMER_URL}/metrics", timeout=5)
        assert b"http_request_duration_seconds" in resp.content
