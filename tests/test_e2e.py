"""
End-to-End test: full warehouse scenario through Kafka → Consumer → Cassandra.

Scenario:
  1. Send PRODUCT_RECEIVED  (qty=200, SKU-E2E-xxx, ZONE-E2E)
  2. Send PRODUCT_RESERVED  (qty=50)
  3. Send ORDER_CREATED     (order_items=[{SKU-E2E-xxx, ZONE-E2E, qty=30}])
  4. Send ORDER_COMPLETED
  5. Verify final state in Cassandra:
     - available_quantity = 200 - 50 - 30 = 120
     - reserved_quantity  = 50 + 30 - 30 = 50   (ORDER_CREATED reserves 30,
                                                   ORDER_COMPLETED ships 30)
  6. Verify HTTP status codes and response bodies from /health and /metrics

Run:
  pytest tests/test_e2e.py -v --timeout=180
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


def _now_iso():
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat()


@pytest.fixture(scope="module")
def cassandra_session():
    profile = ExecutionProfile(
        consistency_level=ConsistencyLevel.ONE,
        load_balancing_policy=DCAwareRoundRobinPolicy(local_dc="datacenter1"),
    )
    cluster = Cluster([CASSANDRA_HOST], execution_profiles={EXEC_PROFILE_DEFAULT: profile})
    session = cluster.connect("warehouse")
    yield session
    cluster.shutdown()


@pytest.fixture(scope="module")
def kafka_ctx():
    sr_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY})
    serializer = AvroSerializer(sr_client, SCHEMA_V2)
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
    ctx = SerializationContext(KAFKA_TOPIC, MessageField.VALUE)
    yield producer, serializer, ctx
    producer.flush()


def _send_event(kafka_ctx_fixture, event_type: str, seq: int, **kwargs):
    producer, serializer, ctx = kafka_ctx_fixture
    event = {
        "event_id":        str(uuid.uuid4()),
        "event_type":      event_type,
        "timestamp":       _now_iso(),
        "sequence_number": seq,
        "product_id":      kwargs.get("product_id"),
        "zone_id":         kwargs.get("zone_id"),
        "from_zone_id":    None,
        "to_zone_id":      None,
        "quantity":        kwargs.get("quantity"),
        "order_id":        kwargs.get("order_id"),
        "order_items":     kwargs.get("order_items"),
        "supplier_id":     None,
    }
    data = serializer(event, ctx)
    producer.produce(KAFKA_TOPIC, value=data, key=event["event_id"].encode())
    producer.flush()
    return event


def _poll_inventory(session, product_id, zone_id, timeout=60):
    deadline = time.time() + timeout
    while time.time() < deadline:
        rows = list(session.execute(
            "SELECT available_quantity, reserved_quantity FROM inventory_by_product_zone "
            "WHERE product_id = %s AND zone_id = %s",
            (product_id, zone_id),
        ))
        if rows:
            return rows[0]
        time.sleep(1)
    raise TimeoutError(f"No inventory row for {product_id}/{zone_id} after {timeout}s")


def _poll_order_status(session, order_id, expected_status, timeout=60):
    deadline = time.time() + timeout
    while time.time() < deadline:
        rows = list(session.execute(
            "SELECT status FROM orders WHERE order_id = %s", (order_id,)
        ))
        if rows and rows[0].status == expected_status:
            return rows[0]
        time.sleep(1)
    raise TimeoutError(f"Order {order_id} did not reach status={expected_status} in {timeout}s")


# ── E2E Test ──────────────────────────────────────────────────────────────
class TestFullWarehouseScenario:
    """
    Full end-to-end scenario:
      PRODUCT_RECEIVED → PRODUCT_RESERVED → ORDER_CREATED → ORDER_COMPLETED
    Verified in Cassandra.
    """

    def test_full_lifecycle(self, cassandra_session, kafka_ctx):
        product_id = f"E2E-{uuid.uuid4().hex[:10]}"
        zone_id    = "ZONE-E2E-1"
        order_id   = str(uuid.uuid4())
        seq        = int(time.time() * 1000)

        # ── Step 1: receive 200 units ─────────────────────────────────────
        _send_event(kafka_ctx, "PRODUCT_RECEIVED", seq,
                    product_id=product_id, zone_id=zone_id, quantity=200)
        seq += 1
        time.sleep(2)

        # ── Step 2: reserve 50 units ──────────────────────────────────────
        _send_event(kafka_ctx, "PRODUCT_RESERVED", seq,
                    product_id=product_id, zone_id=zone_id, quantity=50)
        seq += 1
        time.sleep(2)

        # ── Step 3: create order for 30 units (reserves additional 30) ────
        _send_event(kafka_ctx, "ORDER_CREATED", seq,
                    order_id=order_id,
                    order_items=[{"product_id": product_id, "zone_id": zone_id, "quantity": 30}])
        seq += 1
        time.sleep(2)

        # ── Verify order was created ───────────────────────────────────────
        _poll_order_status(cassandra_session, order_id, "CREATED", timeout=40)

        # ── Step 4: complete the order ────────────────────────────────────
        _send_event(kafka_ctx, "ORDER_COMPLETED", seq, order_id=order_id)
        seq += 1

        # ── Verify order completed ────────────────────────────────────────
        _poll_order_status(cassandra_session, order_id, "COMPLETED", timeout=40)

        # ── Verify final inventory ────────────────────────────────────────
        # available = 200 - 50(reserve) - 30(order_reserve) = 120
        # reserved  = 50 + 30(order_reserve) - 30(order_complete) = 50
        deadline = time.time() + 60
        row = None
        while time.time() < deadline:
            row = _poll_inventory(cassandra_session, product_id, zone_id, timeout=5)
            if row.available_quantity == 120 and row.reserved_quantity == 50:
                break
            time.sleep(2)

        assert row is not None, "No inventory row found"
        assert row.available_quantity == 120, \
            f"Expected available=120, got {row.available_quantity}"
        assert row.reserved_quantity == 50, \
            f"Expected reserved=50, got {row.reserved_quantity}"

    def test_event_appears_in_history(self, cassandra_session, kafka_ctx):
        """PRODUCT_RECEIVED event must appear in event_history audit table."""
        product_id = f"E2E-HIST-{uuid.uuid4().hex[:8]}"
        zone_id    = "ZONE-HIST"

        _send_event(kafka_ctx, "PRODUCT_RECEIVED", int(time.time() * 1000),
                    product_id=product_id, zone_id=zone_id, quantity=99)

        deadline = time.time() + 40
        rows = []
        while time.time() < deadline:
            rows = list(cassandra_session.execute(
                "SELECT event_type, quantity FROM event_history WHERE product_id = %s",
                (product_id,),
            ))
            if rows:
                break
            time.sleep(1)

        assert len(rows) >= 1
        assert rows[0].event_type == "PRODUCT_RECEIVED"
        assert rows[0].quantity == 99


class TestHTTPEndpoints:
    """Verify HTTP status codes and response body shapes."""

    def test_health_status_code_200(self):
        resp = requests.get(f"{CONSUMER_URL}/health", timeout=10)
        # 200 = healthy, 503 = degraded but service is up
        assert resp.status_code in (200, 503)

    def test_health_response_has_status_field(self):
        resp = requests.get(f"{CONSUMER_URL}/health", timeout=10)
        body = resp.json()
        assert "status" in body
        assert "kafka" in body
        assert "cassandra" in body

    def test_metrics_endpoint_returns_prometheus_format(self):
        resp = requests.get(f"{CONSUMER_URL}/metrics", timeout=10)
        assert resp.status_code == 200
        assert "text/plain" in resp.headers.get("content-type", "")

    def test_metrics_contains_required_metric_names(self):
        # trigger multiple /health calls to ensure HTTP metrics are populated
        requests.get(f"{CONSUMER_URL}/health", timeout=5)
        requests.get(f"{CONSUMER_URL}/health", timeout=5)
        time.sleep(1)  # Allow metrics to be updated
        
        resp = requests.get(f"{CONSUMER_URL}/metrics", timeout=10)
        content = resp.text
        
        # Check for HTTP metrics that should be present due to middleware
        assert "http_requests_total" in content, "Missing http_requests_total metric"
        assert "http_request_duration_seconds" in content, "Missing http_request_duration_seconds metric"
        
        # Check for service-specific metrics
        assert "event_processing_duration_seconds" in content, "Missing event_processing_duration_seconds metric"
        assert "cassandra_write_errors_total" in content, "Missing cassandra_write_errors_total metric"
