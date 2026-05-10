"""
WMS Producer Service
Generates warehouse events and publishes to Kafka using Avro + Schema Registry.
Demonstrates all event types including V1 and V2 schemas.
"""

import json
import os
import time
import uuid
import logging
import requests

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [PRODUCER] %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "warehouse-events")

# ──────────────────────────────────────────────
# Avro schemas
# ──────────────────────────────────────────────
SCHEMA_V1 = json.dumps({
    "type": "record",
    "name": "WarehouseEvent",
    "namespace": "com.warehouse",
    "doc": "V1 - warehouse event schema",
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
        }], "default": None}
    ]
})

SCHEMA_V2 = json.dumps({
    "type": "record",
    "name": "WarehouseEvent",
    "namespace": "com.warehouse",
    "doc": "V2 - adds supplier_id for PRODUCT_RECEIVED",
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
        {"name": "supplier_id", "type": ["null","string"], "default": None,
         "doc": "V2: supplier identifier for PRODUCT_RECEIVED events"}
    ]
})


def wait_for_schema_registry(url: str, retries: int = 30, delay: int = 5) -> None:
    for attempt in range(retries):
        try:
            r = requests.get(f"{url}/subjects", timeout=5)
            if r.status_code == 200:
                logger.info("Schema Registry is ready.")
                return
        except Exception:
            pass
        logger.info("Waiting for Schema Registry... (%d/%d)", attempt + 1, retries)
        time.sleep(delay)
    raise RuntimeError("Schema Registry not available after retries")


def register_schemas(sr_url: str) -> None:
    """Register V1 and V2 schemas with backward compatibility."""
    subject = f"{KAFKA_TOPIC}-value"
    headers = {"Content-Type": "application/vnd.schemaregistry.v1+json"}

    # Set compatibility to BACKWARD
    requests.put(
        f"{sr_url}/config/{subject}",
        json={"compatibility": "BACKWARD"},
        headers=headers,
        timeout=10,
    )

    for version_name, schema_str in [("V1", SCHEMA_V1), ("V2", SCHEMA_V2)]:
        r = requests.post(
            f"{sr_url}/subjects/{subject}/versions",
            json={"schema": schema_str},
            headers=headers,
            timeout=10,
        )
        if r.status_code in (200, 409):
            logger.info("Schema %s registered/already exists (id=%s).", version_name, r.json().get("id"))
        else:
            logger.warning("Schema %s registration returned %d: %s", version_name, r.status_code, r.text)


def now_iso() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat()


def build_event(event_type: str, seq: int, **kwargs) -> dict:
    return {
        "event_id":        str(uuid.uuid4()),
        "event_type":      event_type,
        "timestamp":       now_iso(),
        "sequence_number": seq,
        "product_id":      kwargs.get("product_id"),
        "zone_id":         kwargs.get("zone_id"),
        "from_zone_id":    kwargs.get("from_zone_id"),
        "to_zone_id":      kwargs.get("to_zone_id"),
        "quantity":        kwargs.get("quantity"),
        "order_id":        kwargs.get("order_id"),
        "order_items":     kwargs.get("order_items"),
        "supplier_id":     kwargs.get("supplier_id"),
    }


def run_scenario(producer: Producer, serializer_v1, serializer_v2, ctx) -> None:
    """
    Runs E2E scenarios from the assignment sequentially with pauses.
    Covers all scenarios 1-8.
    """
    seq = int(time.time() * 1000)

    def send(event: dict, use_v2: bool = False):
        ser = serializer_v2 if use_v2 else serializer_v1
        # V1 events don't have supplier_id field; strip it if not using V2
        if not use_v2:
            event.pop("supplier_id", None)
        data = ser(event, ctx)
        producer.produce(KAFKA_TOPIC, value=data, key=event["event_id"].encode())
        producer.flush()
        logger.info("[SENT] event_type=%s event_id=%s", event["event_type"], event["event_id"])

    # ── Scenario 1: Basic warehouse cycle ─────────────────────────────────
    logger.info("=== Scenario 1: Basic warehouse cycle ===")

    e1 = build_event("PRODUCT_RECEIVED", seq, product_id="SKU-001", zone_id="ZONE-A", quantity=100)
    send(e1); seq += 1; time.sleep(1)

    e2 = build_event("PRODUCT_RESERVED", seq, product_id="SKU-001", zone_id="ZONE-A", quantity=30)
    send(e2); seq += 1; time.sleep(1)

    e3 = build_event("PRODUCT_MOVED", seq, product_id="SKU-001",
                     from_zone_id="ZONE-A", to_zone_id="ZONE-B", quantity=20)
    send(e3); seq += 1; time.sleep(1)

    e4 = build_event("PRODUCT_SHIPPED", seq, product_id="SKU-001", zone_id="ZONE-A", quantity=10)
    send(e4); seq += 1; time.sleep(1)

    order_id = str(uuid.uuid4())
    e5 = build_event("ORDER_CREATED", seq, order_id=order_id,
                     order_items=[{"product_id": "SKU-001", "zone_id": "ZONE-A", "quantity": 15}])
    send(e5); seq += 1; time.sleep(1)

    e6 = build_event("ORDER_COMPLETED", seq, order_id=order_id)
    send(e6); seq += 1; time.sleep(2)

    # ── Scenario 2: Idempotency ────────────────────────────────────────────
    logger.info("=== Scenario 2: Idempotency test ===")
    e7 = build_event("PRODUCT_RECEIVED", seq, product_id="SKU-002", zone_id="ZONE-A", quantity=50)
    send(e7); seq += 1; time.sleep(1)
    # Send the same event again (same event_id) — should be deduplicated
    e7_dup = dict(e7)  # same event_id!
    send(e7_dup); time.sleep(2)

    # ── Scenario 3: Out-of-order events ───────────────────────────────────
    logger.info("=== Scenario 4: Out-of-order events ===")
    from datetime import datetime, timezone, timedelta
    base_time = datetime.now(timezone.utc)
    ts_1200 = (base_time - timedelta(minutes=5)).isoformat()
    ts_1205 = (base_time - timedelta(minutes=0)).isoformat()
    ts_1202 = (base_time - timedelta(minutes=3)).isoformat()  # older than ts_1205

    e_recv = build_event("PRODUCT_RECEIVED", seq,
                         product_id="SKU-004", zone_id="ZONE-A", quantity=100)
    e_recv["timestamp"] = ts_1200
    send(e_recv); seq += 1; time.sleep(0.5)

    e_ship = build_event("PRODUCT_SHIPPED", seq,
                         product_id="SKU-004", zone_id="ZONE-A", quantity=20)
    e_ship["timestamp"] = ts_1205
    send(e_ship); seq += 1; time.sleep(0.5)

    # Old event — should be ignored
    e_old = build_event("PRODUCT_RECEIVED", seq,
                        product_id="SKU-004", zone_id="ZONE-A", quantity=50)
    e_old["timestamp"] = ts_1202
    send(e_old); seq += 1; time.sleep(2)

    # ── Scenario 5: DLQ — invalid event ──────────────────────────────────
    logger.info("=== Scenario 5: DLQ invalid event ===")
    e_bad = build_event("PRODUCT_SHIPPED", seq, product_id="SKU-005",
                        zone_id="ZONE-A", quantity=-5)  # invalid quantity
    send(e_bad); seq += 1; time.sleep(1)

    # Valid event after invalid — should be processed
    e_good = build_event("PRODUCT_RECEIVED", seq, product_id="SKU-005",
                         zone_id="ZONE-A", quantity=100)
    send(e_good); seq += 1; time.sleep(2)

    # ── Scenario 6: Cassandra cluster / SKU-006 ───────────────────────────
    logger.info("=== Scenario 6: Cassandra cluster events ===")
    e8 = build_event("PRODUCT_RECEIVED", seq, product_id="SKU-006", zone_id="ZONE-A", quantity=200)
    send(e8); seq += 1; time.sleep(1)

    e9 = build_event("PRODUCT_SHIPPED", seq, product_id="SKU-006", zone_id="ZONE-A", quantity=50)
    send(e9); seq += 1; time.sleep(2)

    # ── Scenario 8: Schema Evolution V1 vs V2 ────────────────────────────
    logger.info("=== Scenario 8: Schema Evolution V1 / V2 ===")
    e_v1 = build_event("PRODUCT_RECEIVED", seq, product_id="SKU-007",
                       zone_id="ZONE-C", quantity=300)
    e_v1.pop("supplier_id", None)
    send(e_v1, use_v2=False); seq += 1; time.sleep(1)

    e_v2 = build_event("PRODUCT_RECEIVED", seq, product_id="SKU-007",
                       zone_id="ZONE-C", quantity=50, supplier_id="SUP-001")
    send(e_v2, use_v2=True); seq += 1; time.sleep(1)

    logger.info("All scenarios dispatched. Sleeping 30s before next cycle...")
    time.sleep(30)


def main():
    wait_for_schema_registry(SCHEMA_REGISTRY_URL)
    register_schemas(SCHEMA_REGISTRY_URL)

    sr_conf = {"url": SCHEMA_REGISTRY_URL}
    sr_client = SchemaRegistryClient(sr_conf)

    from confluent_kafka.schema_registry import Schema
    schema_v1_obj = Schema(SCHEMA_V1, schema_type="AVRO")
    schema_v2_obj = Schema(SCHEMA_V2, schema_type="AVRO")

    serializer_v1 = AvroSerializer(sr_client, SCHEMA_V1)
    serializer_v2 = AvroSerializer(sr_client, SCHEMA_V2)

    producer_conf = {"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS}
    producer = Producer(producer_conf)

    ctx = SerializationContext(KAFKA_TOPIC, MessageField.VALUE)

    logger.info("Producer started. Producing events to topic '%s'.", KAFKA_TOPIC)
    while True:
        try:
            run_scenario(producer, serializer_v1, serializer_v2, ctx)
        except Exception as exc:
            logger.error("Scenario error: %s", exc, exc_info=True)
            time.sleep(10)


if __name__ == "__main__":
    main()
