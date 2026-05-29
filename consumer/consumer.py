"""
Kafka Consumer Service — core loop.

Implements:
  - Point 1: consumer group, at-least-once semantics (offset committed after
    successful Cassandra write)
  - Point 2/3: Avro deserialization + event handling to Cassandra
  - Point 4: idempotency (deduplication by event_id)
  - Point 5: logged batch for atomic multi-table writes
  - Point 6: out-of-order rejection (timestamp-based)
  - Point 7: Dead Letter Queue (DLQ) on error
  - Point 9: Prometheus metrics (consumer lag, throughput, durations, errors)
"""

import json
import logging
import os
import time
from datetime import datetime, timezone

from confluent_kafka import Consumer, Producer, KafkaError, KafkaException, TopicPartition
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

import db
import handlers
import metrics as m

logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
SCHEMA_REGISTRY_URL     = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
KAFKA_TOPIC             = os.getenv("KAFKA_TOPIC", "warehouse-events")
KAFKA_DLQ_TOPIC         = os.getenv("KAFKA_DLQ_TOPIC", "warehouse-events-dlq")
KAFKA_GROUP_ID          = os.getenv("KAFKA_GROUP_ID", "warehouse-state-consumer")

# Shared state for health checks
_kafka_connected = False
_cassandra_connected = False


def _build_consumer() -> Consumer:
    conf = {
        "bootstrap.servers":        KAFKA_BOOTSTRAP_SERVERS,
        "group.id":                 KAFKA_GROUP_ID,
        "auto.offset.reset":        "earliest",
        # Disable auto-commit — we commit manually after successful Cassandra write
        "enable.auto.commit":       False,
        "session.timeout.ms":       30000,
        "max.poll.interval.ms":     300000,
    }
    return Consumer(conf)


def _build_producer() -> Producer:
    return Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})


def _build_deserializer() -> AvroDeserializer:
    sr_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    return AvroDeserializer(sr_client)


def _send_to_dlq(producer: Producer, original_msg, error_reason: str,
                 error_code: str) -> None:
    """Send a failed event to the DLQ topic (point 7)."""
    try:
        dlq_payload = json.dumps({
            "original_event": original_msg.value().decode("utf-8", errors="replace")
                              if original_msg.value() else None,
            "error_reason":  error_reason,
            "error_code":    error_code,
            "failed_at":     datetime.now(timezone.utc).isoformat(),
            "kafka_metadata": {
                "partition": original_msg.partition(),
                "offset":    original_msg.offset(),
                "topic":     original_msg.topic(),
            },
        }, ensure_ascii=False)
        producer.produce(KAFKA_DLQ_TOPIC, value=dlq_payload.encode())
        producer.flush(timeout=5)
        logger.info("[DLQ] Event sent to %s | reason=%s", KAFKA_DLQ_TOPIC, error_reason)
        m.dlq_events_total.labels(reason=error_code).inc()
    except Exception as exc:
        logger.error("[DLQ] Failed to send to DLQ: %s", exc)


def _update_lag_metrics(consumer: Consumer) -> None:
    """Compute and expose consumer lag per partition (point 9)."""
    try:
        assignment = consumer.assignment()
        if not assignment:
            return
        for tp in assignment:
            _, high = consumer.get_watermark_offsets(tp, timeout=1.0)
            committed_list = consumer.committed([tp], timeout=1.0)
            committed_offset = committed_list[0].offset if committed_list and committed_list[0].offset >= 0 else 0
            lag = max(0, high - committed_offset)
            m.consumer_lag.labels(topic=tp.topic, partition=str(tp.partition)).set(lag)
    except Exception as exc:
        logger.debug("Lag metric update error: %s", exc)


def run_consumer():
    global _kafka_connected, _cassandra_connected

    # Wait for Cassandra
    logger.info("Initialising Cassandra connection...")
    session = db.get_session()
    _cassandra_connected = True
    logger.info("Cassandra ready.")

    # Build Kafka consumer, producer (DLQ), deserializer
    consumer = _build_consumer()
    dlq_producer = _build_producer()
    deserializer = _build_deserializer()
    ctx = SerializationContext(KAFKA_TOPIC, MessageField.VALUE)

    consumer.subscribe([KAFKA_TOPIC])
    _kafka_connected = True
    logger.info("Subscribed to topic '%s' with group '%s'.", KAFKA_TOPIC, KAFKA_GROUP_ID)

    lag_update_counter = 0

    while True:
        try:
            msg = consumer.poll(timeout=1.0)

            # Update lag metrics every ~10 polls
            lag_update_counter += 1
            if lag_update_counter % 10 == 0:
                _update_lag_metrics(consumer)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logger.error("Kafka error: %s", msg.error())
                _kafka_connected = False
                time.sleep(2)
                _kafka_connected = True
                continue

            # ── Deserialize ──────────────────────────────────────────────
            try:
                event = deserializer(msg.value(), ctx)
            except Exception as exc:
                logger.error("[DESER] Deserialization failed: %s", exc)
                _send_to_dlq(dlq_producer, msg, str(exc), "DESERIALIZATION_ERROR")
                consumer.commit(message=msg)
                continue

            if event is None:
                consumer.commit(message=msg)
                continue

            event_id   = event.get("event_id", "UNKNOWN")
            event_type = event.get("event_type", "UNKNOWN")
            partition  = msg.partition()
            offset     = msg.offset()

            logger.info("[EVENT] event_id=%s event_type=%s partition=%d offset=%d",
                        event_id, event_type, partition, offset)

            # ── Idempotency check (point 4) ───────────────────────────────
            if db.is_already_processed(event_id):
                logger.info("[DEDUP] Skipping duplicate event_id=%s", event_id)
                consumer.commit(message=msg)
                continue

            # ── Process event ─────────────────────────────────────────────
            start = time.monotonic()
            try:
                result = handlers.dispatch(event)
            except ValueError as exc:
                # Business validation error — send to DLQ
                logger.error("[VALIDATION] event_id=%s error=%s", event_id, exc)
                _send_to_dlq(dlq_producer, msg, str(exc), "VALIDATION_ERROR")
                consumer.commit(message=msg)
                m.events_processed_total.labels(event_type=event_type).inc()
                continue
            except Exception as exc:
                logger.error("[HANDLER] Unexpected error for event_id=%s: %s", event_id, exc, exc_info=True)
                _send_to_dlq(dlq_producer, msg, str(exc), "PROCESSING_ERROR")
                consumer.commit(message=msg)
                continue

            elapsed = time.monotonic() - start
            m.event_processing_duration_seconds.observe(elapsed)

            # result may be: None (skipped), a BatchStatement, or a list of BatchStatements
            if result is None:
                # Out-of-order skip — commit offset so we don't reprocess
                consumer.commit(message=msg)
                continue

            batches = result if isinstance(result, list) else [result]

            # ── Execute batch(es) in Cassandra (points 3, 5) ─────────────
            try:
                for batch in batches:
                    if batch is not None:
                        db.execute_batch(batch)
            except Exception as exc:
                logger.error("[CASSANDRA] Write failed for event_id=%s: %s", event_id, exc, exc_info=True)
                # Do NOT commit offset — will be retried (at-least-once)
                time.sleep(1)
                continue

            # ── Commit offset AFTER successful write (point 1) ───────────
            consumer.commit(message=msg)
            m.events_processed_total.labels(event_type=event_type).inc()
            logger.info("[DONE] event_id=%s event_type=%s partition=%d offset=%d elapsed=%.3fs",
                        event_id, event_type, partition, offset, elapsed)

        except KafkaException as exc:
            logger.error("KafkaException: %s", exc)
            _kafka_connected = False
            time.sleep(5)
            _kafka_connected = True
        except Exception as exc:
            logger.error("Unhandled exception in consumer loop: %s", exc, exc_info=True)
            time.sleep(2)


def is_kafka_connected() -> bool:
    return _kafka_connected


def is_cassandra_connected() -> bool:
    return _cassandra_connected
