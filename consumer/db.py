"""
Cassandra connection management and query helpers.
Uses QUORUM consistency for writes (point 8) and ONE for reads (trade-off: speed).
QUORUM writes ensure data is durable on majority of nodes (2 of 3).
ONE reads are acceptable because our use-case is event-driven state updates
where the consumer itself is the source of truth — stale reads for monitoring
are acceptable; correctness is ensured by the write path.
"""

import os
import time
import logging
from datetime import datetime, timezone
from typing import Optional, List

from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import DCAwareRoundRobinPolicy, RetryPolicy
from cassandra.query import ConsistencyLevel, BatchStatement, BatchType
from cassandra import WriteTimeout, WriteFailure, ReadTimeout, ReadFailure, Unavailable

from metrics import cassandra_write_errors_total

logger = logging.getLogger(__name__)

CASSANDRA_HOSTS = os.getenv("CASSANDRA_HOSTS", "localhost").split(",")
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "warehouse")

# Global session singleton
_cluster = None
_session = None


def get_session():
    global _cluster, _session
    if _session is not None:
        return _session

    write_profile = ExecutionProfile(
        consistency_level=ConsistencyLevel.QUORUM,
        load_balancing_policy=DCAwareRoundRobinPolicy(local_dc="datacenter1"),
        retry_policy=RetryPolicy(),
    )
    read_profile = ExecutionProfile(
        consistency_level=ConsistencyLevel.ONE,
        load_balancing_policy=DCAwareRoundRobinPolicy(local_dc="datacenter1"),
        retry_policy=RetryPolicy(),
    )

    retries = 30
    for attempt in range(retries):
        try:
            _cluster = Cluster(
                CASSANDRA_HOSTS,
                execution_profiles={
                    EXEC_PROFILE_DEFAULT: write_profile,
                    "read": read_profile,
                },
            )
            _session = _cluster.connect(CASSANDRA_KEYSPACE)
            logger.info("Connected to Cassandra (keyspace=%s, nodes=%s).",
                        CASSANDRA_KEYSPACE, CASSANDRA_HOSTS)
            _prepare_statements()
            return _session
        except Exception as exc:
            logger.warning("Cassandra not ready (%d/%d): %s", attempt + 1, retries, exc)
            time.sleep(5)

    raise RuntimeError("Could not connect to Cassandra after retries")


# ──────────────────────────────────────────────────────────────────────────
# Prepared statements (compiled once, reused)
# ──────────────────────────────────────────────────────────────────────────
_stmts = {}


def _prepare_statements():
    s = _session

    _stmts["check_idempotent"] = s.prepare(
        "SELECT event_id FROM processed_events WHERE event_id = ?"
    )
    _stmts["check_idempotent"].consistency_level = ConsistencyLevel.ONE

    _stmts["mark_processed"] = s.prepare(
        "INSERT INTO processed_events (event_id, processed_at) VALUES (?, ?) USING TTL 604800"
    )

    # inventory_by_product_zone
    _stmts["upsert_inv_pz"] = s.prepare("""
        UPDATE inventory_by_product_zone
        SET available_quantity = ?,
            reserved_quantity  = ?,
            last_updated       = ?,
            last_sequence      = ?,
            supplier_id        = ?
        WHERE product_id = ? AND zone_id = ?
    """)
    _stmts["get_inv_pz"] = s.prepare(
        "SELECT * FROM inventory_by_product_zone WHERE product_id = ? AND zone_id = ?"
    )
    _stmts["get_inv_pz"].consistency_level = ConsistencyLevel.ONE

    # inventory_by_zone
    _stmts["upsert_inv_z"] = s.prepare("""
        UPDATE inventory_by_zone
        SET available_quantity = ?,
            reserved_quantity  = ?,
            last_updated       = ?,
            last_sequence      = ?
        WHERE zone_id = ? AND product_id = ?
    """)

    # inventory_by_product (aggregate)
    _stmts["get_inv_p"] = s.prepare(
        "SELECT * FROM inventory_by_product WHERE product_id = ?"
    )
    _stmts["get_inv_p"].consistency_level = ConsistencyLevel.ONE

    _stmts["upsert_inv_p"] = s.prepare("""
        UPDATE inventory_by_product
        SET total_available = ?,
            total_reserved  = ?,
            last_updated    = ?
        WHERE product_id = ?
    """)

    # orders
    _stmts["insert_order"] = s.prepare(
        "INSERT INTO orders (order_id, status, created_at, updated_at) VALUES (?, ?, ?, ?)"
    )
    _stmts["update_order_status"] = s.prepare(
        "UPDATE orders SET status = ?, updated_at = ? WHERE order_id = ?"
    )
    _stmts["get_order"] = s.prepare(
        "SELECT * FROM orders WHERE order_id = ?"
    )
    _stmts["get_order"].consistency_level = ConsistencyLevel.ONE

    # order_items
    _stmts["insert_order_item"] = s.prepare(
        "INSERT INTO order_items (order_id, product_id, zone_id, quantity) VALUES (?, ?, ?, ?)"
    )
    _stmts["get_order_items"] = s.prepare(
        "SELECT * FROM order_items WHERE order_id = ?"
    )
    _stmts["get_order_items"].consistency_level = ConsistencyLevel.ONE

    # event_history
    _stmts["insert_history"] = s.prepare("""
        INSERT INTO event_history (product_id, event_time, event_id, event_type,
                                   zone_id, quantity, raw_payload)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """)

    # zone_sequence (out-of-order)
    _stmts["get_zone_seq"] = s.prepare(
        "SELECT last_event_time FROM zone_sequence WHERE product_id = ? AND zone_id = ?"
    )
    _stmts["get_zone_seq"].consistency_level = ConsistencyLevel.ONE

    _stmts["upsert_zone_seq"] = s.prepare(
        "UPDATE zone_sequence SET last_event_time = ? WHERE product_id = ? AND zone_id = ?"
    )

    logger.info("All prepared statements ready.")


# ──────────────────────────────────────────────────────────────────────────
# Helper functions
# ──────────────────────────────────────────────────────────────────────────

def is_already_processed(event_id: str) -> bool:
    """Idempotency check (point 4)."""
    rows = _session.execute(_stmts["check_idempotent"], (event_id,))
    return rows.one() is not None


def mark_event_processed(batch: BatchStatement, event_id: str) -> None:
    """Add the idempotency mark to a batch."""
    batch.add(_stmts["mark_processed"], (event_id, datetime.now(timezone.utc)))


def get_inventory(product_id: str, zone_id: str) -> dict:
    row = _session.execute(_stmts["get_inv_pz"], (product_id, zone_id),
                           execution_profile="read").one()
    if row:
        return {
            "available_quantity": row.available_quantity or 0,
            "reserved_quantity":  row.reserved_quantity  or 0,
            "last_sequence":      row.last_sequence      or 0,
        }
    return {"available_quantity": 0, "reserved_quantity": 0, "last_sequence": 0}


def get_aggregate_inventory(product_id: str) -> dict:
    row = _session.execute(_stmts["get_inv_p"], (product_id,),
                           execution_profile="read").one()
    if row:
        return {
            "total_available": row.total_available or 0,
            "total_reserved":  row.total_reserved  or 0,
        }
    return {"total_available": 0, "total_reserved": 0}


def get_zone_last_time(product_id: str, zone_id: str) -> Optional[datetime]:
    """Return the last processed event time for (product, zone). Used for out-of-order rejection."""
    row = _session.execute(_stmts["get_zone_seq"], (product_id, zone_id),
                           execution_profile="read").one()
    return row.last_event_time if row else None


def execute_batch(batch: BatchStatement) -> None:
    """Execute a logged batch with QUORUM consistency and update metrics on failure."""
    try:
        _session.execute(batch)
    except (WriteTimeout, WriteFailure, Unavailable) as exc:
        cassandra_write_errors_total.inc()
        raise


def build_inventory_batch(
    product_id: str,
    zone_id: str,
    available_delta: int,
    reserved_delta: int,
    event_id: str,
    event_time: datetime,
    sequence: int,
    supplier_id: Optional[str] = None,
) -> BatchStatement:
    """
    Build a LOGGED BATCH that atomically updates:
      - inventory_by_product_zone
      - inventory_by_zone
      - inventory_by_product (aggregate)
      - processed_events (idempotency)
      - zone_sequence (out-of-order tracking)
    (Point 5: consistency across denormalized tables)
    """
    inv = get_inventory(product_id, zone_id)
    agg = get_aggregate_inventory(product_id)

    new_avail = inv["available_quantity"] + available_delta
    new_rsvd  = inv["reserved_quantity"]  + reserved_delta
    new_total_avail = agg["total_available"] + available_delta
    new_total_rsvd  = agg["total_reserved"]  + reserved_delta

    batch = BatchStatement(batch_type=BatchType.LOGGED,
                           consistency_level=ConsistencyLevel.QUORUM)

    batch.add(_stmts["upsert_inv_pz"],
              (new_avail, new_rsvd, event_time, sequence, supplier_id, product_id, zone_id))
    batch.add(_stmts["upsert_inv_z"],
              (new_avail, new_rsvd, event_time, sequence, zone_id, product_id))
    batch.add(_stmts["upsert_inv_p"],
              (new_total_avail, new_total_rsvd, event_time, product_id))
    batch.add(_stmts["upsert_zone_seq"],
              (event_time, product_id, zone_id))

    mark_event_processed(batch, event_id)

    return batch


def is_session_alive() -> bool:
    """Quick health check for the /health endpoint."""
    try:
        _session.execute("SELECT now() FROM system.local")
        return True
    except Exception:
        return False
