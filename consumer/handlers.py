"""
Event handlers — one function per event type.
Each handler returns a BatchStatement (logged batch) that the consumer
executes atomically (points 3, 4, 5).
Out-of-order rejection is implemented here (point 6).
DLQ is triggered by raising ValueError for invalid business data (point 7).
"""

import json
import logging
from datetime import datetime, timezone
from typing import Optional

from cassandra.query import BatchStatement, BatchType, ConsistencyLevel

import db

logger = logging.getLogger(__name__)


def _parse_ts(ts_str: str) -> datetime:
    """Parse ISO-8601 timestamp; return UTC datetime."""
    try:
        dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return datetime.now(timezone.utc)


def _check_out_of_order(product_id: str, zone_id: str, event_time: datetime) -> bool:
    """
    Return True if this event is stale (older than the last processed event for
    the same product+zone combination).  (Point 6)
    """
    last = db.get_zone_last_time(product_id, zone_id)
    if last is not None:
        # Ensure both are tz-aware for comparison
        if last.tzinfo is None:
            last = last.replace(tzinfo=timezone.utc)
        if event_time <= last:
            return True  # stale — ignore
    return False


# ──────────────────────────────────────────────────────────────────────────
# PRODUCT_RECEIVED
# ──────────────────────────────────────────────────────────────────────────
def handle_product_received(event: dict) -> BatchStatement:
    product_id  = event["product_id"]
    zone_id     = event["zone_id"]
    quantity    = event["quantity"]
    event_id    = event["event_id"]
    event_time  = _parse_ts(event["timestamp"])
    sequence    = event.get("sequence_number", 0)
    supplier_id = event.get("supplier_id")  # V2 field (may be None)

    if quantity is None or quantity <= 0:
        raise ValueError(f"Invalid quantity: {quantity} (must be positive)")

    if _check_out_of_order(product_id, zone_id, event_time):
        logger.warning("[OUT-OF-ORDER] PRODUCT_RECEIVED ignored: event_id=%s event_time=%s",
                       event_id, event_time)
        return None  # signal: skip but don't DLQ

    batch = db.build_inventory_batch(
        product_id=product_id, zone_id=zone_id,
        available_delta=quantity, reserved_delta=0,
        event_id=event_id, event_time=event_time,
        sequence=sequence, supplier_id=supplier_id,
    )

    # Audit history
    batch.add(db._stmts["insert_history"],
              (product_id, event_time, event_id, "PRODUCT_RECEIVED",
               zone_id, quantity, json.dumps(event, default=str)))
    return batch


# ──────────────────────────────────────────────────────────────────────────
# PRODUCT_SHIPPED
# ──────────────────────────────────────────────────────────────────────────
def handle_product_shipped(event: dict) -> BatchStatement:
    product_id = event["product_id"]
    zone_id    = event["zone_id"]
    quantity   = event["quantity"]
    event_id   = event["event_id"]
    event_time = _parse_ts(event["timestamp"])
    sequence   = event.get("sequence_number", 0)

    if quantity is None or quantity <= 0:
        raise ValueError(f"Invalid quantity: {quantity} (must be positive)")

    if _check_out_of_order(product_id, zone_id, event_time):
        logger.warning("[OUT-OF-ORDER] PRODUCT_SHIPPED ignored: event_id=%s", event_id)
        return None

    batch = db.build_inventory_batch(
        product_id=product_id, zone_id=zone_id,
        available_delta=-quantity, reserved_delta=0,
        event_id=event_id, event_time=event_time, sequence=sequence,
    )
    batch.add(db._stmts["insert_history"],
              (product_id, event_time, event_id, "PRODUCT_SHIPPED",
               zone_id, quantity, json.dumps(event, default=str)))
    return batch


# ──────────────────────────────────────────────────────────────────────────
# PRODUCT_MOVED  (two-zone update — two separate batches executed sequentially)
# ──────────────────────────────────────────────────────────────────────────
def handle_product_moved(event: dict) -> list:
    product_id  = event["product_id"]
    from_zone   = event["from_zone_id"]
    to_zone     = event["to_zone_id"]
    quantity    = event["quantity"]
    event_id    = event["event_id"]
    event_time  = _parse_ts(event["timestamp"])
    sequence    = event.get("sequence_number", 0)

    if quantity is None or quantity <= 0:
        raise ValueError(f"Invalid quantity: {quantity} (must be positive)")

    if _check_out_of_order(product_id, from_zone, event_time):
        logger.warning("[OUT-OF-ORDER] PRODUCT_MOVED ignored: event_id=%s", event_id)
        return []

    batch_from = db.build_inventory_batch(
        product_id=product_id, zone_id=from_zone,
        available_delta=-quantity, reserved_delta=0,
        event_id=event_id + "_from", event_time=event_time, sequence=sequence,
    )
    batch_to = db.build_inventory_batch(
        product_id=product_id, zone_id=to_zone,
        available_delta=quantity, reserved_delta=0,
        event_id=event_id + "_to", event_time=event_time, sequence=sequence,
    )
    # Audit once
    batch_from.add(db._stmts["insert_history"],
                   (product_id, event_time, event_id, "PRODUCT_MOVED",
                    from_zone, quantity, json.dumps(event, default=str)))
    return [batch_from, batch_to]


# ──────────────────────────────────────────────────────────────────────────
# PRODUCT_RESERVED
# ──────────────────────────────────────────────────────────────────────────
def handle_product_reserved(event: dict) -> BatchStatement:
    product_id = event["product_id"]
    zone_id    = event["zone_id"]
    quantity   = event["quantity"]
    event_id   = event["event_id"]
    event_time = _parse_ts(event["timestamp"])
    sequence   = event.get("sequence_number", 0)

    if quantity is None or quantity <= 0:
        raise ValueError(f"Invalid quantity: {quantity} (must be positive)")

    if _check_out_of_order(product_id, zone_id, event_time):
        logger.warning("[OUT-OF-ORDER] PRODUCT_RESERVED ignored: event_id=%s", event_id)
        return None

    batch = db.build_inventory_batch(
        product_id=product_id, zone_id=zone_id,
        available_delta=-quantity, reserved_delta=quantity,
        event_id=event_id, event_time=event_time, sequence=sequence,
    )
    batch.add(db._stmts["insert_history"],
              (product_id, event_time, event_id, "PRODUCT_RESERVED",
               zone_id, quantity, json.dumps(event, default=str)))
    return batch


# ──────────────────────────────────────────────────────────────────────────
# PRODUCT_RELEASED
# ──────────────────────────────────────────────────────────────────────────
def handle_product_released(event: dict) -> BatchStatement:
    product_id = event["product_id"]
    zone_id    = event["zone_id"]
    quantity   = event["quantity"]
    event_id   = event["event_id"]
    event_time = _parse_ts(event["timestamp"])
    sequence   = event.get("sequence_number", 0)

    if quantity is None or quantity <= 0:
        raise ValueError(f"Invalid quantity: {quantity} (must be positive)")

    if _check_out_of_order(product_id, zone_id, event_time):
        logger.warning("[OUT-OF-ORDER] PRODUCT_RELEASED ignored: event_id=%s", event_id)
        return None

    batch = db.build_inventory_batch(
        product_id=product_id, zone_id=zone_id,
        available_delta=quantity, reserved_delta=-quantity,
        event_id=event_id, event_time=event_time, sequence=sequence,
    )
    batch.add(db._stmts["insert_history"],
              (product_id, event_time, event_id, "PRODUCT_RELEASED",
               zone_id, quantity, json.dumps(event, default=str)))
    return batch


# ──────────────────────────────────────────────────────────────────────────
# INVENTORY_COUNTED  (absolute overwrite — uses UPDATE with explicit values)
# ──────────────────────────────────────────────────────────────────────────
def handle_inventory_counted(event: dict) -> BatchStatement:
    product_id    = event["product_id"]
    zone_id       = event["zone_id"]
    counted_qty   = event["quantity"]
    event_id      = event["event_id"]
    event_time    = _parse_ts(event["timestamp"])
    sequence      = event.get("sequence_number", 0)

    if counted_qty is None or counted_qty < 0:
        raise ValueError(f"Invalid counted_quantity: {counted_qty}")

    if _check_out_of_order(product_id, zone_id, event_time):
        logger.warning("[OUT-OF-ORDER] INVENTORY_COUNTED ignored: event_id=%s", event_id)
        return None

    session = db._session

    # Read current values to compute aggregate delta
    inv = db.get_inventory(product_id, zone_id)
    current_avail = inv["available_quantity"]
    avail_delta   = counted_qty - current_avail
    agg           = db.get_aggregate_inventory(product_id)

    batch = BatchStatement(batch_type=BatchType.LOGGED,
                           consistency_level=ConsistencyLevel.QUORUM)

    # Set absolute values directly
    batch.add(db._stmts["upsert_inv_pz"],
              (counted_qty, inv["reserved_quantity"], event_time, sequence, None, product_id, zone_id))
    batch.add(db._stmts["upsert_inv_z"],
              (counted_qty, inv["reserved_quantity"], event_time, sequence, zone_id, product_id))
    batch.add(db._stmts["upsert_inv_p"],
              (agg["total_available"] + avail_delta, agg["total_reserved"], event_time, product_id))
    batch.add(db._stmts["upsert_zone_seq"],
              (event_time, product_id, zone_id))
    db.mark_event_processed(batch, event_id)

    batch.add(db._stmts["insert_history"],
              (product_id, event_time, event_id, "INVENTORY_COUNTED",
               zone_id, counted_qty, json.dumps(event, default=str)))
    return batch


# ──────────────────────────────────────────────────────────────────────────
# ORDER_CREATED
# ──────────────────────────────────────────────────────────────────────────
def handle_order_created(event: dict) -> list:
    order_id   = event["order_id"]
    items      = event.get("order_items") or []
    event_id   = event["event_id"]
    event_time = _parse_ts(event["timestamp"])

    if not order_id:
        raise ValueError("order_id is required for ORDER_CREATED")
    if not items:
        raise ValueError("order_items cannot be empty for ORDER_CREATED")

    batches = []
    now = event_time

    # 1. Create order record
    order_batch = BatchStatement(batch_type=BatchType.LOGGED,
                                 consistency_level=ConsistencyLevel.QUORUM)
    order_batch.add(db._stmts["insert_order"], (order_id, "CREATED", now, now))

    for item in items:
        pid = item["product_id"]
        zid = item["zone_id"]
        qty = item["quantity"]
        order_batch.add(db._stmts["insert_order_item"], (order_id, pid, zid, qty))

    db.mark_event_processed(order_batch, event_id)
    batches.append(order_batch)

    # 2. Reserve inventory for each item
    for i, item in enumerate(items):
        pid = item["product_id"]
        zid = item["zone_id"]
        qty = item["quantity"]

        inv_batch = db.build_inventory_batch(
            product_id=pid, zone_id=zid,
            available_delta=-qty, reserved_delta=qty,
            event_id=event_id + f"_item_{i}",
            event_time=now, sequence=event.get("sequence_number", 0),
        )
        batches.append(inv_batch)

    return batches


# ──────────────────────────────────────────────────────────────────────────
# ORDER_COMPLETED
# ──────────────────────────────────────────────────────────────────────────
def handle_order_completed(event: dict) -> list:
    order_id   = event["order_id"]
    event_id   = event["event_id"]
    event_time = _parse_ts(event["timestamp"])

    if not order_id:
        raise ValueError("order_id is required for ORDER_COMPLETED")

    # Look up order items
    items = list(db._session.execute(db._stmts["get_order_items"], (order_id,),
                                     execution_profile="read"))

    batches = []

    # Update order status
    status_batch = BatchStatement(batch_type=BatchType.LOGGED,
                                  consistency_level=ConsistencyLevel.QUORUM)
    status_batch.add(db._stmts["update_order_status"],
                     ("COMPLETED", event_time, order_id))
    db.mark_event_processed(status_batch, event_id)
    batches.append(status_batch)

    # Ship reserved items: reserved_quantity -= qty, available unchanged
    for i, item in enumerate(items):
        pid = item.product_id
        zid = item.zone_id
        qty = item.quantity

        inv = db.get_inventory(pid, zid)
        new_rsvd  = inv["reserved_quantity"] - qty
        new_avail = inv["available_quantity"]   # no change
        agg       = db.get_aggregate_inventory(pid)

        inv_batch = BatchStatement(batch_type=BatchType.LOGGED,
                                   consistency_level=ConsistencyLevel.QUORUM)
        inv_batch.add(db._stmts["upsert_inv_pz"],
                      (new_avail, new_rsvd, event_time,
                       event.get("sequence_number", 0), None, pid, zid))
        inv_batch.add(db._stmts["upsert_inv_z"],
                      (new_avail, new_rsvd, event_time,
                       event.get("sequence_number", 0), zid, pid))
        inv_batch.add(db._stmts["upsert_inv_p"],
                      (agg["total_available"], agg["total_reserved"] - qty, event_time, pid))
        inv_batch.add(db._stmts["upsert_zone_seq"],
                      (event_time, pid, zid))
        batches.append(inv_batch)

    return batches


# ──────────────────────────────────────────────────────────────────────────
# Dispatcher
# ──────────────────────────────────────────────────────────────────────────
HANDLERS = {
    "PRODUCT_RECEIVED":  handle_product_received,
    "PRODUCT_SHIPPED":   handle_product_shipped,
    "PRODUCT_MOVED":     handle_product_moved,
    "PRODUCT_RESERVED":  handle_product_reserved,
    "PRODUCT_RELEASED":  handle_product_released,
    "INVENTORY_COUNTED": handle_inventory_counted,
    "ORDER_CREATED":     handle_order_created,
    "ORDER_COMPLETED":   handle_order_completed,
}


def dispatch(event: dict):
    """
    Dispatch the event to the correct handler.
    Returns either None, a BatchStatement, or a list of BatchStatements.
    """
    event_type = event.get("event_type")
    handler = HANDLERS.get(event_type)
    if handler is None:
        raise ValueError(f"Unknown event_type: {event_type}")
    return handler(event)
