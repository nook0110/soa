-- ============================================================
-- Warehouse keyspace — NetworkTopologyStrategy, RF=3
-- ============================================================
CREATE KEYSPACE IF NOT EXISTS warehouse
  WITH replication = {
    'class':              'NetworkTopologyStrategy',
    'datacenter1':        3
  }
  AND durable_writes = true;

USE warehouse;

-- ============================================================
-- Table 1: inventory_by_product_zone
-- Query: SELECT * FROM inventory_by_product_zone WHERE product_id = ? AND zone_id = ?
--
-- Partition key: product_id — groups all zones for a product together.
-- Clustering key: zone_id — allows filtering to a specific zone.
-- This supports both the "product in zone" and "all zones for product" queries.
-- ============================================================
CREATE TABLE IF NOT EXISTS inventory_by_product_zone (
    product_id          text,
    zone_id             text,
    available_quantity  bigint,
    reserved_quantity   bigint,
    last_updated        timestamp,
    last_sequence       bigint,
    supplier_id         text,   -- V2 schema field
    PRIMARY KEY (product_id, zone_id)
) WITH CLUSTERING ORDER BY (zone_id ASC)
  AND comment = 'Inventory per product per zone. PK=product_id, CK=zone_id';

-- ============================================================
-- Table 2: inventory_by_zone
-- Query: SELECT * FROM inventory_by_zone WHERE zone_id = ?
--
-- Partition key: zone_id — all products in a zone on same partition node.
-- Clustering key: product_id — sorted product list within a zone.
-- ============================================================
CREATE TABLE IF NOT EXISTS inventory_by_zone (
    zone_id             text,
    product_id          text,
    available_quantity  bigint,
    reserved_quantity   bigint,
    last_updated        timestamp,
    last_sequence       bigint,
    PRIMARY KEY (zone_id, product_id)
) WITH CLUSTERING ORDER BY (product_id ASC)
  AND comment = 'Inventory per zone. PK=zone_id, CK=product_id';

-- ============================================================
-- Table 3: inventory_by_product  (aggregate across all zones)
-- Query: SELECT * FROM inventory_by_product WHERE product_id = ?
--
-- Partition key: product_id — all aggregate data on one partition.
-- No clustering key needed (one row per product).
-- Denormalized: total_available / total_reserved aggregated from zone-level events.
-- ============================================================
CREATE TABLE IF NOT EXISTS inventory_by_product (
    product_id          text,
    total_available     bigint,
    total_reserved      bigint,
    last_updated        timestamp,
    PRIMARY KEY (product_id)
) WITH comment = 'Aggregate inventory per product (all zones). PK=product_id';

-- ============================================================
-- Table 4: orders
-- Query: SELECT * FROM orders WHERE order_id = ?
-- ============================================================
CREATE TABLE IF NOT EXISTS orders (
    order_id    text,
    status      text,   -- CREATED | COMPLETED
    created_at  timestamp,
    updated_at  timestamp,
    PRIMARY KEY (order_id)
) WITH comment = 'Order records. PK=order_id';

-- ============================================================
-- Table 5: order_items
-- Query: SELECT * FROM order_items WHERE order_id = ?
-- ============================================================
CREATE TABLE IF NOT EXISTS order_items (
    order_id    text,
    product_id  text,
    zone_id     text,
    quantity    int,
    PRIMARY KEY (order_id, product_id, zone_id)
) WITH comment = 'Order line items. PK=order_id, CK=(product_id, zone_id)';

-- ============================================================
-- Table 6: processed_events  (idempotency deduplication)
-- Query: SELECT * FROM processed_events WHERE event_id = ?
--
-- Partition key: event_id — O(1) lookup.
-- TTL of 7 days (604800s) to avoid unbounded growth.
-- ============================================================
CREATE TABLE IF NOT EXISTS processed_events (
    event_id    text,
    processed_at timestamp,
    PRIMARY KEY (event_id)
) WITH default_time_to_live = 604800
  AND comment = 'Idempotency log. PK=event_id. TTL=7d';

-- ============================================================
-- Table 7: event_history  (audit log)
-- Query: SELECT * FROM event_history WHERE product_id = ? [AND event_time > ?]
--
-- Partition key: product_id — audit trail per product.
-- Clustering key: event_time DESC + event_id — newest events first.
-- ============================================================
CREATE TABLE IF NOT EXISTS event_history (
    product_id  text,
    event_time  timestamp,
    event_id    text,
    event_type  text,
    zone_id     text,
    quantity    int,
    raw_payload text,
    PRIMARY KEY (product_id, event_time, event_id)
) WITH CLUSTERING ORDER BY (event_time DESC, event_id ASC)
  AND comment = 'Audit event history per product. PK=product_id, CK=(event_time DESC, event_id)';

-- ============================================================
-- Table 8: zone_sequence  (out-of-order tracking)
-- Stores the last-processed timestamp per (product_id, zone_id).
-- Used in point 6 to reject stale events.
-- ============================================================
CREATE TABLE IF NOT EXISTS zone_sequence (
    product_id      text,
    zone_id         text,
    last_event_time timestamp,
    PRIMARY KEY (product_id, zone_id)
) WITH comment = 'Last processed event timestamp per product+zone for out-of-order rejection';
