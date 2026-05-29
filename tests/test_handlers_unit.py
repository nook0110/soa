"""
Unit tests for consumer/handlers.py.

All Cassandra I/O is mocked so no external services are required.
Run: pytest tests/test_handlers_unit.py -v
"""

import sys
import types
import uuid
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock, patch, call

import pytest

# ── Mock heavy dependencies before importing handlers ─────────────────────
# cassandra
cassandra_mod = types.ModuleType("cassandra")
cassandra_cluster_mod = types.ModuleType("cassandra.cluster")
cassandra_query_mod = types.ModuleType("cassandra.query")
cassandra_policies_mod = types.ModuleType("cassandra.policies")

# BatchType / BatchStatement stubs
class _BatchType:
    LOGGED = "LOGGED"

class _BatchStatement(list):
    def __init__(self, batch_type=None, consistency_level=None):
        super().__init__()
        self.batch_type = batch_type
        self.consistency_level = consistency_level

    def add(self, stmt, params=None):
        self.append((stmt, params))

cassandra_query_mod.BatchStatement = _BatchStatement
cassandra_query_mod.BatchType = _BatchType
cassandra_query_mod.ConsistencyLevel = MagicMock()
cassandra_mod.WriteTimeout = Exception
cassandra_mod.WriteFailure = Exception
cassandra_mod.ReadTimeout = Exception
cassandra_mod.ReadFailure = Exception
cassandra_mod.Unavailable = Exception

sys.modules["cassandra"] = cassandra_mod
sys.modules["cassandra.cluster"] = cassandra_cluster_mod
sys.modules["cassandra.query"] = cassandra_query_mod
sys.modules["cassandra.policies"] = cassandra_policies_mod
sys.modules["cassandra.cluster"].Cluster = MagicMock()
sys.modules["cassandra.cluster"].ExecutionProfile = MagicMock()
sys.modules["cassandra.cluster"].EXEC_PROFILE_DEFAULT = "default"

# confluent_kafka stubs
confluent_mod = types.ModuleType("confluent_kafka")
confluent_mod.Consumer = MagicMock()
confluent_mod.Producer = MagicMock()
confluent_mod.KafkaError = MagicMock()
confluent_mod.KafkaException = Exception
confluent_mod.TopicPartition = MagicMock()
sys.modules["confluent_kafka"] = confluent_mod
sys.modules["confluent_kafka.schema_registry"] = types.ModuleType("confluent_kafka.schema_registry")
sys.modules["confluent_kafka.schema_registry.avro"] = types.ModuleType("confluent_kafka.schema_registry.avro")
sys.modules["confluent_kafka.serialization"] = types.ModuleType("confluent_kafka.serialization")

# prometheus_client stub
prom_mod = types.ModuleType("prometheus_client")
class _FakeMetric:
    def __init__(self, *a, **kw): pass
    def labels(self, **kw): return self
    def inc(self, amount=1): pass
    def observe(self, v): pass
    def set(self, v): pass

prom_mod.Counter = lambda *a, **kw: _FakeMetric()
prom_mod.Histogram = lambda *a, **kw: _FakeMetric()
prom_mod.Gauge = lambda *a, **kw: _FakeMetric()
prom_mod.generate_latest = lambda: b""
prom_mod.CONTENT_TYPE_LATEST = "text/plain"
sys.modules["prometheus_client"] = prom_mod

# fastapi / starlette stubs
fastapi_mod = types.ModuleType("fastapi")
fastapi_mod.FastAPI = MagicMock(return_value=MagicMock())
fastapi_mod.Request = MagicMock()
fastapi_mod.Response = MagicMock()
sys.modules["fastapi"] = fastapi_mod
sys.modules["uvicorn"] = types.ModuleType("uvicorn")

# ── Now import the modules under test ────────────────────────────────────
sys.path.insert(0, "/home/blokhtin/github/soa/consumer")

# We patch db at the module level
import importlib
import metrics  # noqa: F401 — side-effect: populate sys.modules["metrics"]

# Patch db functions used by handlers
db_mock = MagicMock()
db_mock.get_inventory.return_value = {"available_quantity": 100, "reserved_quantity": 0, "last_sequence": 0}
db_mock.get_aggregate_inventory.return_value = {"total_available": 100, "total_reserved": 0}
db_mock.get_zone_last_time.return_value = None  # no out-of-order by default
db_mock.build_inventory_batch.side_effect = lambda **kw: _BatchStatement()
db_mock.mark_event_processed.return_value = None
db_mock._stmts = {
    "insert_history":    MagicMock(),
    "insert_order":      MagicMock(),
    "insert_order_item": MagicMock(),
    "update_order_status": MagicMock(),
    "get_order_items":   MagicMock(),
    "upsert_inv_pz":     MagicMock(),
    "upsert_inv_z":      MagicMock(),
    "upsert_inv_p":      MagicMock(),
    "upsert_zone_seq":   MagicMock(),
}
db_mock._session = MagicMock()
db_mock._session.execute.return_value = iter([])
sys.modules["db"] = db_mock

import handlers  # noqa: E402


# ── Helpers ───────────────────────────────────────────────────────────────
def _ts(dt: datetime = None) -> str:
    return (dt or datetime.now(timezone.utc)).isoformat()


def _event(event_type: str, **kwargs) -> dict:
    base = {
        "event_id":        str(uuid.uuid4()),
        "event_type":      event_type,
        "timestamp":       _ts(),
        "sequence_number": 1,
        "product_id":      "SKU-001",
        "zone_id":         "ZONE-A",
        "from_zone_id":    None,
        "to_zone_id":      None,
        "quantity":        10,
        "order_id":        None,
        "order_items":     None,
        "supplier_id":     None,
    }
    base.update(kwargs)
    return base


# ── PRODUCT_RECEIVED ─────────────────────────────────────────────────────
class TestProductReceived:
    def test_returns_batch_for_valid_event(self):
        db_mock.get_zone_last_time.return_value = None
        evt = _event("PRODUCT_RECEIVED", quantity=50)
        result = handlers.handle_product_received(evt)
        assert result is not None
        db_mock.build_inventory_batch.assert_called_once()

    def test_raises_for_zero_quantity(self):
        evt = _event("PRODUCT_RECEIVED", quantity=0)
        with pytest.raises(ValueError, match="Invalid quantity"):
            handlers.handle_product_received(evt)

    def test_raises_for_negative_quantity(self):
        evt = _event("PRODUCT_RECEIVED", quantity=-1)
        with pytest.raises(ValueError):
            handlers.handle_product_received(evt)

    def test_returns_none_for_out_of_order(self):
        future_time = datetime.now(timezone.utc) + timedelta(hours=1)
        db_mock.get_zone_last_time.return_value = future_time
        evt = _event("PRODUCT_RECEIVED", quantity=10)
        result = handlers.handle_product_received(evt)
        assert result is None
        db_mock.get_zone_last_time.return_value = None  # reset

    def test_supplier_id_passed_v2(self):
        db_mock.get_zone_last_time.return_value = None
        db_mock.build_inventory_batch.reset_mock()
        evt = _event("PRODUCT_RECEIVED", quantity=20, supplier_id="SUP-42")
        handlers.handle_product_received(evt)
        _, kwargs = db_mock.build_inventory_batch.call_args
        assert kwargs.get("supplier_id") == "SUP-42"


# ── PRODUCT_SHIPPED ──────────────────────────────────────────────────────
class TestProductShipped:
    def test_returns_batch(self):
        db_mock.get_zone_last_time.return_value = None
        db_mock.build_inventory_batch.reset_mock()
        evt = _event("PRODUCT_SHIPPED", quantity=5)
        result = handlers.handle_product_shipped(evt)
        assert result is not None
        _, kwargs = db_mock.build_inventory_batch.call_args
        assert kwargs["available_delta"] == -5

    def test_raises_for_negative_quantity(self):
        evt = _event("PRODUCT_SHIPPED", quantity=-3)
        with pytest.raises(ValueError):
            handlers.handle_product_shipped(evt)

    def test_out_of_order_returns_none(self):
        db_mock.get_zone_last_time.return_value = datetime.now(timezone.utc) + timedelta(hours=1)
        evt = _event("PRODUCT_SHIPPED", quantity=5)
        assert handlers.handle_product_shipped(evt) is None
        db_mock.get_zone_last_time.return_value = None


# ── PRODUCT_MOVED ────────────────────────────────────────────────────────
class TestProductMoved:
    def test_returns_two_batches(self):
        db_mock.get_zone_last_time.return_value = None
        db_mock.build_inventory_batch.reset_mock()
        evt = _event("PRODUCT_MOVED", from_zone_id="ZONE-A", to_zone_id="ZONE-B", quantity=15)
        result = handlers.handle_product_moved(evt)
        assert isinstance(result, list)
        assert len(result) == 2

    def test_raises_for_zero_quantity(self):
        evt = _event("PRODUCT_MOVED", from_zone_id="ZONE-A", to_zone_id="ZONE-B", quantity=0)
        with pytest.raises(ValueError):
            handlers.handle_product_moved(evt)

    def test_out_of_order_returns_empty_list(self):
        db_mock.get_zone_last_time.return_value = datetime.now(timezone.utc) + timedelta(hours=1)
        evt = _event("PRODUCT_MOVED", from_zone_id="ZONE-A", to_zone_id="ZONE-B", quantity=5)
        assert handlers.handle_product_moved(evt) == []
        db_mock.get_zone_last_time.return_value = None


# ── PRODUCT_RESERVED ─────────────────────────────────────────────────────
class TestProductReserved:
    def test_reserved_and_available_delta(self):
        db_mock.get_zone_last_time.return_value = None
        db_mock.build_inventory_batch.reset_mock()
        evt = _event("PRODUCT_RESERVED", quantity=7)
        handlers.handle_product_reserved(evt)
        _, kwargs = db_mock.build_inventory_batch.call_args
        assert kwargs["available_delta"] == -7
        assert kwargs["reserved_delta"] == 7

    def test_raises_for_negative_quantity(self):
        evt = _event("PRODUCT_RESERVED", quantity=-1)
        with pytest.raises(ValueError):
            handlers.handle_product_reserved(evt)


# ── PRODUCT_RELEASED ─────────────────────────────────────────────────────
class TestProductReleased:
    def test_release_inverts_reserve(self):
        db_mock.get_zone_last_time.return_value = None
        db_mock.build_inventory_batch.reset_mock()
        evt = _event("PRODUCT_RELEASED", quantity=3)
        handlers.handle_product_released(evt)
        _, kwargs = db_mock.build_inventory_batch.call_args
        assert kwargs["available_delta"] == 3
        assert kwargs["reserved_delta"] == -3


# ── ORDER_CREATED ────────────────────────────────────────────────────────
class TestOrderCreated:
    def test_returns_list_with_order_plus_item_batches(self):
        items = [
            {"product_id": "SKU-001", "zone_id": "ZONE-A", "quantity": 5},
            {"product_id": "SKU-002", "zone_id": "ZONE-B", "quantity": 3},
        ]
        evt = _event("ORDER_CREATED", order_id=str(uuid.uuid4()), order_items=items)
        result = handlers.handle_order_created(evt)
        assert isinstance(result, list)
        # 1 order batch + 2 inventory batches
        assert len(result) == 3

    def test_raises_if_no_order_id(self):
        evt = _event("ORDER_CREATED", order_id=None, order_items=[
            {"product_id": "SKU-001", "zone_id": "ZONE-A", "quantity": 1}
        ])
        with pytest.raises(ValueError, match="order_id"):
            handlers.handle_order_created(evt)

    def test_raises_if_empty_items(self):
        evt = _event("ORDER_CREATED", order_id=str(uuid.uuid4()), order_items=[])
        with pytest.raises(ValueError, match="order_items"):
            handlers.handle_order_created(evt)


# ── DISPATCHER ───────────────────────────────────────────────────────────
class TestDispatcher:
    def test_unknown_event_type_raises(self):
        evt = _event("UNKNOWN_TYPE")
        with pytest.raises(ValueError, match="Unknown event_type"):
            handlers.dispatch(evt)

    @pytest.mark.parametrize("event_type", [
        "PRODUCT_RECEIVED",
        "PRODUCT_SHIPPED",
        "PRODUCT_MOVED",
        "PRODUCT_RESERVED",
        "PRODUCT_RELEASED",
    ])
    def test_known_event_types_dispatch(self, event_type):
        db_mock.get_zone_last_time.return_value = None
        db_mock.build_inventory_batch.reset_mock()
        if event_type == "PRODUCT_MOVED":
            evt = _event(event_type, from_zone_id="ZONE-A", to_zone_id="ZONE-B")
        else:
            evt = _event(event_type)
        # Should not raise
        handlers.dispatch(evt)
