# Warehouse SOA — HW7: CI/CD, Testing & Observability

## System overview

A Kafka-based warehouse management system with:

| Component | Role |
|-----------|------|
| **Producer** | Generates warehouse events (PRODUCT_RECEIVED, PRODUCT_SHIPPED, …) and publishes to Kafka with Avro + Schema Registry |
| **Consumer** | Consumes events, processes them, writes state to Cassandra; exposes `/health` and `/metrics` |
| **Cassandra (×3)** | Stores inventory state with QUORUM writes |
| **Kafka + Zookeeper** | Event bus |
| **Schema Registry** | Avro schema management with backward compatibility |
| **Prometheus** | Metrics scraping + alert rules |
| **Alertmanager** | Alert routing |
| **Grafana** | Dashboards (service + infra) |
| **k6** | Load testing |

## Quick start

```bash
docker compose up
```

Services:
- Consumer API:  http://localhost:8000
- Prometheus:    http://localhost:9090
- Alertmanager:  http://localhost:9093
- Grafana:       http://localhost:3000  (admin/admin)

## Running tests

```bash
# Unit tests (no external deps)
pytest tests/test_handlers_unit.py -v

# Integration tests (requires docker-compose up)
pytest tests/test_integration.py -v --timeout=120

# E2E tests (requires docker-compose up)
pytest tests/test_e2e.py -v --timeout=180

# Load test
k6 run --env CONSUMER_URL=http://localhost:8000 load-tests/load_test.js

# Metrics validation
python scripts/check_metrics.py --prometheus-url http://localhost:9090
```

---

## SLI / SLO Definition

### SLI 1 — API Availability

**What is measured:** fraction of HTTP requests to the consumer that return a 2xx response.

**PromQL:**
```promql
sum(rate(http_requests_total{status=~"2.."}[5m]))
/
(sum(rate(http_requests_total[5m])) + 0.0001)
```

| | Value |
|--|--|
| **SLO** | ≥ 99.5% |
| **Failure threshold** | < 95% |

**Rationale:** The consumer serves two endpoints (`/health`, `/metrics`). Availability below 95% means the service or its probes are broken, which directly impacts monitoring and CI. 99.5% is the normal operating target.

---

### SLI 2 — HTTP p95 Latency

**What is measured:** 95th percentile of HTTP response time for all endpoints.

**PromQL:**
```promql
histogram_quantile(0.95,
  rate(http_request_duration_seconds_bucket[5m])
)
```

| | Value |
|--|--|
| **SLO** | < 500ms |
| **Failure threshold** | > 1000ms |

**Rationale:** `/health` and `/metrics` are lightweight endpoints. p95 > 500ms indicates GC pressure or resource exhaustion. Above 1000ms (1s) the CI pipeline treats this as a broken system.

---

### SLI 3 — Event Processing p95 Latency

**What is measured:** 95th percentile time from event receipt to Cassandra commit.

**PromQL:**
```promql
histogram_quantile(0.95,
  rate(event_processing_duration_seconds_bucket[5m])
)
```

| | Value |
|--|--|
| **SLO** | < 1000ms |
| **Failure threshold** | > 2000ms |

**Rationale:** A Cassandra QUORUM write to a 3-node local cluster takes ~5–50ms normally. p95 > 1s indicates Cassandra compaction pressure or network issues. > 2s is the failure threshold used in CI.

---

### SLI 4 — DLQ Rate

**What is measured:** fraction of events routed to the Dead Letter Queue.

**PromQL:**
```promql
rate(dlq_events_total[5m])
/
(rate(events_processed_total[5m]) + rate(dlq_events_total[5m]) + 0.0001)
```

| | Value |
|--|--|
| **SLO** | < 1% |
| **Failure threshold** | > 5% |

**Rationale:** DLQ events indicate validation or deserialization failures. Under normal operation the producer sends valid Avro events so DLQ rate should be 0. Up to 1% is acceptable for transient schema mismatches. Above 5% indicates a systematic producer bug or schema incompatibility.

---

## Alert rules

Defined in `prometheus/alerts.yml`. Loaded automatically by Prometheus.

| Alert | Condition | Severity |
|-------|-----------|----------|
| `HighDLQRate` | DLQ rate > 5% for 2 min | warning |
| `HighEventProcessingLatency` | p95 > 1s for 5 min | warning |
| `HighConsumerLag` | Total lag > 1000 for 5 min | warning |
| `WarehouseConsumerDown` | Target unreachable for 1 min | critical |
| `HighHTTPErrorRate` | HTTP error rate > 5% for 5 min | warning |
| `HighHTTPLatency` | HTTP p95 > 500ms for 5 min | warning |

To trigger a test alert, stop the consumer:
```bash
docker compose stop consumer
# wait ~1 minute → WarehouseConsumerDown fires in Alertmanager UI
```

## Grafana dashboards

| Dashboard | UID | Description |
|-----------|-----|-------------|
| Warehouse Consumer — Service Metrics | `warehouse-service` | Throughput, latency p50/p95/p99, error rate, event processing, consumer lag |
| Warehouse — Infrastructure | `warehouse-infra` | Kafka consumer lag, partition count, Cassandra request rate, heap memory, pending compactions, service availability |

Dashboards are provisioned automatically on `docker compose up`.
