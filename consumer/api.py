"""
FastAPI application exposing:
  - GET /health   — liveness/readiness probe (point 9)
  - GET /metrics  — Prometheus metrics endpoint (point 9)
"""

from fastapi import FastAPI, Response
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
import db

app = FastAPI(title="Warehouse Consumer", docs_url=None, redoc_url=None)


@app.get("/health")
def health():
    """
    Returns 200 if both Kafka and Cassandra connections are alive.
    Returns 503 otherwise.
    """
    import consumer as c
    kafka_ok    = c.is_kafka_connected()
    cassandra_ok = db.is_session_alive()

    if kafka_ok and cassandra_ok:
        return {"status": "ok", "kafka": "connected", "cassandra": "connected"}

    return Response(
        content='{"status":"unhealthy","kafka":"%s","cassandra":"%s"}' % (
            "connected" if kafka_ok else "disconnected",
            "connected" if cassandra_ok else "disconnected",
        ),
        status_code=503,
        media_type="application/json",
    )


@app.get("/metrics")
def prometheus_metrics():
    """Prometheus-format metrics."""
    data = generate_latest()
    return Response(content=data, media_type=CONTENT_TYPE_LATEST)
