"""
FastAPI application exposing:
  - GET /health   — liveness/readiness probe
  - GET /metrics  — Prometheus metrics endpoint
  - Starlette middleware for automatic HTTP metrics (HW7 point 4)
"""

import time
import logging

from fastapi import FastAPI, Request, Response
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST, CollectorRegistry, REGISTRY
import db
import metrics as m

logger = logging.getLogger(__name__)

app = FastAPI(title="Warehouse Consumer", docs_url=None, redoc_url=None)


@app.middleware("http")
async def prometheus_middleware(request: Request, call_next):
    """Record http_requests_total, http_request_errors_total, http_request_duration_seconds."""
    method = request.method
    endpoint = request.url.path
    start = time.monotonic()
    try:
        response = await call_next(request)
        elapsed = time.monotonic() - start
        status = str(response.status_code)
        m.http_requests_total.labels(method=method, endpoint=endpoint, status=status).inc()
        m.http_request_duration_seconds.labels(method=method, endpoint=endpoint).observe(elapsed)
        if response.status_code >= 400:
            error_type = "4xx" if response.status_code < 500 else "5xx"
            m.http_request_errors_total.labels(
                method=method, endpoint=endpoint, error_type=error_type
            ).inc()
        return response
    except Exception as exc:
        elapsed = time.monotonic() - start
        m.http_requests_total.labels(method=method, endpoint=endpoint, status="500").inc()
        m.http_request_errors_total.labels(
            method=method, endpoint=endpoint, error_type="exception"
        ).inc()
        m.http_request_duration_seconds.labels(method=method, endpoint=endpoint).observe(elapsed)
        raise


@app.get("/health")
def health():
    """
    Returns 200 if both Kafka and Cassandra connections are alive.
    Returns 503 otherwise.
    """
    import consumer as c
    kafka_ok     = c.is_kafka_connected()
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
