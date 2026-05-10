"""
Prometheus metrics definitions (point 9).
"""

from prometheus_client import Counter, Histogram, Gauge

events_processed_total = Counter(
    "events_processed_total",
    "Total number of successfully processed events",
    ["event_type"],
)

event_processing_duration_seconds = Histogram(
    "event_processing_duration_seconds",
    "Time spent processing a single event (seconds)",
    buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0],
)

cassandra_write_errors_total = Counter(
    "cassandra_write_errors_total",
    "Total number of errors writing to Cassandra",
)

consumer_lag = Gauge(
    "consumer_lag",
    "Consumer lag (latest offset - committed offset) per partition",
    ["topic", "partition"],
)

dlq_events_total = Counter(
    "dlq_events_total",
    "Total events sent to Dead Letter Queue",
    ["reason"],
)
