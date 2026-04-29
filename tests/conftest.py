"""
Shared pytest fixtures for integration tests.
Assumes docker-compose services are running.
"""

import os
import time
import uuid

import clickhouse_connect
import pytest
from confluent_kafka import Producer

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
PRODUCER_URL = os.getenv("PRODUCER_URL", "http://localhost:8000")
KAFKA_TOPIC = "movie-events"


@pytest.fixture(scope="session")
def kafka_producer():
    p = Producer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "acks": "all",
            "retries": 3,
        }
    )
    yield p
    p.flush(10)


@pytest.fixture(scope="session")
def ch_client():
    client = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        database="cinema",
        username="default",
        password="",
    )
    yield client
    client.close()


@pytest.fixture(autouse=True)
def unique_event_id():
    """Returns a new UUID for each test."""
    return str(uuid.uuid4())
