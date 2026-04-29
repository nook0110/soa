"""
ClickHouse client wrapper using clickhouse-connect.
"""

import logging

import clickhouse_connect
from clickhouse_connect.driver import Client

from .config import settings

logger = logging.getLogger(__name__)


def get_client() -> Client:
    return clickhouse_connect.get_client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        database=settings.clickhouse_db,
        username=settings.clickhouse_user,
        password=settings.clickhouse_password,
    )
