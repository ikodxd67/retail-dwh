"""Подключения к базам и хранилищам. Драйверы импортируются лениво:
модулю генератора не нужен ClickHouse, а тестам парсеров — вообще ничего."""

from __future__ import annotations

from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

from retail_dwh.settings import settings

if TYPE_CHECKING:
    from collections.abc import Iterator

    import oracledb
    import psycopg


@contextmanager
def oracle() -> Iterator[oracledb.Connection]:
    import oracledb

    s = settings()
    # thin-режим: чистый Python, Oracle Instant Client не нужен
    conn = oracledb.connect(user=s.oracle_user, password=s.oracle_password, dsn=s.oracle_dsn)
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def postgres(dsn: str, *, autocommit: bool = False) -> Iterator[psycopg.Connection]:
    import psycopg

    with psycopg.connect(dsn, autocommit=autocommit) as conn:
        yield conn


def crm(**kwargs: Any):
    return postgres(settings().crm_dsn, **kwargs)


def dwh(**kwargs: Any):
    return postgres(settings().dwh_dsn, **kwargs)


def s3():
    import boto3
    from botocore.config import Config

    s = settings()
    return boto3.client(
        "s3",
        endpoint_url=s.s3_endpoint,
        aws_access_key_id=s.s3_access_key,
        aws_secret_access_key=s.s3_secret_key,
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}),
    )


def clickhouse():
    import clickhouse_connect

    s = settings()
    return clickhouse_connect.get_client(
        host=s.clickhouse_host, port=s.clickhouse_port, username="default", password=""
    )
