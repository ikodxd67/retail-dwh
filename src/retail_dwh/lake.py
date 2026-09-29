"""Озеро: запуск Spark-задач через Spark Connect и регистрация таблиц в Trino."""

from __future__ import annotations

import logging
import sys

from retail_dwh.settings import PROJECT_DIR, settings

log = logging.getLogger(__name__)

DELTA_TABLES = {
    "events": "s3://lake/delta/web/events",
    "events_quarantine": "s3://lake/delta/web/events_quarantine",
    "sessions": "s3://lake/delta/web/sessions",
}


def spark_session(host: str | None = None):
    from pyspark.sql import SparkSession

    host = host or settings().spark_connect
    return SparkSession.builder.remote(host).getOrCreate()


def jobs():
    """Модуль spark/jobs/web_events.py — он не пакет, его кладут в sys.path."""
    path = str(PROJECT_DIR / "spark" / "jobs")
    if path not in sys.path:
        sys.path.insert(0, path)
    import web_events

    return web_events


def trino_cursor(user: str = "retail-dwh"):
    import trino

    s = settings()
    return trino.dbapi.connect(host=s.trino_host, port=s.trino_port, user=user).cursor()


def register_delta_tables() -> list[str]:
    """Сделать Delta-таблицы видимыми в Trino (каталог lake). Повторный вызов безопасен."""
    cur = trino_cursor()
    cur.execute("CREATE SCHEMA IF NOT EXISTS lake.web WITH (location = 's3://lake/delta/web')")
    cur.fetchall()
    cur.execute("SHOW TABLES FROM lake.web")
    existing = {r[0] for r in cur.fetchall()}
    registered = []
    for name, location in DELTA_TABLES.items():
        if name in existing:
            continue
        try:
            cur.execute(
                "CALL lake.system.register_table("
                f"schema_name => 'web', table_name => '{name}', table_location => '{location}')"
            )
            cur.fetchall()
            registered.append(name)
        except Exception as exc:  # таблицы ещё нет в озере — зарегистрируем в следующий раз
            log.warning("не удалось зарегистрировать %s: %s", name, exc)
    return registered
