"""Сборка витрин: шаги sql/marts/NN_*.sql по порядку, в одной транзакции.

Одна транзакция на всю сборку — сознательно. Шаги зависят друг от друга
(факт ищет ключи в измерениях, SCD2 закрывает версии), и частично
применённая сборка при повторе дала бы неверную историю. Либо всё, либо
ничего; водяной знак сдвигается в той же транзакции.

Инкрементальность: каждый шаг берёт из vault только строки с
load_dts > водяного знака прошлой сборки.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime

from psycopg import ClientCursor

from retail_dwh import connections, metrics
from retail_dwh.settings import PROJECT_DIR

log = logging.getLogger(__name__)
WATERMARK = "marts_build"


def steps():
    return sorted((PROJECT_DIR / "sql/marts").glob("[0-9][0-9]_*.sql"))


def build(full: bool = False) -> dict[str, int]:
    stats: dict[str, int] = {}
    started = time.monotonic()
    with connections.dwh() as conn:
        # тот же замок, что у загрузчика vault: пока идёт сборка, новые
        # версии в vault не появляются, и водяной знак не пропустит строк
        conn.execute("SELECT pg_advisory_lock(hashtext('vault_load'))")
        try:
            with conn.transaction():
                until: datetime = conn.execute("SELECT now()").fetchone()[0]
                row = conn.execute(
                    "SELECT value FROM meta.watermarks WHERE pipeline = %s", (WATERMARK,)
                ).fetchone()
                since = "-infinity" if full or not row else row[0]
                log.info("сборка витрин: изменения vault после %s", since)
                cur = ClientCursor(conn)
                for path in steps():
                    t0 = time.monotonic()
                    # ClientCursor: несколько команд в одном файле с параметрами
                    cur.execute(path.read_text(encoding="utf-8"), {"since": since})
                    rows = max(cur.rowcount, 0)
                    stats[path.stem] = rows
                    log.info("  %s: %.1f с", path.stem, time.monotonic() - t0)
                conn.execute(
                    "INSERT INTO meta.watermarks (pipeline, value) VALUES (%s, %s) "
                    "ON CONFLICT (pipeline) DO UPDATE SET value = excluded.value, updated_at = now()",
                    (WATERMARK, until.isoformat()),
                )
        finally:
            conn.execute("SELECT pg_advisory_unlock(hashtext('vault_load'))")
            conn.commit()
        if full:
            conn.autocommit = True
            conn.execute("ANALYZE marts.fact_sales")
    metrics.push_build("marts", time.monotonic() - started, sum(stats.values()))
    return stats
