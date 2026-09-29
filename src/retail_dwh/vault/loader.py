"""Загрузка ожидающих партий из stg в raw_vault, строго по порядку номеров."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass

from retail_dwh import connections, metrics
from retail_dwh.vault.model import VaultModel, load_model
from retail_dwh.vault.render import load_statements

log = logging.getLogger(__name__)


@dataclass
class BatchLoad:
    batch_id: int
    table: str
    inserted: dict[str, int]
    seconds: float


def _target(stmt: str) -> str:
    if stmt.startswith("INSERT INTO "):
        return stmt.split()[2].split(".")[1]
    return "_src"


def pending_batches(conn, tables: list[str]) -> list[tuple[int, str]]:
    return conn.execute(
        "SELECT batch_id, target_table FROM meta.batches "
        "WHERE status = 'loaded' AND vault_loaded_at IS NULL AND target_table = ANY(%s) "
        "ORDER BY batch_id",
        (tables,),
    ).fetchall()


def load_pending(model: VaultModel | None = None, limit: int | None = None) -> list[BatchLoad]:
    model = model or load_model()
    compiled = {t: load_statements(model, t) for t in model.loadable_tables}
    done: list[BatchLoad] = []
    started = time.monotonic()
    with connections.dwh() as conn:
        # один загрузчик за раз: два параллельных прогона вставили бы одну
        # версию сателлита дважды. Блокировка снимается с концом сессии.
        if not conn.execute("SELECT pg_try_advisory_lock(hashtext('vault_load'))").fetchone()[0]:
            raise RuntimeError("загрузка vault уже идёт в другой сессии")
        conn.commit()
        batches = pending_batches(conn, model.loadable_tables)
        conn.commit()
        for batch_id, table in batches[:limit]:
            t0 = time.monotonic()
            inserted = {}
            spec = model.sources.get(table) or model.references[table]
            params = {"record_source": spec.record_source}
            with conn.transaction():
                for stmt in compiled[table]:
                    # номер партии подставляем литералом: CREATE TABLE AS не
                    # принимает параметры на стороне сервера
                    cur = conn.execute(stmt.replace("%(batch_id)s", str(int(batch_id))), params)
                    target = _target(stmt)
                    inserted[target] = inserted.get(target, 0) + max(cur.rowcount, 0)
                conn.execute(
                    "UPDATE meta.batches SET vault_loaded_at = now() WHERE batch_id = %s",
                    (batch_id,),
                )
            load = BatchLoad(batch_id, table, inserted, time.monotonic() - t0)
            done.append(load)
            log.info(
                "vault: партия %s (%s) за %.1f с: %s",
                batch_id,
                table,
                load.seconds,
                {k: v for k, v in inserted.items() if k != "_src"},
            )
        # пустые партии тоже помечаем, чтобы не висели в очереди
        conn.execute(
            "UPDATE meta.batches SET vault_loaded_at = now() "
            "WHERE status = 'empty' AND vault_loaded_at IS NULL"
        )
        conn.commit()
    rows = sum(v for b in done for k, v in b.inserted.items() if k != "_src")
    metrics.push_build("vault_load", time.monotonic() - started, rows)
    return done
