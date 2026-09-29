"""Миграции DWH: sql/dwh/NNN_*.sql по порядку, каждая один раз, плюс бакеты S3.

DDL слоя raw_vault не лежит файлом — он генерируется из models/vault.yml и
применяется на каждом запуске (всё через IF NOT EXISTS). Новые хабы и
сателлиты появляются сами после правки модели.
"""

from __future__ import annotations

import logging

from retail_dwh import connections
from retail_dwh.settings import PROJECT_DIR
from retail_dwh.vault.model import load_model
from retail_dwh.vault.render import ddl

log = logging.getLogger(__name__)
VAULT_AFTER = "002"  # vault строится после stg и до витрин


def migrate() -> list[str]:
    applied_now = []
    files = sorted((PROJECT_DIR / "sql/dwh").glob("[0-9][0-9][0-9]_*.sql"))
    with connections.dwh() as conn:
        conn.execute("CREATE SCHEMA IF NOT EXISTS meta")
        conn.execute(
            "CREATE TABLE IF NOT EXISTS meta.schema_migrations "
            "(version text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())"
        )
        done = {r[0] for r in conn.execute("SELECT version FROM meta.schema_migrations")}
        vault_done = False
        for path in files:
            if path.name[:3] > VAULT_AFTER and not vault_done:
                conn.execute(ddl(load_model()))
                vault_done = True
            if path.stem in done:
                continue
            log.info("применяю %s", path.name)
            conn.execute(path.read_text(encoding="utf-8"))
            conn.execute("INSERT INTO meta.schema_migrations (version) VALUES (%s)", (path.stem,))
            applied_now.append(path.stem)
        if not vault_done:
            conn.execute(ddl(load_model()))
        conn.commit()
    created = connections.ensure_buckets()
    if created:
        log.info("созданы бакеты: %s", created)
    return applied_now
