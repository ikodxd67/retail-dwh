"""MCP-сервер хранилища для AI-агентов (Claude Code, Codex и т.п.).

Даёт агенту то же, что нужно аналитику или дежурному инженеру: каталог с
lineage, состояние пайплайнов, результаты проверок качества и SQL только
на чтение. Запуск: `python -m retail_dwh.mcp_server` (stdio), подключение
к Claude Code — в .mcp.json в корне репозитория.

Защита SQL двухслойная, и главная — вторая:
1. разбор sqlglot: один оператор и только SELECT — даёт агенту понятную
   ошибку и не тратит время базы;
2. права: запросы идут в Trino пользователем mcp-agent, которому правила
   доступа (docker/trino/etc/rules.json) дают только чтение каталогов dwh,
   ch и lake и закрывают erp. Каталог dwh вдобавок ходит в PostgreSQL ролью
   analyst_ro (только схема marts, read-only, statement_timeout 30s).
   Разбор можно обойти хитрым SQL, права — нет.
Служебные таблицы (журнал партий, проверки) читаются ролью monitoring_ro.
"""

from __future__ import annotations

import functools
import json
from datetime import date, datetime
from decimal import Decimal

import sqlglot
from sqlglot import exp

MAX_ROWS = 500
# каталоги Trino, которые агенту можно читать; erp — боевая ERP, туда не пускаем
ALLOWED_CATALOGS = {"dwh", "ch", "lake"}


class QueryRejected(ValueError):
    pass


def check_query(sql: str) -> str:
    """Пропускает только одиночный SELECT по разрешённым каталогам."""
    try:
        statements = [s for s in sqlglot.parse(sql, read="trino") if s is not None]
    except sqlglot.errors.ParseError as exc:
        raise QueryRejected(f"не разобрал SQL: {exc}") from exc
    if len(statements) != 1:
        raise QueryRejected("нужен ровно один оператор")
    stmt = statements[0]
    if not isinstance(stmt, exp.Query):
        raise QueryRejected(f"разрешён только SELECT, а пришёл {stmt.key.upper()}")
    for node in stmt.walk():
        if isinstance(node, exp.Insert | exp.Update | exp.Delete | exp.Merge | exp.Create | exp.Drop):
            raise QueryRejected("изменение данных запрещено")
    ctes = {c.alias_or_name for c in stmt.find_all(exp.CTE)}
    for table in stmt.find_all(exp.Table):
        if table.name in ctes and not table.db:
            continue
        if not table.catalog:
            raise QueryRejected(f"укажите каталог полностью: catalog.schema.table ({table.sql()})")
        if table.catalog not in ALLOWED_CATALOGS:
            raise QueryRejected(f"каталог {table.catalog} закрыт; доступны: {sorted(ALLOWED_CATALOGS)}")
    return stmt.sql(dialect="trino")


def _jsonable(value):
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, datetime | date):
        return value.isoformat()
    return value


def run_sql_impl(sql: str, limit: int = 100) -> dict:
    from retail_dwh.lake import trino_cursor

    safe = check_query(sql)
    limit = max(1, min(limit, MAX_ROWS))
    cur = trino_cursor(user="mcp-agent")
    # SQL не оборачиваем в SELECT ... LIMIT: Trino не обязан сохранять ORDER BY
    # подзапроса. Читаем limit+1 строк из потока и отменяем остаток.
    cur.execute(safe)
    rows = cur.fetchmany(limit + 1)
    columns = [d[0] for d in cur.description]
    cur.cancel()
    return {
        "columns": columns,
        "rows": [[_jsonable(v) for v in r] for r in rows[:limit]],
        "truncated": len(rows) > limit,
    }


def _dwh_rows(query: str, params: tuple = ()) -> list[dict]:
    from retail_dwh import connections
    from retail_dwh.settings import settings

    with connections.postgres(settings().dwh_readonly_dsn) as conn:
        cur = conn.execute(query, params)
        cols = [d.name for d in cur.description]
        return [dict(zip(cols, (_jsonable(v) for v in r), strict=True)) for r in cur.fetchall()]


def build_server():
    from mcp.server.mcpserver import MCPServer
    from mcp.server.mcpserver.exceptions import ToolError
    from mcp.types import ToolAnnotations

    from retail_dwh.catalog import build_lineage, datasets

    def tool_errors(fn):
        """Ожидаемые отказы — текстом агенту, а не «неожиданной ошибкой»."""

        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            try:
                return fn(*args, **kwargs)
            except (QueryRejected, ValueError, LookupError) as exc:
                raise ToolError(str(exc)) from exc
            except Exception as exc:
                if type(exc).__module__.startswith("trino"):
                    raise ToolError(f"ошибка Trino: {exc}") from exc
                raise

        return wrapper

    server = MCPServer(
        name="retail-dwh",
        instructions=(
            "Хранилище данных сети магазинов. Прежде чем писать SQL, найди таблицу через "
            "search_datasets и посмотри describe_dataset: там смысл полей, зерно и "
            "особые значения ключей (-1, -2). SQL — диалект Trino, имена полностью: "
            "dwh.marts.fact_sales, ch.retail.sales_wide, lake.web.events. Тяжёлые "
            "агрегаты по продажам считай в ch.retail.sales_wide, а не в dwh."
        ),
    )
    read_only = ToolAnnotations(readOnlyHint=True, openWorldHint=False)

    @server.tool(annotations=read_only)
    @tool_errors
    def search_datasets(query: str = "") -> list[dict]:
        """Найти наборы данных по имени или описанию (пустой запрос — все)."""
        q = query.lower()
        return [
            {
                "name": name,
                "description": meta.get("description"),
                "owner": meta.get("owner"),
                "pii": meta.get("pii"),
            }
            for name, meta in datasets().items()
            if not q or q in name.lower() or q in (meta.get("description") or "").lower()
        ]

    @server.tool(annotations=read_only)
    @tool_errors
    def describe_dataset(name: str) -> dict:
        """Описание набора: владелец, SLA, зерно, поля, откуда данные и куда уходят."""
        meta = datasets().get(name)
        if meta is None:
            raise ValueError(f"нет в каталоге: {name}. Посмотри search_datasets")
        lineage = build_lineage()
        columns = []
        if name.startswith("marts."):
            schema, table = name.split(".")
            columns = _dwh_rows(
                "SELECT column_name, data_type FROM information_schema.columns "
                "WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position",
                (schema, table),
            )
            notes = meta.get("columns") or {}
            for c in columns:
                c["note"] = notes.get(c["column_name"])
        return {
            "name": name,
            **{k: v for k, v in meta.items() if k != "columns"},
            "columns": columns,
            "upstream": sorted(lineage.upstream(name)),
            "downstream": sorted(lineage.downstream(name)),
        }

    @server.tool(annotations=read_only)
    @tool_errors
    def run_sql(sql: str, limit: int = 100) -> dict:
        """Выполнить SELECT в Trino (каталоги dwh, ch, lake), не больше 500 строк."""
        return run_sql_impl(sql, limit)

    @server.tool(annotations=read_only)
    @tool_errors
    def pipeline_status() -> list[dict]:
        """Состояние загрузок: последняя партия каждого пайплайна, водяной знак, ошибки за сутки."""
        return _dwh_rows(
            """
            SELECT b.pipeline,
                   max(b.finished_at) FILTER (WHERE b.status IN ('loaded', 'empty')) AS last_success,
                   count(*) FILTER (WHERE b.status = 'failed' AND b.started_at > now() - interval '1 day')
                       AS failed_24h,
                   count(*) FILTER (WHERE b.status = 'rejected') AS rejected_total,
                   count(*) FILTER (WHERE b.status = 'loaded' AND b.vault_loaded_at IS NULL)
                       AS waiting_for_vault,
                   w.value AS watermark
            FROM meta.batches b
            LEFT JOIN meta.watermarks w ON w.pipeline = b.pipeline
            GROUP BY b.pipeline, w.value
            ORDER BY b.pipeline
            """
        )

    @server.tool(annotations=read_only)
    @tool_errors
    def data_quality(hours: int = 24, only_failed: bool = True) -> list[dict]:
        """Результаты проверок качества за последние часы (по умолчанию — только проваленные)."""
        latest = _dwh_rows(
            """
            SELECT DISTINCT ON (table_name, check_name)
                   table_name, check_name, severity, passed, observed, threshold, checked_at
            FROM meta.dq_results
            WHERE checked_at > now() - make_interval(hours => %s)
            ORDER BY table_name, check_name, checked_at DESC
            """,
            (hours,),
        )
        return [r for r in latest if not r["passed"]] if only_failed else latest

    @server.tool(annotations=read_only)
    @tool_errors
    def batch_details(batch_id: int) -> dict:
        """Партия загрузки целиком: окно, строки, ошибка и все проверки по ней."""
        rows = _dwh_rows("SELECT * FROM meta.batches WHERE batch_id = %s", (batch_id,))
        if not rows:
            raise ValueError(f"партии {batch_id} нет")
        return json.loads(json.dumps(rows[0], default=str))

    return server


def main() -> None:
    build_server().run("stdio")


if __name__ == "__main__":
    main()
