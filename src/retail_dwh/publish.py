"""Публикация витрин в ClickHouse.

Продажи забирает сам ClickHouse через табличную функцию postgresql():
данные идут напрямую из базы в базу, не через память Airflow. Публикуем
помесячно и только изменившиеся месяцы: месяц собирается в промежуточной
таблице и атомарно подменяет партицию (REPLACE PARTITION). Повторный запуск
даёт тот же результат, дублей не бывает, а читатели ни в какой момент не
видят половину месяца.
"""

from __future__ import annotations

import logging
import time

from retail_dwh import connections, metrics

log = logging.getLogger(__name__)
WATERMARK = "clickhouse_sales"

COLUMNS = [
    "order_date", "order_ts", "order_no", "line_no", "store_code", "store_city", "store_format",
    "sku", "product_name", "brand", "group_name", "category_name", "subcategory_name",
    "customer_id", "loyalty_tier", "customer_city", "channel", "payment_type", "order_status",
    "promo_campaign_id", "qty", "gross_amount", "discount_amount", "net_amount", "cost_amount",
    "margin_amount",
]  # fmt: skip

# адрес PostgreSQL так, как его видит контейнер ClickHouse
PG_SOURCE = "postgresql('dwh-db:5432', 'dwh', 'v_sales_wide', 'analyst_ro', 'analyst_ro', 'marts')"


def changed_months(full: bool) -> tuple[list[int], str]:
    with connections.dwh() as conn:
        until = conn.execute("SELECT now()").fetchone()[0].isoformat()
        row = conn.execute("SELECT value FROM meta.watermarks WHERE pipeline = %s", (WATERMARK,)).fetchone()
        if full or not row:
            months = conn.execute("SELECT DISTINCT date_key / 100 FROM marts.fact_sales ORDER BY 1")
        else:
            months = conn.execute(
                "SELECT DISTINCT date_key / 100 FROM marts.fact_sales WHERE updated_at > %s ORDER BY 1",
                (row[0],),
            )
        return [int(m[0]) for m in months], until


def publish_sales(full: bool = False) -> dict[int, int]:
    started = time.monotonic()
    months, until = changed_months(full)
    ch = connections.clickhouse()
    ch.command("CREATE TABLE IF NOT EXISTS retail.sales_wide_stage AS retail.sales_wide")
    published = {}
    cols = ", ".join(COLUMNS)
    for month in months:
        lo, hi = month * 100, month * 100 + 99
        ch.command("TRUNCATE TABLE retail.sales_wide_stage")
        ch.command(
            f"INSERT INTO retail.sales_wide_stage ({cols}) "
            f"SELECT {cols} FROM {PG_SOURCE} WHERE date_key BETWEEN {lo} AND {hi}"
        )
        staged = ch.query("SELECT count() FROM retail.sales_wide_stage").result_rows[0][0]
        with connections.dwh() as conn:
            expected = conn.execute(
                "SELECT count(*) FROM marts.fact_sales WHERE date_key BETWEEN %s AND %s", (lo, hi)
            ).fetchone()[0]
        if staged != expected:
            # витрина могла измениться между запросами — месяц не подменяем,
            # водяной знак не двигаем, следующий запуск повторит
            raise RuntimeError(f"{month}: в ClickHouse {staged} строк, в PostgreSQL {expected}")
        ch.command(f"ALTER TABLE retail.sales_wide REPLACE PARTITION {month} FROM retail.sales_wide_stage")
        published[month] = staged
        log.info("clickhouse: месяц %s опубликован, %s строк", month, staged)
    with connections.dwh() as conn:
        conn.execute(
            "INSERT INTO meta.watermarks (pipeline, value) VALUES (%s, %s) "
            "ON CONFLICT (pipeline) DO UPDATE SET value = excluded.value, updated_at = now()",
            (WATERMARK, until),
        )
        conn.commit()
    metrics.push_build("clickhouse_sales", time.monotonic() - started, sum(published.values()))
    return published


def publish_funnel(rows: list[tuple]) -> int:
    """Воронка сайта: строки (session_date, device, source, sessions, ...)."""
    if not rows:
        return 0
    connections.clickhouse().insert(
        "retail.web_funnel_daily",
        rows,
        column_names=[
            "session_date",
            "device",
            "traffic_source",
            "sessions",
            "with_cart",
            "with_checkout",
            "with_purchase",
            "revenue",
            "avg_duration_sec",
        ],
    )
    return len(rows)
