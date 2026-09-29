"""Сквозные проверки на живом стенде: Oracle, CRM, DWH, S3.

Запуск: `pytest -m integration` при поднятом `docker compose up -d` и
выполненных `retail-dwh migrate` и `retail-dwh simulate`. Тесты добавляют
в источники несколько записей с префиксом IT- и оставляют их — это стенд.
"""

from __future__ import annotations

import os
import random
from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from retail_dwh import connections, dq
from retail_dwh.ingest.config import load_pipelines
from retail_dwh.ingest.runner import run_pipeline
from retail_dwh.marts import build
from retail_dwh.vault.loader import load_pending

pytestmark = pytest.mark.integration
ALMATY = ZoneInfo("Asia/Almaty")


# в CI внешнее API Нацбанка может быть недоступно — такие пайплайны пропускаем
SKIP = set(filter(None, os.environ.get("IT_SKIP_PIPELINES", "").split(",")))


def refresh(*names: str) -> None:
    pipelines = load_pipelines()
    for name in names or [p for p in pipelines if p not in SKIP]:
        result = run_pipeline(name)
        dq.check_batches(pipelines[name], result.batches)
    load_pending()
    build()


def dwh_value(sql: str, params: tuple = ()):
    with connections.dwh() as conn:
        return conn.execute(sql, params).fetchone()[0]


@pytest.fixture(scope="module", autouse=True)
def full_refresh():
    refresh()


def test_marts_quality_checks_pass():
    results = dq.check_marts()  # бросит DataQualityError, если fail-проверка упала
    assert results


def test_fact_matches_erp():
    watermark = dwh_value("SELECT value FROM meta.watermarks WHERE pipeline = 'erp_order_lines'")
    with connections.oracle() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT count(*) FROM order_lines WHERE updated_at <= :wm",
            wm=datetime.fromisoformat(watermark),
        )
        erp_lines = cur.fetchone()[0]
    assert dwh_value("SELECT count(*) FROM marts.fact_sales") >= erp_lines
    assert dwh_value("SELECT count(*) FROM raw_vault.lnk_order_line") == dwh_value(
        "SELECT count(*) FROM marts.fact_sales"
    )


def test_rerun_is_idempotent():
    """Окно lookback перечитывает последний час — в vault это не должно дать новых версий."""
    before = dwh_value("SELECT count(*) FROM raw_vault.sat_order_erp")
    started = dwh_value("SELECT now()")
    refresh("erp_orders", "erp_order_lines")
    overlap = dwh_value(
        "SELECT rows_loaded FROM meta.batches WHERE pipeline = 'erp_orders' ORDER BY batch_id DESC LIMIT 1"
    )
    duplicates = dwh_value(
        "SELECT count(*) FROM (SELECT hk_order, hashdiff FROM raw_vault.sat_order_erp "
        "GROUP BY 1, 2 HAVING count(*) > 1) d"
    )
    assert duplicates == 0
    # прирост — только версии, записанные этим прогоном (если симулятор успел
    # добавить заказы); перечитанное окно lookback прироста не даёт
    new_orders = dwh_value("SELECT count(*) FROM raw_vault.sat_order_erp WHERE load_dts >= %s", (started,))
    assert dwh_value("SELECT count(*) FROM raw_vault.sat_order_erp") == before + new_orders
    assert overlap is not None


def _customer_with_orders() -> str:
    return dwh_value(
        "SELECT c.customer_id FROM marts.dim_customer c "
        "JOIN marts.fact_sales f ON f.customer_sk = c.customer_sk "
        "WHERE c.is_current AND c.customer_sk > 0 AND NOT c.is_deleted LIMIT 1"
    )


def test_scd2_new_version_on_change():
    customer_id = _customer_with_orders()
    old_tier = dwh_value(
        "SELECT loyalty_tier FROM marts.dim_customer WHERE customer_id = %s AND is_current", (customer_id,)
    )
    new_tier = "platinum" if old_tier != "platinum" else "silver"
    with connections.crm() as conn:
        conn.execute(
            "UPDATE customers SET loyalty_tier = %s, updated_at = now() WHERE customer_id = %s",
            (new_tier, int(customer_id)),
        )
        conn.commit()
    refresh("crm_customers")

    with connections.dwh() as conn:
        versions = conn.execute(
            # границы как текст: у первой версии valid_from = -infinity, в datetime его не перевести
            "SELECT loyalty_tier, valid_from::text, valid_to::text, is_current FROM marts.dim_customer "
            "WHERE customer_id = %s ORDER BY valid_from",
            (customer_id,),
        ).fetchall()
    assert len(versions) >= 2
    assert [v[3] for v in versions].count(True) == 1
    assert versions[-1][0] == new_tier and versions[-1][3]
    assert versions[-2][2] == versions[-1][1], "старая версия закрывается ровно в момент новой"
    # старые заказы остались на старой версии: отчёт по уровню лояльности не переписан задним числом
    stale = dwh_value(
        "SELECT count(*) FROM marts.fact_sales f JOIN marts.dim_customer c USING (customer_sk) "
        "WHERE c.customer_id = %s AND c.is_current AND f.order_ts < c.valid_from",
        (customer_id,),
    )
    assert stale == 0


def test_late_arriving_customer_is_rekeyed():
    """Заказ пришёл раньше клиента: сначала ключ -2, после загрузки CRM — настоящий."""
    customer_id = 9_000_000 + random.randint(0, 999_999)
    now = datetime.now(ALMATY).replace(tzinfo=None)
    order_no = f"IT-{customer_id}"
    with connections.oracle() as conn:
        cur = conn.cursor()
        cur.execute("SELECT max(order_id) + 1 FROM orders")
        order_id = cur.fetchone()[0]
        cur.execute(
            "INSERT INTO orders (order_id, order_no, store_id, customer_id, order_ts, channel, "
            "status, payment_type, updated_at) "
            "VALUES (:id, :no, 1, :cust, :ts, 'STORE', 'PAID', 'CARD', :ts)",
            {"id": order_id, "no": order_no, "cust": customer_id, "ts": now},
        )
        cur.execute(
            "INSERT INTO order_lines (order_id, line_no, sku, qty, unit_price, discount_amt, updated_at) "
            "VALUES (:id, 1, 'SKU-000001', 2, 500, 0, :ts)",
            {"id": order_id, "ts": now},
        )
        conn.commit()
    refresh("erp_orders", "erp_order_lines")
    assert dwh_value("SELECT customer_sk FROM marts.fact_sales WHERE order_no = %s", (order_no,)) == -2

    with connections.crm() as conn:
        conn.execute(
            "INSERT INTO customers (customer_id, first_name, last_name, loyalty_tier, created_at, "
            "updated_at) VALUES (%s, 'Тест', 'Интеграционный', 'basic', now(), now())",
            (customer_id,),
        )
        conn.commit()
    refresh("crm_customers")
    sk = dwh_value("SELECT customer_sk FROM marts.fact_sales WHERE order_no = %s", (order_no,))
    assert sk > 0
    assert dwh_value("SELECT customer_id FROM marts.dim_customer WHERE customer_sk = %s", (sk,)) == str(
        customer_id
    )
