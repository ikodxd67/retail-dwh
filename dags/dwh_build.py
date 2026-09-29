"""Сборка хранилища: stg -> raw_vault -> витрины -> проверки качества.

Запускается по событию, а не по часам: как только любой DAG загрузки
обновил свою stg-таблицу (Asset), здесь стартует прогон. Несколько
загрузок подряд схлопываются в один прогон — max_active_runs=1, а vault
всё равно забирает все ожидающие партии разом.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from functools import reduce

from airflow.sdk import DAG, Asset, task

from retail_dwh.airflow.operators import DataQualityOperator
from retail_dwh.ingest.config import load_pipelines

STG_ASSETS = [Asset(name=p.target) for p in load_pipelines().values()]
MART_ASSETS = [Asset(name=t) for t in ("marts.fact_sales", "marts.fact_stock_daily", "marts.dim_customer")]

with DAG(
    dag_id="dwh_build",
    description="stg -> Data Vault -> звезда -> DQ",
    schedule=reduce(lambda a, b: a | b, STG_ASSETS),
    start_date=datetime(2026, 9, 1),
    catchup=False,
    max_active_runs=1,
    tags=["dwh", "vault", "marts"],
    default_args={"owner": "data-platform", "retries": 1, "retry_delay": timedelta(minutes=5)},
    doc_md=__doc__,
) as dag:

    @task(execution_timeout=timedelta(minutes=30))
    def vault_load() -> dict:
        from retail_dwh.vault.loader import load_pending

        loads = load_pending()
        return {
            "batches": len(loads),
            "rows": sum(v for b in loads for k, v in b.inserted.items() if k != "_src"),
        }

    @task(execution_timeout=timedelta(minutes=30))
    def build_marts() -> dict:
        from retail_dwh.marts import build

        return build()

    dq_marts = DataQualityOperator(task_id="dq_marts", outlets=MART_ASSETS)

    vault_load() >> build_marts() >> dq_marts
