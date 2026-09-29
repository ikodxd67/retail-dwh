"""Озеро и публикация в ClickHouse.

web_events — микропакетный стриминг: каждые 10 минут Spark забирает из
Kafka всё новое (trigger availableNow), пересобирает сессии за последние
дни и отдаёт воронку в ClickHouse. Для настоящего непрерывного потока тот
же код запускается постоянно: `web_events.py stream` на сервере Spark.
Разница только в триггере — логика и чекпоинты общие.

publish_clickhouse — после каждой сборки витрин (Asset) переносит в
ClickHouse изменившиеся месяцы продаж.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.sdk import DAG, Asset, task

WEB_EVENTS = Asset(name="lake.web.events")
WEB_SESSIONS = Asset(name="lake.web.sessions")

with DAG(
    dag_id="web_events",
    description="Kafka -> Delta bronze -> сессии -> воронка в ClickHouse",
    schedule="*/10 * * * *",
    start_date=datetime(2026, 9, 1),
    catchup=False,
    max_active_runs=1,
    tags=["lake", "spark", "kafka"],
    default_args={"owner": "data-platform", "retries": 2, "retry_delay": timedelta(minutes=1)},
    doc_md=__doc__,
):

    @task(outlets=[WEB_EVENTS], execution_timeout=timedelta(minutes=15))
    def ingest_from_kafka() -> dict:
        from retail_dwh.lake import jobs, spark_session

        return jobs().ingest_events(spark_session())

    @task(outlets=[WEB_SESSIONS], execution_timeout=timedelta(minutes=15))
    def sessions_and_funnel() -> int:
        from retail_dwh.lake import jobs, spark_session
        from retail_dwh.publish import publish_funnel

        funnel = jobs().build_sessions(spark_session(), days_back=2)
        rows = [tuple(r) for r in funnel.collect()]  # сотни строк, в память влезают спокойно
        return publish_funnel(rows)

    @task
    def register_in_trino() -> list[str]:
        from retail_dwh.lake import register_delta_tables

        return register_delta_tables()

    ingest_from_kafka() >> sessions_and_funnel() >> register_in_trino()


with DAG(
    dag_id="publish_clickhouse",
    description="Изменившиеся месяцы продаж -> ClickHouse (REPLACE PARTITION)",
    schedule=Asset(name="marts.fact_sales"),
    start_date=datetime(2026, 9, 1),
    catchup=False,
    max_active_runs=1,
    tags=["clickhouse", "publish"],
    default_args={"owner": "data-platform", "retries": 2, "retry_delay": timedelta(minutes=2)},
):

    @task(execution_timeout=timedelta(minutes=30))
    def publish_sales() -> dict:
        from retail_dwh.publish import publish_sales

        return {str(k): v for k, v in publish_sales().items()}

    publish_sales()
