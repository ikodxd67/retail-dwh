"""Обслуживание: симулятор источников и чистка staging."""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.sdk import DAG, task

with DAG(
    dag_id="simulate_sources",
    description="Стенд: генерирует новые заказы, правки клиентов и файлы",
    schedule="*/20 * * * *",
    start_date=datetime(2026, 9, 1),
    catchup=False,
    max_active_runs=1,
    tags=["demo"],
    default_args={"owner": "data-platform", "retries": 0},
    doc_md="Заменяет реальные системы на стенде. В проде этого DAG-а нет.",
):

    @task
    def simulate() -> dict:
        from retail_dwh.generator.simulator import Simulator

        return Simulator().run()

    simulate()


with DAG(
    dag_id="stg_retention",
    description="Удаляет из stg партии, которые уже разложены по vault и старше 7 дней",
    schedule="0 4 * * *",
    start_date=datetime(2026, 9, 1),
    catchup=False,
    tags=["maintenance"],
    default_args={"owner": "data-platform", "retries": 1, "retry_delay": timedelta(minutes=10)},
):

    @task
    def purge_stg(keep_days: int = 7) -> dict:
        from retail_dwh import connections

        deleted = {}
        with connections.dwh() as conn:
            tables = [
                r[0]
                for r in conn.execute(
                    "SELECT DISTINCT target_table FROM meta.batches WHERE vault_loaded_at IS NOT NULL"
                )
            ]
            for table in tables:
                # удаляем только то, что vault уже забрал: stg — буфер, а не архив
                cur = conn.execute(
                    f"DELETE FROM {table} WHERE _batch_id IN ("
                    "  SELECT batch_id FROM meta.batches"
                    "  WHERE target_table = %s AND vault_loaded_at < now() - make_interval(days => %s))",
                    (table, keep_days),
                )
                deleted[table] = cur.rowcount
            conn.commit()
        return deleted

    purge_stg()
