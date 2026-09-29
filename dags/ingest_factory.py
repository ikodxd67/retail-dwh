"""Фабрика DAG-ов загрузки: один DAG на каждый pipelines/*.yml.

Новый источник = новый YAML, Python писать не нужно. DAG получает
расписание, владельца и теги из конфига, а по завершении обновляет Asset
с именем stg-таблицы — на эти Asset-ы подписан DAG сборки хранилища.

Для файловых источников перед загрузкой стоит сенсор в режиме reschedule:
пока файлов нет, он не держит слот исполнителя.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.sdk import DAG, Asset

from retail_dwh.airflow.operators import IngestOperator, LandingFilesSensor
from retail_dwh.ingest.config import FilesSource, load_pipelines


def build_dag(pipeline) -> DAG:
    with DAG(
        dag_id=f"ingest__{pipeline.name}",
        description=pipeline.description,
        schedule=pipeline.schedule,
        start_date=datetime(2026, 9, 1),
        catchup=False,
        max_active_runs=1,  # водяной знак у пайплайна один, параллельные запуски бессмысленны
        tags=["ingest", *pipeline.tags],
        default_args={
            "owner": pipeline.owner,
            "retries": pipeline.retries,
            "retry_delay": timedelta(minutes=2),
            "retry_exponential_backoff": True,
        },
        doc_md=f"**{pipeline.description}**\n\nКонфиг: `pipelines/{pipeline.name}.yml`, "
        f"цель: `{pipeline.target}`.",
    ) as dag:
        ingest = IngestOperator(
            task_id="extract_load_check",
            pipeline=pipeline.name,
            outlets=[Asset(name=pipeline.target)],
            execution_timeout=timedelta(minutes=30),
        )
        if isinstance(pipeline.source, FilesSource):
            wait = LandingFilesSensor(
                task_id="wait_for_files",
                pipeline=pipeline.name,
                bucket=pipeline.source.bucket,
                prefix=pipeline.source.prefix,
                mode="reschedule",
                poke_interval=300,
                timeout=timedelta(hours=6),
                soft_fail=True,  # не дождались файла — это не авария, а пропуск
            )
            wait >> ingest
    return dag


for _pipeline in load_pipelines().values():
    globals()[f"ingest__{_pipeline.name}"] = build_dag(_pipeline)
