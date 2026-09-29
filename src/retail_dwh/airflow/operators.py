"""Собственные компоненты Airflow.

Логика загрузки живёт в пакете retail_dwh и не знает про Airflow — её же
вызывает CLI. Здесь только тонкие обёртки: какие параметры принять, что
положить в XCom, когда пропустить задачу.
"""

from __future__ import annotations

import logging
from typing import Any

from airflow.sdk import BaseOperator, BaseSensorOperator
from airflow.sdk.exceptions import AirflowSkipException

log = logging.getLogger(__name__)


class IngestOperator(BaseOperator):
    """Загрузить источник в stg и сразу проверить свежие партии.

    Возвращает (и кладёт в XCom) номера партий и число строк. Если новых
    данных нет, задача помечается skipped — так в интерфейсе видно, что
    источник молчал, а не что загрузка «успешно загрузила ноль».
    """

    template_fields = ("pipeline",)
    ui_color = "#e8f4fd"

    def __init__(self, *, pipeline: str, skip_when_empty: bool = False, **kwargs: Any):
        super().__init__(**kwargs)
        self.pipeline = pipeline
        self.skip_when_empty = skip_when_empty

    def execute(self, context) -> dict[str, Any]:
        from retail_dwh import dq
        from retail_dwh.ingest.config import load_pipelines
        from retail_dwh.ingest.runner import run_pipeline

        config = load_pipelines()[self.pipeline]
        result = run_pipeline(self.pipeline)
        dq.check_batches(config, result.batches)
        if result.skipped_files:
            log.warning("отложены файлы: %s", result.skipped_files)
        if not result.rows and self.skip_when_empty:
            raise AirflowSkipException("новых данных нет")
        return {"batches": result.batches, "rows": result.rows, "skipped": result.skipped_files}


class LandingFilesSensor(BaseSensorOperator):
    """Ждёт в landing хотя бы один файл, который ещё не загружен."""

    template_fields = ("prefix",)
    ui_color = "#fdf5e8"

    def __init__(self, *, pipeline: str, bucket: str, prefix: str, **kwargs: Any):
        super().__init__(**kwargs)
        self.pipeline = pipeline
        self.bucket = bucket
        self.prefix = prefix

    def poke(self, context) -> bool:
        from retail_dwh import connections

        resp = connections.s3().list_objects_v2(Bucket=self.bucket, Prefix=self.prefix, MaxKeys=1000)
        keys = {(o["Key"], o["ETag"].strip('"')) for o in resp.get("Contents", [])}
        if not keys:
            return False
        with connections.dwh() as conn:
            done = set(
                conn.execute(
                    "SELECT object_key, etag FROM meta.processed_files WHERE pipeline = %s",
                    (self.pipeline,),
                ).fetchall()
            )
        new = keys - done
        log.info("в %s/%s новых файлов: %s", self.bucket, self.prefix, len(new))
        return bool(new)


class DataQualityOperator(BaseOperator):
    """Проверки витрин из catalog/dq_marts.yml. warn — в журнал, fail — роняет задачу."""

    ui_color = "#eafaea"

    def execute(self, context) -> dict[str, int]:
        from retail_dwh import dq

        results = dq.check_marts()
        return {
            "passed": sum(r.passed for r in results),
            "warnings": sum(not r.passed for r in results),
        }
