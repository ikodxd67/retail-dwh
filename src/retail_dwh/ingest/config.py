"""Описание пайплайна загрузки (pipelines/*.yml).

Один YAML — один источник: откуда брать, как понимать «что нового», куда
класть и что проверить. По этим файлам фабрика собирает DAG-и Airflow.
"""

from __future__ import annotations

import re
from datetime import timedelta
from pathlib import Path
from typing import Annotated, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator

from retail_dwh.settings import PROJECT_DIR

_DURATION = re.compile(r"^(\d+)([smhd])$")


def parse_duration(value: str) -> timedelta:
    m = _DURATION.match(value)
    if not m:
        raise ValueError(f"длительность вида 30m / 2h / 1d, а пришло {value!r}")
    n, unit = int(m[1]), m[2]
    return timedelta(**{{"s": "seconds", "m": "minutes", "h": "hours", "d": "days"}[unit]: n})


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class Incremental(_Strict):
    column: str
    initial: str
    lookback: str = "0m"

    @field_validator("lookback")
    @classmethod
    def _check(cls, v: str) -> str:
        parse_duration(v)
        return v


class DbSource(_Strict):
    kind: Literal["oracle", "postgres"]
    connection: Literal["erp", "crm"]
    query: str  # путь к .sql от корня проекта
    incremental: Incremental | None = None  # None — полный снимок
    timezone: str | None = None  # для источников, которые пишут время без зоны


class FilesSource(_Strict):
    kind: Literal["s3_files"]
    bucket: str = "landing"
    prefix: str
    parser: Literal["catalog_xml", "stock_csv", "promo_json"]
    max_files_per_run: int = 50
    archive_bucket: str | None = "archive"


class HttpSource(_Strict):
    kind: Literal["nbk_rates"]
    start_date: str
    max_days_per_run: int = 60


Source = Annotated[DbSource | FilesSource | HttpSource, Field(discriminator="kind")]


class Check(_Strict):
    check: Literal["not_null", "unique", "row_count_min", "accepted_values", "sql"]
    columns: list[str] = []
    values: list[str] = []
    value: float | None = None
    query: str | None = None  # для check: sql — запрос, возвращающий число «плохих» строк
    severity: Literal["warn", "fail"] = "fail"
    name: str | None = None


class Pipeline(_Strict):
    name: str
    description: str
    owner: str
    schedule: str | None
    tags: list[str] = []
    source: Source
    target: str
    checks: list[Check] = []
    retries: int = 2

    @field_validator("target")
    @classmethod
    def _stg_only(cls, v: str) -> str:
        if not v.startswith("stg."):
            raise ValueError("пайплайны загрузки пишут только в stg")
        return v


def load_pipelines(directory: Path | None = None) -> dict[str, Pipeline]:
    pipelines = {}
    for path in sorted((directory or PROJECT_DIR / "pipelines").glob("*.yml")):
        pipeline = Pipeline.model_validate(yaml.safe_load(path.read_text(encoding="utf-8")))
        if pipeline.name != path.stem:
            raise ValueError(f"{path.name}: имя пайплайна должно совпадать с именем файла")
        pipelines[pipeline.name] = pipeline
    return pipelines
