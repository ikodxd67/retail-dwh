"""Метрики пайплайнов в Prometheus через Pushgateway.

Задачи Airflow живут секунды, Prometheus их не успеет опросить — поэтому
не pull, а push: задача в конце отправляет свои цифры в Pushgateway, а
Prometheus забирает их оттуда. Если RETAIL_PUSHGATEWAY не задан (тесты,
запуск с ноутбука), метрики молча не отправляются.
"""

from __future__ import annotations

import logging
import time

from retail_dwh.settings import settings

log = logging.getLogger(__name__)


def _push(job: str, grouping: dict[str, str], fill) -> None:
    gateway = settings().pushgateway
    if not gateway:
        return
    from prometheus_client import CollectorRegistry, push_to_gateway

    registry = CollectorRegistry()
    fill(registry)
    try:
        push_to_gateway(gateway, job=job, registry=registry, grouping_key=grouping, timeout=5)
    except OSError as exc:
        # мониторинг не должен ронять загрузку данных
        log.warning("pushgateway недоступен: %s", exc)


def push_ingest(pipeline: str, rows: int, seconds: float, *, ok: bool) -> None:
    from prometheus_client import Gauge

    def fill(registry):
        Gauge("retail_ingest_rows", "Строк загружено за запуск", registry=registry).set(rows)
        Gauge("retail_ingest_duration_seconds", "Длительность запуска", registry=registry).set(seconds)
        Gauge("retail_ingest_success", "1 — запуск успешен", registry=registry).set(int(ok))
        if ok:
            Gauge(
                "retail_ingest_last_success_timestamp_seconds", "Время последнего успеха", registry=registry
            ).set(time.time())

    _push("ingest", {"pipeline": pipeline}, fill)


def push_dq(layer: str, results: list[dict]) -> None:
    from prometheus_client import Gauge

    def fill(registry):
        passed = Gauge(
            "retail_dq_check_passed", "1 — проверка прошла", ["check", "table", "severity"], registry=registry
        )
        observed = Gauge(
            "retail_dq_check_observed",
            "Наблюдаемое значение проверки",
            ["check", "table", "severity"],
            registry=registry,
        )
        for r in results:
            labels = (r["check_name"], r["table_name"], r["severity"])
            passed.labels(*labels).set(int(r["passed"]))
            if r["observed"] is not None:
                observed.labels(*labels).set(float(r["observed"]))
        Gauge("retail_dq_last_run_timestamp_seconds", "Время прогона проверок", registry=registry).set(
            time.time()
        )

    _push("dq", {"layer": layer}, fill)


def push_build(step: str, seconds: float, rows: int) -> None:
    from prometheus_client import Gauge

    def fill(registry):
        Gauge("retail_build_duration_seconds", "Длительность шага сборки", registry=registry).set(seconds)
        Gauge("retail_build_rows", "Строк затронуто шагом", registry=registry).set(rows)
        Gauge("retail_build_last_success_timestamp_seconds", "Время успешной сборки", registry=registry).set(
            time.time()
        )

    _push("build", {"step": step}, fill)
