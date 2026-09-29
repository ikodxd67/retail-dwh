"""Проверки качества данных.

Две точки проверки:
- партия в stg сразу после загрузки (правила из pipelines/*.yml) — ловит
  проблемы источника до того, как они разойдутся по хранилищу;
- витрины после сборки (правила из catalog/dq_marts.yml) — ссылочная
  целостность звезды, сверка сумм с vault, свежесть.

Уровень fail роняет задачу Airflow, warn только пишется в журнал и метрики.
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass

import yaml

from retail_dwh import connections, metrics
from retail_dwh.ingest.config import Check, Pipeline
from retail_dwh.settings import PROJECT_DIR

log = logging.getLogger(__name__)


class DataQualityError(RuntimeError):
    pass


@dataclass
class CheckResult:
    check_name: str
    table_name: str
    severity: str
    passed: bool
    observed: float | None
    threshold: float | None
    details: str | None = None


def _ident_list(columns: list[str]) -> str:
    for c in columns:
        if not c.replace("_", "").isalnum():
            raise ValueError(f"недопустимое имя колонки: {c}")
    return ", ".join(columns)


def check_sql(check: Check, relation: str) -> tuple[str, str, float]:
    """(имя проверки, SQL, возвращающий число нарушений, порог)."""
    cols = _ident_list(check.columns)
    threshold = 0.0
    if check.check == "not_null":
        cond = " OR ".join(f"{c} IS NULL" for c in check.columns)
        name, sql = f"not_null({cols})", f"SELECT count(*) FROM {relation} t WHERE {cond}"
    elif check.check == "unique":
        name = f"unique({cols})"
        sql = (
            f"SELECT coalesce(sum(n - 1), 0) FROM (SELECT count(*) AS n FROM {relation} t "
            f"GROUP BY {cols} HAVING count(*) > 1) d"
        )
    elif check.check == "accepted_values":
        values = ", ".join("'" + v.replace("'", "''") + "'" for v in check.values)
        [col] = check.columns
        name = f"accepted_values({col})"
        sql = f"SELECT count(*) FROM {relation} t WHERE {col} IS NOT NULL AND {col} NOT IN ({values})"
    elif check.check == "row_count_min":
        # «нарушение» = сколько строк не хватает до минимума
        name = "row_count_min"
        sql = f"SELECT greatest(%s - count(*), 0) FROM {relation} t" % int(check.value or 1)
    else:
        name = check.name or "custom_sql"
        sql = check.query.format(table=relation)
        threshold = float(check.value or 0)
    return check.name or name, sql, threshold


def _record(conn, run_id: str, results: list[CheckResult]) -> None:
    with conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO meta.dq_results (run_id, check_name, table_name, severity, passed, "
            "observed, threshold, details) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
            [
                (run_id, r.check_name, r.table_name, r.severity, r.passed, r.observed, r.threshold, r.details)
                for r in results
            ],
        )


def _evaluate(conn, checks: list[tuple[Check, str]], table: str) -> list[CheckResult]:
    results = []
    for check, relation in checks:
        name, sql, threshold = check_sql(check, relation)
        observed = float(conn.execute(sql).fetchone()[0] or 0)
        results.append(CheckResult(name, table, check.severity, observed <= threshold, observed, threshold))
    return results


def _finish(conn, layer: str, results: list[CheckResult]) -> list[CheckResult]:
    run_id = uuid.uuid4().hex[:12]
    _record(conn, run_id, results)
    conn.commit()
    metrics.push_dq(layer, [r.__dict__ for r in results])
    for r in results:
        level = logging.INFO if r.passed else logging.WARNING
        log.log(
            level,
            "%s %s.%s: %s (порог %s)",
            "OK  " if r.passed else "FAIL",
            r.table_name,
            r.check_name,
            r.observed,
            r.threshold,
        )
    failed = [r for r in results if not r.passed and r.severity == "fail"]
    if failed:
        raise DataQualityError("; ".join(f"{r.table_name}.{r.check_name} = {r.observed:g}" for r in failed))
    return results


def check_batches(pipeline: Pipeline, batch_ids: list[int]) -> list[CheckResult]:
    """Проверить партии stg. Партия с проваленной fail-проверкой получает статус
    rejected, и vault её не заберёт, пока её не отпустят (`retail-dwh batch release`)."""
    if not batch_ids or not pipeline.checks:
        return []
    results = []
    with connections.dwh() as conn:
        for batch_id in map(int, batch_ids):
            relation = f"(SELECT * FROM {pipeline.target} WHERE _batch_id = {batch_id})"
            batch_results = _evaluate(conn, [(c, relation) for c in pipeline.checks], pipeline.target)
            bad = [r for r in batch_results if not r.passed and r.severity == "fail"]
            if bad:
                conn.execute(
                    "UPDATE meta.batches SET status = 'rejected', error = %s WHERE batch_id = %s",
                    ("; ".join(f"{r.check_name} = {r.observed:g}" for r in bad), batch_id),
                )
            results += batch_results
        return _finish(conn, f"stg:{pipeline.name}", results)


def check_marts() -> list[CheckResult]:
    spec = yaml.safe_load((PROJECT_DIR / "catalog/dq_marts.yml").read_text(encoding="utf-8"))
    with connections.dwh() as conn:
        results = []
        for table, checks in spec.items():
            parsed = [Check.model_validate(c) for c in checks]
            results += _evaluate(conn, [(c, table) for c in parsed], table)
        return _finish(conn, "marts", results)
