"""Запуск пайплайна загрузки: источник -> stg, одна или несколько партий.

Гарантии:
- партия пишется в одной транзакции вместе с записью в meta.batches и новым
  водяным знаком — упавшая загрузка не сдвигает водяной знак и не оставляет
  половину строк;
- повторный запуск безопасен: перекрытие окна (lookback) даёт дубли в stg,
  но vault их отбрасывает по hashdiff, а файлы учитываются по ключу и ETag.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

import httpx

from retail_dwh import connections, metrics
from retail_dwh.ingest.config import (
    DbSource,
    FilesSource,
    HttpSource,
    Pipeline,
    load_pipelines,
    parse_duration,
)
from retail_dwh.ingest.parsers import PARSERS, ParseError, parse_nbk_rates
from retail_dwh.settings import PROJECT_DIR

log = logging.getLogger(__name__)

NBK_URL = "https://nationalbank.kz/rss/get_rates.cfm"


@dataclass
class RunResult:
    pipeline: str
    batches: list[int] = field(default_factory=list)
    rows: int = 0
    skipped_files: list[str] = field(default_factory=list)


def stg_columns(conn, table: str) -> list[str]:
    schema, name = table.split(".")
    rows = conn.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = %s AND table_name = %s AND column_name NOT LIKE '\\_%%' "
        "ORDER BY ordinal_position",
        (schema, name),
    ).fetchall()
    if not rows:
        raise RuntimeError(f"нет таблицы {table} — сначала `retail-dwh migrate`")
    return [r[0] for r in rows]


def _open_batch(conn, pipeline: Pipeline, **extra) -> int:
    cols = ", ".join(["pipeline", "target_table", *extra])
    vals = ", ".join(["%s"] * (2 + len(extra)))
    return conn.execute(
        f"INSERT INTO meta.batches ({cols}) VALUES ({vals}) RETURNING batch_id",
        (pipeline.name, pipeline.target, *extra.values()),
    ).fetchone()[0]


def _close_batch(conn, batch_id: int, rows: int, **extra) -> None:
    sets = "".join(f", {k} = %({k})s" for k in extra)
    conn.execute(
        "UPDATE meta.batches SET finished_at = now(), rows_loaded = %(rows)s, "
        f"status = %(status)s{sets} WHERE batch_id = %(id)s",
        {"rows": rows, "status": "loaded" if rows else "empty", "id": batch_id, **extra},
    )


def _fail_batch(pipeline: Pipeline, error: str, **extra) -> None:
    # отдельное соединение: основная транзакция уже откатилась
    with connections.dwh(autocommit=True) as conn:
        batch_id = _open_batch(conn, pipeline, **extra)
        conn.execute(
            "UPDATE meta.batches SET status = 'failed', finished_at = now(), error = %s WHERE batch_id = %s",
            (error[:2000], batch_id),
        )


def copy_rows(conn, table: str, columns: list[str], rows: Iterable, batch_id: int) -> int:
    n = 0
    with conn.cursor() as cur, cur.copy(f"COPY {table} ({', '.join(columns)}, _batch_id) FROM STDIN") as copy:
        for row in rows:
            copy.write_row([*row, batch_id])
            n += 1
    return n


# -------------------------------------------------------------- базы данных
def _tz_fixer(tz_name: str | None):
    if not tz_name:
        return lambda row: row
    tz = ZoneInfo(tz_name)

    def fix(row):
        return tuple(v.replace(tzinfo=tz) if isinstance(v, datetime) and v.tzinfo is None else v for v in row)

    return fix


def _db_rows(source: DbSource, params: dict) -> Iterator[tuple]:
    sql = (PROJECT_DIR / source.query).read_text(encoding="utf-8")
    fix = _tz_fixer(source.timezone)
    if source.kind == "oracle":
        with connections.oracle() as conn:
            cur = conn.cursor()
            cur.arraysize = 20_000
            cur.prefetchrows = 20_001
            cur.execute(sql, params)
            while batch := cur.fetchmany():
                yield from map(fix, batch)
    else:
        with connections.crm() as conn, conn.cursor(name="extract") as cur:
            cur.itersize = 20_000
            cur.execute(sql, params)
            yield from map(fix, cur)


def _run_db(pipeline: Pipeline, source: DbSource, result: RunResult) -> None:
    now_local = datetime.now(ZoneInfo(source.timezone)) if source.timezone else None
    with connections.dwh() as conn:
        columns = stg_columns(conn, pipeline.target)
        params: dict = {}
        wm_from = wm_to = None
        if source.incremental:
            inc = source.incremental
            row = conn.execute(
                "SELECT value FROM meta.watermarks WHERE pipeline = %s", (pipeline.name,)
            ).fetchone()
            stored = datetime.fromisoformat(row[0] if row else inc.initial)
            wm_from = stored - parse_duration(inc.lookback)
            if source.timezone:
                # источник хранит локальное время без зоны — и сравниваем так же
                wm_to = now_local.replace(tzinfo=None)
                wm_from = wm_from.replace(tzinfo=None)
            else:
                wm_to = datetime.now(UTC)
            params = {"wm_from": wm_from, "wm_to": wm_to}

        batch_id = _open_batch(
            conn,
            pipeline,
            watermark_from=wm_from.isoformat() if wm_from else None,
            watermark_to=wm_to.isoformat() if wm_to else None,
        )
        rows = copy_rows(conn, pipeline.target, columns, _db_rows(source, params), batch_id)
        _close_batch(conn, batch_id, rows)
        if source.incremental:
            conn.execute(
                "INSERT INTO meta.watermarks (pipeline, value) VALUES (%s, %s) "
                "ON CONFLICT (pipeline) DO UPDATE SET value = excluded.value, updated_at = now()",
                (pipeline.name, wm_to.isoformat()),
            )
        conn.commit()
    result.batches.append(batch_id)
    result.rows += rows
    log.info("%s: партия %s, %s строк, окно %s .. %s", pipeline.name, batch_id, rows, wm_from, wm_to)


# -------------------------------------------------------------------- файлы
def _run_files(pipeline: Pipeline, source: FilesSource, result: RunResult) -> None:
    s3 = connections.s3()
    objects = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=source.bucket, Prefix=source.prefix):
        objects += page.get("Contents", [])
    with connections.dwh() as conn:
        done = {
            (k, e)
            for k, e in conn.execute(
                "SELECT object_key, etag FROM meta.processed_files WHERE pipeline = %s",
                (pipeline.name,),
            )
        }
        columns = stg_columns(conn, pipeline.target)

    def archive(key: str) -> None:
        if source.archive_bucket:
            s3.copy_object(
                Bucket=source.archive_bucket, Key=key, CopySource={"Bucket": source.bucket, "Key": key}
            )
            s3.delete_object(Bucket=source.bucket, Key=key)

    # тот же файл с тем же содержимым прислали повторно: грузить нечего,
    # но и оставлять его в landing нельзя — убираем в архив
    for o in objects:
        if (o["Key"], o["ETag"].strip('"')) in done:
            log.info("%s: %s уже загружен, переношу в архив", pipeline.name, o["Key"])
            archive(o["Key"])

    # имена файлов содержат дату, поэтому сортировка по ключу = хронология
    todo = sorted(
        (o for o in objects if (o["Key"], o["ETag"].strip('"')) not in done),
        key=lambda o: o["Key"],
    )[: source.max_files_per_run]
    parser = PARSERS[source.parser]

    for obj in todo:
        key, etag = obj["Key"], obj["ETag"].strip('"')
        body = s3.get_object(Bucket=source.bucket, Key=key)["Body"].read()
        try:
            with connections.dwh() as conn:
                batch_id = _open_batch(conn, pipeline, source_object=key)
                rows = copy_rows(
                    conn,
                    pipeline.target,
                    columns,
                    ([r[c] for c in columns] for r in parser(body)),
                    batch_id,
                )
                _close_batch(conn, batch_id, rows)
                conn.execute(
                    "INSERT INTO meta.processed_files "
                    "(object_key, etag, pipeline, batch_id, rows_loaded) VALUES (%s, %s, %s, %s, %s)",
                    (key, etag, pipeline.name, batch_id, rows),
                )
                conn.commit()
        except ParseError as exc:
            # плохой файл не должен блокировать остальные: он остаётся в landing,
            # попадает в журнал как failed и в отчёт задачи
            log.error("%s: файл %s отложен: %s", pipeline.name, key, exc)
            _fail_batch(pipeline, str(exc), source_object=key)
            result.skipped_files.append(key)
            continue
        archive(key)
        result.batches.append(batch_id)
        result.rows += rows
        log.info("%s: %s -> партия %s, %s строк", pipeline.name, key, batch_id, rows)


# ---------------------------------------------------------------------- API
def _fetch_nbk(client: httpx.Client, day: date) -> bytes:
    for attempt in range(4):
        try:
            resp = client.get(NBK_URL, params={"fdate": day.strftime("%d.%m.%Y")})
            resp.raise_for_status()
            return resp.content
        except httpx.HTTPError as exc:
            if attempt == 3:
                raise
            wait = 2**attempt
            log.warning("НБРК %s: %s, повтор через %s с", day, exc, wait)
            time.sleep(wait)
    raise AssertionError("unreachable")


def _run_nbk(pipeline: Pipeline, source: HttpSource, result: RunResult) -> None:
    with connections.dwh() as conn:
        columns = stg_columns(conn, pipeline.target)
        row = conn.execute(
            "SELECT value FROM meta.watermarks WHERE pipeline = %s", (pipeline.name,)
        ).fetchone()
        start = (
            date.fromisoformat(row[0]) + timedelta(days=1) if row else date.fromisoformat(source.start_date)
        )
        today = datetime.now(ZoneInfo("Asia/Almaty")).date()
        days = [start + timedelta(days=i) for i in range((today - start).days + 1)]
        days = days[: source.max_days_per_run]
        if not days:
            log.info("%s: курсы уже загружены по %s", pipeline.name, today)
            return

        loaded_until = None

        def rows():
            nonlocal loaded_until
            with httpx.Client(timeout=20, headers={"User-Agent": "retail-dwh/0.1"}) as client:
                for day in days:
                    parsed = list(parse_nbk_rates(_fetch_nbk(client, day)))
                    if not parsed:
                        # курсов на дату ещё нет — дальше не идём, чтобы не
                        # сдвинуть водяной знак за «дыру»
                        break
                    for r in parsed:
                        yield [r[c] for c in columns]
                    loaded_until = day
                    time.sleep(0.05)

        batch_id = _open_batch(conn, pipeline, watermark_from=start.isoformat())
        n = copy_rows(conn, pipeline.target, columns, rows(), batch_id)
        _close_batch(conn, batch_id, n, watermark_to=loaded_until.isoformat() if loaded_until else None)
        if loaded_until:
            conn.execute(
                "INSERT INTO meta.watermarks (pipeline, value) VALUES (%s, %s) "
                "ON CONFLICT (pipeline) DO UPDATE SET value = excluded.value, updated_at = now()",
                (pipeline.name, loaded_until.isoformat()),
            )
        conn.commit()
    result.batches.append(batch_id)
    result.rows += n
    log.info("%s: партия %s, %s строк по %s", pipeline.name, batch_id, n, loaded_until)


def run_pipeline(name: str) -> RunResult:
    pipeline = load_pipelines()[name]
    result = RunResult(name)
    started = time.monotonic()
    source = pipeline.source
    try:
        if isinstance(source, DbSource):
            _run_db(pipeline, source, result)
        elif isinstance(source, FilesSource):
            _run_files(pipeline, source, result)
        elif isinstance(source, HttpSource):
            _run_nbk(pipeline, source, result)
    except Exception as exc:
        if not isinstance(source, FilesSource):
            _fail_batch(pipeline, repr(exc))
        metrics.push_ingest(name, result.rows, time.monotonic() - started, ok=False)
        raise
    metrics.push_ingest(name, result.rows, time.monotonic() - started, ok=True)
    return result
