"""Каталог данных и lineage.

Метаданные (описание, владелец, PII) — из catalog/datasets.yml. Связи
между таблицами вычисляются из того, что реально исполняется:
- pipelines/*.yml       источник -> stg
- models/vault.yml      stg -> хабы, линки, сателлиты
- sql/marts/*.sql       разбор sqlglot: какие таблицы шаг читает и в какие пишет
- публикации            витрины -> ClickHouse, Kafka -> Delta
Поэтому lineage не расходится с кодом: поменял SQL — поменялся граф.
"""

from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import dataclass, field
from functools import lru_cache

import sqlglot
import yaml
from sqlglot import exp

from retail_dwh.ingest.config import DbSource, FilesSource, load_pipelines
from retail_dwh.settings import PROJECT_DIR
from retail_dwh.vault.model import SCHEMA, load_model

# связи вне SQL-файлов: публикации и поток
STATIC_EDGES = [
    ("marts.v_sales_wide", "clickhouse.retail.sales_wide", "publish_clickhouse"),
    ("kafka.web.events", "lake.web.events", "web_events"),
    ("lake.web.events", "lake.web.sessions", "web_events"),
    ("lake.web.sessions", "clickhouse.retail.web_funnel_daily", "web_events"),
]


@dataclass(frozen=True)
class Edge:
    source: str
    target: str
    via: str  # что создаёт связь: пайплайн, модель vault, шаг сборки


@dataclass
class Lineage:
    edges: list[Edge] = field(default_factory=list)

    def upstream(self, table: str) -> set[str]:
        return self._walk(table, lambda e: e.target, lambda e: e.source)

    def downstream(self, table: str) -> set[str]:
        return self._walk(table, lambda e: e.source, lambda e: e.target)

    def _walk(self, start: str, key, nxt) -> set[str]:
        index = defaultdict(list)
        for e in self.edges:
            index[key(e)].append(nxt(e))
        seen: set[str] = set()
        stack = [start]
        while stack:
            for n in index[stack.pop()]:
                if n not in seen:
                    seen.add(n)
                    stack.append(n)
        return seen


def _sql_tables(sql: str) -> tuple[set[str], set[str]]:
    """(что читает, во что пишет) — только постоянные таблицы со схемой."""
    sql = re.sub(r"%\(\w+\)s", "NULL", sql)
    # sqlglot не знает ON COMMIT DROP и разбирает такую команду как «чёрный
    # ящик» — для lineage опция не важна, убираем её
    sql = re.sub(r"ON COMMIT DROP", "", sql, flags=re.I)
    reads: set[str] = set()
    writes: set[str] = set()
    for stmt in sqlglot.parse(sql, read="postgres"):
        if stmt is None:
            continue
        targets = set()
        if isinstance(stmt, exp.Insert | exp.Merge | exp.Update | exp.Delete):
            target = stmt.this.find(exp.Table) if not isinstance(stmt.this, exp.Table) else stmt.this
            if target is not None and target.db:
                targets.add(f"{target.db}.{target.name}")
        for table in stmt.find_all(exp.Table):
            name = f"{table.db}.{table.name}" if table.db else None
            if name and name not in targets:
                reads.add(name)
        writes |= targets
    return reads - writes, writes


def build_lineage() -> Lineage:
    lineage = Lineage()
    for p in load_pipelines().values():
        src = p.source
        if isinstance(src, DbSource):
            origin = f"{src.connection}:{PROJECT_DIR.joinpath(src.query).stem}"
        elif isinstance(src, FilesSource):
            origin = f"s3://{src.bucket}/{src.prefix}"
        else:
            origin = "api.nationalbank.kz"
        lineage.edges.append(Edge(origin, p.target, f"pipeline:{p.name}"))

    model = load_model()
    for table, src in model.sources.items():
        targets = {model.hubs[h].table for h in src.keys}
        targets |= {model.links[n].table for n in (*src.links, *src.non_historized_links)}
        targets |= set(src.satellites)
        for t in sorted(targets):
            lineage.edges.append(Edge(table, f"{SCHEMA}.{t}", "vault_model"))
    for table, ref in model.references.items():
        lineage.edges.append(Edge(table, f"{SCHEMA}.{ref.table}", "vault_model"))

    for path in sorted((PROJECT_DIR / "sql").glob("*/[0-9][0-9]*_*.sql")):
        if path.parent.name not in {"marts", "dwh"}:
            continue
        text = path.read_text(encoding="utf-8")
        if path.parent.name == "dwh":
            # из миграций берём только представления: у них есть «источники»
            for m in re.finditer(r"CREATE VIEW (\w+\.\w+) AS(.*?);", text, re.S | re.I):
                reads, _ = _sql_tables(m.group(2))
                lineage.edges += [Edge(r, m.group(1), f"view:{path.stem}") for r in sorted(reads)]
            continue
        reads, writes = _sql_tables(text)
        for w in sorted(writes):
            lineage.edges += [Edge(r, w, f"marts:{path.stem}") for r in sorted(reads) if r != w]

    lineage.edges += [Edge(s, t, v) for s, t, v in STATIC_EDGES]
    return lineage


@lru_cache
def datasets() -> dict[str, dict]:
    return yaml.safe_load((PROJECT_DIR / "catalog/datasets.yml").read_text(encoding="utf-8"))
