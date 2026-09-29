"""SQL для raw_vault, сгенерированный из модели: DDL и загрузка одной партии.

Загрузка партии устроена так:
1. из stg-таблицы берётся партия, считаются ключи, хеши и атрибуты — всё
   один раз, во временную таблицу _src;
2. хабы и линки получают только новые ключи (ON CONFLICT DO NOTHING);
3. в сателлит попадает запись, только если её hashdiff отличается от
   последней версии этого ключа. Последняя версия ищется точечно по индексу
   (LATERAL ... LIMIT 1), а не DISTINCT ON по всему сателлиту — на больших
   сателлитах разница в разы, замер в docs/performance.md.
"""

from __future__ import annotations

from retail_dwh.vault.model import (
    SCHEMA,
    Link,
    Reference,
    Satellite,
    SourceMapping,
    VaultModel,
)


def _bk(expr: str) -> str:
    return f"nullif(trim(({expr})::text), '')"


def _hash(parts: list[str]) -> str:
    joined = ", ".join(f"upper({p})" for p in parts)
    return f"md5(concat_ws('||', {joined}))::uuid"


def _hashdiff(columns: list[str]) -> str:
    parts = ", ".join(f"coalesce({c}::text, '')" for c in columns)
    return f"md5(concat_ws('||', {parts}))::uuid"


# ------------------------------------------------------------------------ DDL
def ddl(model: VaultModel) -> str:
    out = [f"CREATE SCHEMA IF NOT EXISTS {SCHEMA};"]
    for hub in model.hubs.values():
        out.append(
            f"CREATE TABLE IF NOT EXISTS {SCHEMA}.{hub.table} (\n"
            f"    {hub.hk} uuid PRIMARY KEY,\n"
            f"    {hub.business_key} text NOT NULL,\n"
            "    load_dts timestamptz NOT NULL,\n"
            "    record_source text NOT NULL\n);"
        )
    for link in model.links.values():
        cols = [f"    {link.hk} uuid PRIMARY KEY"]
        cols += [f"    hk_{h} uuid NOT NULL" for h in link.hubs]
        if link.dependent_key and link.dependent_key not in link.columns:
            cols.append(f"    {link.dependent_key} text NOT NULL")
        cols += [f"    {c} {t}" for c, t in link.columns.items()]
        cols += ["    load_dts timestamptz NOT NULL", "    record_source text NOT NULL"]
        out.append(f"CREATE TABLE IF NOT EXISTS {SCHEMA}.{link.table} (\n" + ",\n".join(cols) + "\n);")
        for h in link.hubs:
            out.append(f"CREATE INDEX IF NOT EXISTS {link.table}_{h}_ix ON {SCHEMA}.{link.table} (hk_{h});")
    for sat in model.satellites.values():
        hk = model.parent_hk(sat)
        cols = [
            f"    {hk} uuid NOT NULL",
            "    load_dts timestamptz NOT NULL",
            "    applied_ts timestamptz",
            "    hashdiff uuid NOT NULL",
        ]
        cols += [f"    {c} {t}" for c, t in sat.columns.items()]
        cols += ["    record_source text NOT NULL", f"    PRIMARY KEY ({hk}, load_dts)"]
        out.append(f"CREATE TABLE IF NOT EXISTS {SCHEMA}.{sat.name} (\n" + ",\n".join(cols) + "\n);")
        if sat.pii:
            out.append(f"COMMENT ON TABLE {SCHEMA}.{sat.name} IS 'PII: доступ только у роли etl';")
    for ref in model.references.values():
        cols = [f"    {c} {t}" for c, t in ref.columns.items()]
        cols += [
            "    load_dts timestamptz NOT NULL",
            "    record_source text NOT NULL",
            f"    PRIMARY KEY ({', '.join(ref.key)})",
        ]
        out.append(f"CREATE TABLE IF NOT EXISTS {SCHEMA}.{ref.table} (\n" + ",\n".join(cols) + "\n);")
    return "\n\n".join(out) + "\n"


# ------------------------------------------------------------------- загрузка
def _src_table(model: VaultModel, src: SourceMapping) -> str:
    cols = [f"({src.applied_ts}) AS applied_ts"]
    for hub, expr in src.keys.items():
        cols.append(f"{_bk(expr)} AS bk_{hub}")
    for dep, expr in src.dependent_keys.items():
        cols.append(f"{_bk(expr)} AS dk_{dep}")
    for name in (*src.links, *src.non_historized_links):
        link = model.links[name]
        parts = [_bk(src.keys[h]) for h in link.hubs]
        if link.dependent_key:
            parts.append(_bk(src.dependent_keys[link.dependent_key]))
        cols.append(f"{_hash(parts)} AS hk_{name}")
    for hub, expr in src.keys.items():
        cols.append(f"md5(upper({_bk(expr)}))::uuid AS hk_{hub}")
    for sat_name, mapping in src.satellites.items():
        sat = model.satellites[sat_name]
        for col, expr in mapping.items():
            cols.append(f"({expr})::{sat.columns[col]} AS {sat_name}__{col}")
    for name, mapping in src.non_historized_links.items():
        link = model.links[name]
        for col, expr in mapping.items():
            cols.append(f"({expr})::{link.columns[col]} AS {name}__{col}")
    select = ",\n    ".join(cols)
    return (
        "CREATE TEMP TABLE _src ON COMMIT DROP AS\n"
        f"SELECT\n    {select}\nFROM {src.table}\nWHERE _batch_id = %(batch_id)s;"
    )


def _hub_insert(model: VaultModel, hub_name: str) -> str:
    hub = model.hubs[hub_name]
    return (
        f"INSERT INTO {SCHEMA}.{hub.table} ({hub.hk}, {hub.business_key}, load_dts, record_source)\n"
        f"SELECT DISTINCT ON (hk_{hub_name}) hk_{hub_name}, bk_{hub_name}, now(), %(record_source)s\n"
        f"FROM _src WHERE bk_{hub_name} IS NOT NULL\n"
        f"ORDER BY hk_{hub_name}\n"
        f"ON CONFLICT ({hub.hk}) DO NOTHING;"
    )


def _link_insert(link: Link) -> str:
    hub_cols = [f"hk_{h}" for h in link.hubs]
    cols = [link.hk, *hub_cols]
    select = [f"hk_{link.name}", *hub_cols]
    if link.dependent_key and link.dependent_key not in link.columns:
        cols.append(link.dependent_key)
        select.append(f"dk_{link.dependent_key}")
    for c in link.columns:
        cols.append(c)
        select.append(f"{link.name}__{c}")
    not_null = " AND ".join(f"bk_{h} IS NOT NULL" for h in link.hubs)
    return (
        f"INSERT INTO {SCHEMA}.{link.table} ({', '.join(cols)}, load_dts, record_source)\n"
        f"SELECT DISTINCT ON (hk_{link.name}) {', '.join(select)}, now(), %(record_source)s\n"
        f"FROM _src WHERE {not_null}\n"
        f"ORDER BY hk_{link.name}, applied_ts DESC\n"
        f"ON CONFLICT ({link.hk}) DO NOTHING;"
    )


def _sat_insert(model: VaultModel, sat: Satellite) -> str:
    hk = model.parent_hk(sat)
    src_cols = [f"{sat.name}__{c}" for c in sat.columns]
    return (
        f"INSERT INTO {SCHEMA}.{sat.name} "
        f"({hk}, load_dts, applied_ts, hashdiff, {', '.join(sat.columns)}, record_source)\n"
        f"SELECT s.hk, now(), s.applied_ts, s.hashdiff, {', '.join('s.' + c for c in src_cols)}, "
        "%(record_source)s\n"
        "FROM (\n"
        f"    SELECT DISTINCT ON ({hk}) {hk} AS hk, applied_ts,\n"
        f"           {_hashdiff(src_cols)} AS hashdiff, {', '.join(src_cols)}\n"
        f"    FROM _src WHERE {hk} IS NOT NULL\n"
        f"    ORDER BY {hk}, applied_ts DESC\n"
        ") s\n"
        "LEFT JOIN LATERAL (\n"
        f"    SELECT t.hashdiff FROM {SCHEMA}.{sat.name} t\n"
        f"    WHERE t.{hk} = s.hk ORDER BY t.load_dts DESC LIMIT 1\n"
        ") cur ON true\n"
        "WHERE cur.hashdiff IS DISTINCT FROM s.hashdiff;"
    )


def _reference_upsert(ref: Reference) -> str:
    cols = list(ref.columns)
    select = ", ".join(f"({ref.mapping[c]})::{ref.columns[c]} AS {c}" for c in cols)
    updates = ", ".join(f"{c} = excluded.{c}" for c in cols if c not in ref.key)
    changed = " OR ".join(f"t.{c} IS DISTINCT FROM excluded.{c}" for c in cols if c not in ref.key)
    return (
        f"INSERT INTO {SCHEMA}.{ref.table} AS t ({', '.join(cols)}, load_dts, record_source)\n"
        f"SELECT DISTINCT ON ({', '.join(ref.key)}) *, now(), %(record_source)s FROM (\n"
        f"    SELECT {select} FROM {ref.source_table} WHERE _batch_id = %(batch_id)s\n"
        f") s ORDER BY {', '.join(ref.key)}\n"
        f"ON CONFLICT ({', '.join(ref.key)}) DO UPDATE SET {updates}, load_dts = now()\n"
        f"WHERE {changed};"
    )


def load_statements(model: VaultModel, table: str) -> list[str]:
    """Упорядоченный список команд, загружающих одну партию из stg-таблицы."""
    if table in model.references:
        return [_reference_upsert(model.references[table])]
    src = model.sources[table]
    stmts = [_src_table(model, src)]
    stmts += [_hub_insert(model, h) for h in src.keys]
    stmts += [_link_insert(model.links[name]) for name in src.links]
    stmts += [_link_insert(model.links[name]) for name in src.non_historized_links]
    stmts += [_sat_insert(model, model.satellites[name]) for name in src.satellites]
    return stmts
