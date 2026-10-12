"""Правила доступа Trino (docker/trino/etc/rules.json).

Trino проверяет правила сверху вниз до первого совпадения, а если правил
для процедур нет вовсе, разрешены только system.builtin. Из-за этого
register_table однажды тихо перестал работать — тест держит это место.
"""

from __future__ import annotations

import json
import re

import pytest

from retail_dwh.settings import PROJECT_DIR

RULES = json.loads((PROJECT_DIR / "docker/trino/etc/rules.json").read_text(encoding="utf-8"))


def _match(rule: dict, key: str, value: str) -> bool:
    return re.fullmatch(rule.get(key, ".*"), value) is not None


def can_execute(user: str, catalog: str, schema: str, procedure: str) -> bool:
    for rule in RULES.get("procedures", []):
        if all(
            _match(rule, k, v)
            for k, v in (("user", user), ("catalog", catalog), ("schema", schema), ("procedure", procedure))
        ):
            return "EXECUTE" in rule["privileges"]
    return False


def catalog_access(user: str, catalog: str) -> str:
    for rule in RULES["catalogs"]:
        if _match(rule, "user", user) and _match(rule, "catalog", catalog):
            return rule["allow"]
    return "none"


def test_pipeline_can_register_delta_tables():
    assert can_execute("retail-dwh", "lake", "system", "register_table")


@pytest.mark.parametrize(
    ("user", "procedure"),
    [
        ("mcp-agent", "register_table"),
        ("retail-dwh", "unregister_table"),
        ("retail-dwh", "vacuum"),
        ("analyst", "register_table"),
    ],
)
def test_other_procedures_are_closed(user, procedure):
    assert not can_execute(user, "lake", "system", procedure)


@pytest.mark.parametrize(
    ("catalog", "expected"),
    [("erp", "none"), ("dwh", "read-only"), ("lake", "read-only"), ("ch", "read-only")],
)
def test_mcp_agent_catalogs(catalog, expected):
    assert catalog_access("mcp-agent", catalog) == expected
