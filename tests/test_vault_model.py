from pathlib import Path

import pytest
import yaml

from retail_dwh.settings import PROJECT_DIR
from retail_dwh.vault.model import ModelError, load_model
from retail_dwh.vault.render import ddl, load_statements


def test_project_model_is_valid():
    model = load_model()
    assert {"customer", "order", "product", "store", "promo"} <= set(model.hubs)
    assert "stg.fx_rates" in model.references


def test_every_source_renders():
    model = load_model()
    for table in model.loadable_tables:
        stmts = load_statements(model, table)
        assert stmts, table
        assert "%(batch_id)s" in stmts[0]


def test_hash_keys_are_case_and_space_insensitive():
    sql = load_statements(load_model(), "stg.erp_orders")[0]
    # бизнес-ключ приводится к верхнему регистру и обрезается до хеширования
    assert "md5(upper(nullif(trim((order_no)::text), '')))::uuid AS hk_order" in sql


def test_satellite_only_inserts_changes():
    model = load_model()
    sat_sql = next(
        s
        for s in load_statements(model, "stg.crm_customers")
        if s.startswith("INSERT INTO raw_vault.sat_customer_crm")
    )
    assert "LEFT JOIN LATERAL" in sat_sql
    assert "cur.hashdiff IS DISTINCT FROM s.hashdiff" in sat_sql


def test_pii_satellite_is_marked():
    assert "COMMENT ON TABLE raw_vault.sat_customer_pii IS 'PII" in ddl(load_model())


def _broken_model(tmp_path: Path, patch) -> Path:
    raw = yaml.safe_load((PROJECT_DIR / "models/vault.yml").read_text(encoding="utf-8"))
    patch(raw)
    path = tmp_path / "vault.yml"
    path.write_text(yaml.safe_dump(raw, allow_unicode=True), encoding="utf-8")
    return path


def test_validation_catches_unknown_hub(tmp_path):
    path = _broken_model(tmp_path, lambda m: m["links"]["order_line"]["hubs"].append("nope"))
    with pytest.raises(ModelError, match="нет хаба nope"):
        load_model(path)


def test_validation_catches_column_mismatch(tmp_path):
    def patch(m):
        del m["sources"]["stg.crm_customers"]["satellites"]["sat_customer_crm"]["city"]

    with pytest.raises(ModelError, match="sat_customer_crm"):
        load_model(_broken_model(tmp_path, patch))
