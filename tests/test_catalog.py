import re

from retail_dwh.catalog import build_lineage, datasets
from retail_dwh.settings import PROJECT_DIR

PII_TABLE = "raw_vault.sat_customer_pii"


def test_pii_never_reaches_marts():
    """Из сателлита с ФИО, телефонами и почтой ничего не должно течь дальше."""
    downstream = build_lineage().downstream(PII_TABLE)
    assert not downstream, f"PII утекает в: {sorted(downstream)}"


def test_pii_columns_absent_from_marts_ddl():
    ddl = "\n".join(p.read_text(encoding="utf-8") for p in (PROJECT_DIR / "sql/dwh").glob("*.sql"))
    marts = ddl[ddl.index("CREATE SCHEMA IF NOT EXISTS marts") :]
    for column in ("email ", "phone ", "first_name", "last_name", "birth_date"):
        assert column not in marts, column


def test_every_mart_table_is_in_catalog():
    ddl = (PROJECT_DIR / "sql/dwh/003_marts.sql").read_text(encoding="utf-8")
    tables = set(re.findall(r"CREATE (?:TABLE|VIEW) (marts\.\w+)", ddl))
    # части снежинки и секции факта описаны вместе с родителем
    tables = {t for t in tables if not t.startswith(("marts.dim_product_", "marts.fact_sales_"))}
    missing = tables - set(datasets())
    assert not missing, f"нет описания в catalog/datasets.yml: {sorted(missing)}"


def test_catalog_entries_have_owner_and_pii_class():
    for name, meta in datasets().items():
        assert meta.get("owner"), name
        assert meta.get("pii") in {"none", "pseudonymized", "direct"}, name


def test_fact_sales_lineage_reaches_all_sources():
    upstream = build_lineage().upstream("marts.fact_sales")
    for source in (
        "erp:erp_orders",
        "erp:erp_order_lines",
        "crm:crm_customers",
        "s3://landing/catalog/",
        "api.nationalbank.kz",
    ):
        assert source in upstream, source


def test_lineage_parses_every_marts_step():
    vias = {e.via for e in build_lineage().edges}
    for path in (PROJECT_DIR / "sql/marts").glob("*.sql"):
        assert f"marts:{path.stem}" in vias, f"{path.name} не дал ни одной связи — sqlglot не разобрал?"
