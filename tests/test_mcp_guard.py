import pytest

from retail_dwh.mcp_server import QueryRejected, check_query


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT count(*) FROM dwh.marts.fact_sales",
        "WITH x AS (SELECT 1 AS a) SELECT a FROM x",
        "SELECT * FROM ch.retail.sales_wide s JOIN dwh.marts.dim_store d ON d.store_code = s.store_code",
        "SELECT event_type, count(*) FROM lake.web.events GROUP BY 1 ORDER BY 2 DESC",
    ],
)
def test_allows_reads(sql):
    assert check_query(sql)


@pytest.mark.parametrize(
    ("sql", "reason"),
    [
        ("DELETE FROM dwh.marts.dim_date", "только SELECT"),
        ("DROP TABLE dwh.marts.dim_date", "только SELECT"),
        ("INSERT INTO ch.retail.sales_wide SELECT * FROM ch.retail.sales_wide", "только SELECT"),
        ("SELECT 1; DELETE FROM dwh.marts.dim_date", "ровно один"),
        ("SELECT * FROM erp.erp.orders", "каталог erp закрыт"),
        ("SELECT * FROM marts.fact_sales", "укажите каталог"),
        ("SELECT * FROM (SELECT * FROM system.runtime.queries) q", "каталог system закрыт"),
    ],
)
def test_rejects(sql, reason):
    with pytest.raises(QueryRejected, match=reason):
        check_query(sql)
