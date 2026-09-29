import gzip
import json
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest
from defusedxml import EntitiesForbidden

from retail_dwh.generator import files
from retail_dwh.generator.world import World
from retail_dwh.ingest.parsers import (
    ParseError,
    parse_catalog_xml,
    parse_nbk_rates,
    parse_promo_json,
    parse_stock_csv,
)

FIXTURES = Path(__file__).parent / "fixtures"


def test_nbk_rates_real_response():
    rows = list(parse_nbk_rates((FIXTURES / "nbk_rates_2026-09-25.xml").read_bytes()))
    by_code = {r["currency"]: r for r in rows}
    assert len(rows) == 48
    assert by_code["USD"] == {
        "rate_date": date(2026, 9, 25),
        "currency": "USD",
        "rate": Decimal("441.89"),
        "quant": 1,
    }
    # курс воны дан за 100 единиц
    assert by_code["KRW"]["quant"] == 100


def test_nbk_rates_no_data_is_not_an_error():
    assert list(parse_nbk_rates((FIXTURES / "nbk_rates_no_data.xml").read_bytes())) == []


def test_catalog_xml_roundtrip_with_generator():
    world = World(42)
    rows = list(parse_catalog_xml(files.catalog_xml(world, date(2026, 9, 1))))
    assert rows, "каталог пустой"
    row = next(r for r in rows if r["sku"] == "SKU-000001")
    product = world.product_by_sku["SKU-000001"]
    assert row["group_name"] == product.group
    assert row["subcategory_name"] == product.subcategory
    assert row["retail_price"] == Decimal(str(int(world.retail_price(product, date(2026, 9, 1)))))
    assert row["purchase_currency"] == product.purchase_currency
    assert row["eans"] and all(len(e) == 13 for e in row["eans"])


def test_catalog_xml_escapes_and_missing_parts():
    xml = (
        '<?xml version="1.0"?><catalog snapshot_date="2026-01-05">'
        '<product sku=" SKU-1 "><name>Чай &amp; кофе</name></product></catalog>'
    ).encode()
    [row] = parse_catalog_xml(xml)
    assert row["sku"] == "SKU-1"
    assert row["name"] == "Чай & кофе"
    assert row["retail_price"] is None and row["eans"] == []


def test_catalog_xml_rejects_entity_bomb():
    bomb = b'<?xml version="1.0"?><!DOCTYPE x [<!ENTITY a "aaaa">]><catalog>&a;</catalog>'
    with pytest.raises(EntitiesForbidden):
        list(parse_catalog_xml(bomb))


def test_stock_csv_bom_decimal_comma_gzip():
    body = (
        "\ufeffДата;Код магазина;Артикул;Остаток;Себестоимость остатка\r\n"
        "28.09.2026;S001;SKU-000001;12,500;1 234,50\r\n"
    )
    [row] = parse_stock_csv(gzip.compress(body.encode("utf-8")))
    assert row == {
        "snapshot_date": date(2026, 9, 28),
        "store_code": "S001",
        "sku": "SKU-000001",
        "qty_on_hand": Decimal("12.500"),
        "stock_cost": Decimal("1234.50"),
    }


def test_stock_csv_missing_column():
    with pytest.raises(ParseError, match="нет колонок"):
        list(parse_stock_csv("Дата;Артикул\n01.01.2026;X\n".encode()))


def test_stock_csv_bad_number_points_to_line():
    body = "Дата;Код магазина;Артикул;Остаток;Себестоимость остатка\n01.01.2026;S1;X;abc;1\n"
    with pytest.raises(ParseError, match="строка 2"):
        list(parse_stock_csv(body.encode()))


def test_promo_json_flattens_items():
    doc = {
        "campaign": {
            "id": "P1",
            "title": "t",
            "channels": ["WEB"],
            "period": {"from": "2026-09-28", "to": "2026-10-04"},
        },
        "items": [
            {"sku": "A", "mechanics": {"type": "percent", "value": 15}},
            {"sku": "B", "mechanics": {"type": "gift"}},
        ],
    }
    rows = list(parse_promo_json(json.dumps(doc).encode()))
    assert [r["sku"] for r in rows] == ["A", "B"]
    assert rows[0]["discount_pct"] == Decimal("15") and rows[1]["discount_pct"] is None
    assert rows[0]["valid_to"] == date(2026, 10, 4)


def test_promo_json_requires_campaign():
    with pytest.raises(ParseError):
        list(parse_promo_json(b'{"items": []}'))
