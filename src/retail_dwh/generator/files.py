"""Файлы, которые внешние участники кладут в landing-бакет.

- каталог поставщика: XML, полный снимок;
- остатки склада: CSV в gzip, разделитель «;», дробная часть через запятую —
  так выгружает учётная система склада;
- промо-акции маркетинга: вложенный JSON.
"""

from __future__ import annotations

import csv
import gzip
import io
import json
from datetime import date, timedelta
from xml.sax.saxutils import escape, quoteattr

from retail_dwh.generator.entities import promo_for_week
from retail_dwh.generator.world import World, stable_rand

LANDING_BUCKET = "landing"


def catalog_key(day: date) -> str:
    return f"catalog/catalog_{day:%Y-%m-%d}.xml"


def stock_key(day: date) -> str:
    return f"stock/stock_{day:%Y-%m-%d}.csv.gz"


def promo_key(monday: date) -> str:
    return f"promo/promo_{monday:%Y-%m-%d}.json"


def catalog_xml(world: World, day: date) -> bytes:
    out = io.StringIO()
    out.write('<?xml version="1.0" encoding="UTF-8"?>\n')
    out.write(f'<catalog feed="retail-master" snapshot_date="{day:%Y-%m-%d}">\n')
    for p in world.products:
        if p.launched_on > day:
            continue
        status = "active"
        if p.discontinued_on and p.discontinued_on <= day:
            # выведенный товар ещё 60 дней висит в каталоге со статусом, потом исчезает
            if (day - p.discontinued_on).days > 60:
                continue
            status = "discontinued"
        rnd = stable_rand(world.seed, "ean", p.sku)
        eans = "".join(
            f"<ean>{rnd.randint(4600000000000, 4899999999999)}</ean>" for _ in range(rnd.choice([1, 1, 1, 2]))
        )
        out.write(
            f'  <product sku="{p.sku}" status="{status}">'
            f"<name>{escape(p.name)}</name>"
            f"<brand>{escape(p.brand)}</brand>"
            f"<category group={quoteattr(p.group)} category={quoteattr(p.category)}>"
            f"{escape(p.subcategory)}</category>"
            f"<unit>{p.unit}</unit>"
            f'<supplier code="{p.supplier_code}">'
            f'<purchase_price currency="{p.purchase_currency}">'
            f"{world.purchase_price(p, day):.2f}</purchase_price></supplier>"
            f'<retail_price currency="KZT">{world.retail_price(p, day):.0f}</retail_price>'
            f"<barcodes>{eans}</barcodes>"
            "</product>\n"
        )
    out.write("</catalog>\n")
    return out.getvalue().encode("utf-8")


def stock_csv(world: World, day: date) -> bytes:
    rnd = stable_rand(world.seed, "stock", day.isoformat())
    buf = io.StringIO()
    writer = csv.writer(buf, delimiter=";", lineterminator="\r\n")
    writer.writerow(["Дата", "Код магазина", "Артикул", "Остаток", "Себестоимость остатка"])
    products = world.active_products(day)
    for store in world.stores:
        if store.format == "ONLINE" or not world.store_is_open(store, day):
            continue
        for p in products:
            if rnd.random() < 0.15:
                continue  # товар в этом магазине не представлен
            qty = max(0.0, rnd.gauss(p.popularity * store.weight * 6, 4))
            if p.unit == "шт":
                qty = float(round(qty))
            cost = qty * world.retail_price(p, day) * p.purchase_ratio
            writer.writerow(
                [
                    f"{day:%d.%m.%Y}",
                    store.store_code,
                    p.sku,
                    f"{qty:.3f}".replace(".", ","),
                    f"{cost:.2f}".replace(".", ","),
                ]
            )
    # BOM в начале файла — так сохраняет Excel, парсер обязан это пережить
    return gzip.compress(("﻿" + buf.getvalue()).encode("utf-8"))


def promo_json(world: World, monday: date) -> bytes:
    items = promo_for_week(world.seed, monday)
    doc = {
        "campaign": {
            "id": f"PROMO-{monday:%Y%m%d}",
            "title": f"Скидки недели с {monday:%d.%m}",
            "channels": ["STORE", "WEB"],
            "period": {"from": monday.isoformat(), "to": (monday + timedelta(days=6)).isoformat()},
        },
        "items": [{"sku": sku, "mechanics": {"type": "percent", "value": pct}} for sku, pct in items],
    }
    return json.dumps(doc, ensure_ascii=False, indent=2).encode("utf-8")
