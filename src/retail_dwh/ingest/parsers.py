"""Разбор файлов и ответов API в строки для stg.

Каждый парсер — генератор словарей, ключи совпадают с колонками stg-таблицы.
XML разбирается через defusedxml: файл приходит от внешнего поставщика, а
стандартный парсер уязвим к «бомбам» из вложенных сущностей.
"""

from __future__ import annotations

import csv
import gzip
import io
import json
from collections.abc import Iterator
from datetime import date, datetime
from decimal import Decimal, InvalidOperation

from defusedxml import ElementTree


class ParseError(ValueError):
    """Файл нельзя разобрать целиком — его надо отложить, а не грузить частично."""


def _decimal(value: str | None, *, comma: bool = False) -> Decimal | None:
    if value is None or not value.strip():
        return None
    text = value.strip().replace(" ", "").replace(" ", "")
    if comma:
        text = text.replace(",", ".")
    try:
        return Decimal(text)
    except InvalidOperation as exc:
        raise ParseError(f"не число: {value!r}") from exc


def _text(el, path: str) -> str | None:
    found = el.find(path)
    if found is None or found.text is None:
        return None
    return found.text.strip() or None


def _own_text(el) -> str | None:
    if el is None or el.text is None:
        return None
    return el.text.strip() or None


# ------------------------------------------------------------ каталог (XML)
def parse_catalog_xml(data: bytes) -> Iterator[dict]:
    try:
        root = ElementTree.fromstring(data)
    except ElementTree.ParseError as exc:
        raise ParseError(f"битый XML: {exc}") from exc
    if root.tag != "catalog":
        raise ParseError(f"ожидался <catalog>, пришёл <{root.tag}>")
    snapshot = date.fromisoformat(root.attrib["snapshot_date"])
    for product in root.iter("product"):
        category = product.find("category")
        supplier = product.find("supplier")
        purchase = supplier.find("purchase_price") if supplier is not None else None
        yield {
            "snapshot_date": snapshot,
            "sku": product.attrib["sku"].strip(),
            "status": product.attrib.get("status", "active"),
            "name": _text(product, "name"),
            "brand": _text(product, "brand"),
            "group_name": category.attrib.get("group") if category is not None else None,
            "category_name": category.attrib.get("category") if category is not None else None,
            "subcategory_name": _own_text(category),
            "unit": _text(product, "unit"),
            "supplier_code": supplier.attrib.get("code") if supplier is not None else None,
            "purchase_price": _decimal(purchase.text) if purchase is not None else None,
            "purchase_currency": purchase.attrib.get("currency") if purchase is not None else None,
            "retail_price": _decimal(_text(product, "retail_price")),
            "eans": [e.text.strip() for e in product.iterfind("barcodes/ean") if e.text],
        }


# ------------------------------------------------------------- остатки (CSV)
STOCK_COLUMNS = {
    "Дата": "snapshot_date",
    "Код магазина": "store_code",
    "Артикул": "sku",
    "Остаток": "qty_on_hand",
    "Себестоимость остатка": "stock_cost",
}


def parse_stock_csv(data: bytes) -> Iterator[dict]:
    if data[:2] == b"\x1f\x8b":
        data = gzip.decompress(data)
    # utf-8-sig съедает BOM, который оставляет Excel
    text = data.decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(text), delimiter=";")
    missing = set(STOCK_COLUMNS) - set(reader.fieldnames or [])
    if missing:
        raise ParseError(f"в CSV нет колонок: {sorted(missing)}")
    for line_no, row in enumerate(reader, start=2):
        try:
            yield {
                "snapshot_date": datetime.strptime(row["Дата"].strip(), "%d.%m.%Y").date(),
                "store_code": row["Код магазина"].strip(),
                "sku": row["Артикул"].strip(),
                "qty_on_hand": _decimal(row["Остаток"], comma=True),
                "stock_cost": _decimal(row["Себестоимость остатка"], comma=True),
            }
        except (ValueError, AttributeError) as exc:
            raise ParseError(f"строка {line_no}: {exc}") from exc


# -------------------------------------------------------------- промо (JSON)
def parse_promo_json(data: bytes) -> Iterator[dict]:
    try:
        doc = json.loads(data)
    except json.JSONDecodeError as exc:
        raise ParseError(f"битый JSON: {exc}") from exc
    campaign = doc.get("campaign") or {}
    period = campaign.get("period") or {}
    if not campaign.get("id") or not period.get("from"):
        raise ParseError("у кампании нет id или даты начала")
    for item in doc.get("items", []):
        mech = item.get("mechanics") or {}
        yield {
            "campaign_id": campaign["id"],
            "title": campaign.get("title"),
            "channels": list(campaign.get("channels") or []),
            "valid_from": date.fromisoformat(period["from"]),
            "valid_to": date.fromisoformat(period["to"]) if period.get("to") else None,
            "sku": item["sku"],
            "mechanics": mech.get("type"),
            "discount_pct": _decimal(str(mech["value"])) if mech.get("type") == "percent" else None,
        }


# ------------------------------------------------- курсы Нацбанка РК (XML)
def parse_nbk_rates(data: bytes) -> Iterator[dict]:
    """Ответ https://nationalbank.kz/rss/get_rates.cfm?fdate=ДД.ММ.ГГГГ.

    `description` — курс за `quant` единиц валюты (у воны quant=100), поэтому
    курс за единицу считается уже в DWH, а сюда пишем как есть.
    На дату без курсов приходит <info>...нет</info> и ноль item — это не ошибка.
    """
    try:
        root = ElementTree.fromstring(data)
    except ElementTree.ParseError as exc:
        raise ParseError(f"битый XML: {exc}") from exc
    rate_date = datetime.strptime(_text(root, "date") or "", "%d.%m.%Y").date()
    for item in root.iter("item"):
        yield {
            "rate_date": rate_date,
            "currency": _text(item, "title"),
            "rate": _decimal(_text(item, "description")),
            "quant": int(_text(item, "quant") or 1),
        }


PARSERS = {
    "catalog_xml": parse_catalog_xml,
    "stock_csv": parse_stock_csv,
    "promo_json": parse_promo_json,
    "nbk_rates": parse_nbk_rates,
}
