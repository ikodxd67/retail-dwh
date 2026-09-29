"""Детерминированный «мир» сети: магазины, дерево категорий, товары, цены.

Всё выводится из seed, поэтому генератор можно перезапускать: одна и та же
дата даёт тот же каталог и те же цены. Это нужно, чтобы заказы из ERP и
каталог поставщика из XML-файлов сходились между собой.
"""

from __future__ import annotations

import hashlib
import random
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from functools import cached_property
from typing import ClassVar

HISTORY_START = date(2025, 1, 1)

CITIES = [
    ("Алматы", 9),
    ("Астана", 7),
    ("Шымкент", 5),
    ("Караганда", 3),
    ("Актобе", 3),
    ("Павлодар", 2),
    ("Усть-Каменогорск", 2),
    ("Атырау", 2),
    ("Костанай", 2),
    ("Тараз", 2),
]

CATEGORY_TREE: dict[str, dict[str, list[str]]] = {
    "Продукты": {
        "Молочные продукты": ["Молоко", "Кефир и йогурты", "Сыры"],
        "Бакалея": ["Крупы", "Макароны", "Масло растительное"],
        "Напитки": ["Вода", "Соки", "Чай и кофе"],
        "Кондитерские изделия": ["Шоколад", "Печенье"],
    },
    "Бытовая химия": {
        "Стирка": ["Порошки", "Кондиционеры"],
        "Уборка": ["Средства для кухни", "Средства для ванной"],
    },
    "Гигиена": {
        "Уход за волосами": ["Шампуни", "Бальзамы"],
        "Уход за полостью рта": ["Зубные пасты", "Щётки"],
    },
    "Товары для дома": {
        "Посуда": ["Тарелки", "Кастрюли"],
        "Текстиль": ["Полотенца", "Постельное бельё"],
    },
    "Электроника": {
        "Аксессуары": ["Зарядные устройства", "Наушники"],
        "Мелкая техника": ["Чайники", "Утюги"],
    },
}

# типичная цена подкатегории в тенге: (от, до)
PRICE_BANDS = {
    "Продукты": (250, 4500),
    "Бытовая химия": (600, 6500),
    "Гигиена": (400, 5500),
    "Товары для дома": (1500, 25000),
    "Электроника": (3500, 60000),
}

BRANDS = [
    "Food Master",
    "Адал",
    "Рахат",
    "Цесна",
    "Bonduelle",
    "Persil",
    "Ariel",
    "Fairy",
    "Colgate",
    "Head&Shoulders",
    "Tefal",
    "Philips",
    "Xiaomi",
    "Baseus",
    "Luminarc",
    "Домашний уют",
    "Экономия",
    "Kazakhstan Textile",
    "Шын",
    "Sabi",
]

# грубые курсы только для генерации закупочных цен; в DWH идут настоящие курсы НБРК
APPROX_KZT_RATE = {"KZT": 1.0, "USD": 505.0, "RUB": 6.1, "CNY": 70.0}

FIRST_NAMES = [
    "Айгерим",
    "Алия",
    "Дана",
    "Жанна",
    "Мадина",
    "Асель",
    "Анна",
    "Елена",
    "Ольга",
    "Сауле",
    "Нурлан",
    "Ерлан",
    "Асхат",
    "Даурен",
    "Айдос",
    "Серик",
    "Алексей",
    "Дмитрий",
    "Иван",
    "Тимур",
]
LAST_NAMES = [
    "Ахметов",
    "Серікбаев",
    "Нурланов",
    "Жумабаев",
    "Исаев",
    "Ким",
    "Иванов",
    "Петров",
    "Смагулов",
    "Байжанов",
    "Сидоров",
    "Ли",
    "Омаров",
    "Касымов",
    "Абдрахманов",
    "Попов",
]


def stable_rand(*parts: object) -> random.Random:
    """Генератор случайных чисел, зависящий только от переданных частей ключа."""
    digest = hashlib.sha256("|".join(map(str, parts)).encode()).digest()
    return random.Random(int.from_bytes(digest[:8], "big"))


@dataclass(frozen=True)
class Store:
    store_id: int
    store_code: str
    name: str
    city: str
    format: str
    area_sqm: float | None
    opened_on: date
    weight: float  # доля в оборотах сети


@dataclass(frozen=True)
class Product:
    sku: str
    name: str
    brand: str
    group: str
    category: str
    subcategory: str
    unit: str
    base_price: float
    supplier_code: str
    purchase_currency: str
    purchase_ratio: float  # закупка / розница в момент запуска
    launched_on: date
    discontinued_on: date | None
    popularity: float


class World:
    def __init__(self, seed: int = 42, n_products: int = 1500):
        self.seed = seed
        self.n_products = n_products

    @cached_property
    def stores(self) -> list[Store]:
        rnd = stable_rand(self.seed, "stores")
        stores: list[Store] = []
        store_id = 1
        for city, count in CITIES:
            for i in range(count):
                fmt = rnd.choices(["HYPER", "SUPER", "EXPRESS"], weights=[2, 5, 4])[0]
                area = {"HYPER": 6000, "SUPER": 1500, "EXPRESS": 300}[fmt] * rnd.uniform(0.7, 1.4)
                opened = HISTORY_START - timedelta(days=rnd.randint(200, 3000))
                # несколько магазинов открылись уже внутри истории — у них нет продаж раньше
                if rnd.random() < 0.1:
                    opened = HISTORY_START + timedelta(days=rnd.randint(30, 400))
                weight = {"HYPER": 4.0, "SUPER": 1.6, "EXPRESS": 0.6}[fmt] * rnd.uniform(0.8, 1.2)
                stores.append(
                    Store(
                        store_id,
                        f"S{store_id:03d}",
                        f"{city}, магазин №{i + 1}",
                        city,
                        fmt,
                        round(area, 1),
                        opened,
                        weight,
                    )
                )
                store_id += 1
        stores.append(
            Store(
                store_id,
                "WEB01",
                "Интернет-магазин",
                "Алматы",
                "ONLINE",
                None,
                HISTORY_START - timedelta(days=900),
                9.0,
            )
        )
        return stores

    @cached_property
    def subcategories(self) -> list[tuple[str, str, str]]:
        return [
            (group, cat, sub)
            for group, cats in CATEGORY_TREE.items()
            for cat, subs in cats.items()
            for sub in subs
        ]

    @cached_property
    def products(self) -> list[Product]:
        rnd = stable_rand(self.seed, "products")
        products = []
        for n in range(1, self.n_products + 1):
            group, cat, sub = rnd.choice(self.subcategories)
            lo, hi = PRICE_BANDS[group]
            price = round(rnd.uniform(lo, hi) / 10) * 10
            currency = rnd.choices(["KZT", "RUB", "USD", "CNY"], weights=[55, 25, 10, 10])[0]
            brand = rnd.choice(BRANDS)
            launched = HISTORY_START - timedelta(days=rnd.randint(0, 1000))
            if rnd.random() < 0.15:
                launched = HISTORY_START + timedelta(days=rnd.randint(0, 550))
            discontinued = None
            if rnd.random() < 0.05:
                discontinued = launched + timedelta(days=rnd.randint(120, 500))
            unit = "кг" if sub in {"Крупы", "Сыры"} and rnd.random() < 0.5 else "шт"
            products.append(
                Product(
                    sku=f"SKU-{n:06d}",
                    name=f"{sub[:-1] if sub.endswith('ы') else sub} {brand} {rnd.randint(1, 99):02d}",
                    brand=brand,
                    group=group,
                    category=cat,
                    subcategory=sub,
                    unit=unit,
                    base_price=float(price),
                    supplier_code=f"SUP-{rnd.randint(1, 60):03d}",
                    purchase_currency=currency,
                    purchase_ratio=rnd.uniform(0.55, 0.8),
                    launched_on=launched,
                    discontinued_on=discontinued,
                    # распределение продаж с длинным хвостом, как в жизни
                    popularity=rnd.paretovariate(1.3),
                )
            )
        return products

    @cached_property
    def product_by_sku(self) -> dict[str, Product]:
        return {p.sku: p for p in self.products}

    def active_products(self, day: date) -> list[Product]:
        return [
            p
            for p in self.products
            if p.launched_on <= day and (p.discontinued_on is None or p.discontinued_on > day)
        ]

    def retail_price(self, product: Product, day: date) -> float:
        """Розничная цена меняется раз в месяц: инфляция плюс шум товара."""
        months = (day.year - HISTORY_START.year) * 12 + day.month - HISTORY_START.month
        noise = stable_rand(self.seed, "price", product.sku, day.year, day.month).uniform(-0.03, 0.04)
        return round(product.base_price * (1 + 0.009 * months + noise), -1) or 10.0

    def purchase_price(self, product: Product, day: date) -> float:
        """Закупочная цена в валюте поставщика."""
        kzt = self.retail_price(product, day) * product.purchase_ratio
        return round(kzt / APPROX_KZT_RATE[product.purchase_currency], 2)

    def store_is_open(self, store: Store, day: date) -> bool:
        return store.opened_on <= day

    @staticmethod
    def daily_orders(day: date, base: int) -> float:
        """Сколько заказов ждём за день: рост, недельный цикл и сезонность."""
        days = (day - HISTORY_START).days
        growth = 1 + days / 365 * 0.18
        weekday = [0.9, 0.88, 0.92, 0.97, 1.12, 1.3, 1.15][day.weekday()]
        season = {12: 1.35, 1: 0.85, 3: 1.1, 9: 1.05}.get(day.month, 1.0)
        return base * growth * weekday * season

    HOUR_PROFILE: ClassVar[list[float]] = [
        0.1,
        0.05,
        0.03,
        0.02,
        0.02,
        0.05,
        0.2,
        0.5,
        0.9,
        1.1,
        1.2,
        1.3,
        1.4,
        1.3,
        1.2,
        1.2,
        1.3,
        1.6,
        1.9,
        2.0,
        1.7,
        1.2,
        0.6,
        0.3,
    ]

    def hourly_orders(self, hour_start: datetime, base: int) -> int:
        expected = self.daily_orders(hour_start.date(), base) * self.HOUR_PROFILE[hour_start.hour]
        expected /= sum(self.HOUR_PROFILE)
        rnd = stable_rand(self.seed, "orders-count", hour_start.isoformat())
        # нормальное приближение пуассона, для десятков заказов этого хватает
        return max(0, round(rnd.gauss(expected, expected**0.5)))
