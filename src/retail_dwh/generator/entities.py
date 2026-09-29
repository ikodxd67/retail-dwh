"""Генерация строк для источников: клиенты CRM, заказы ERP, промо-акции."""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from functools import lru_cache
from itertools import accumulate

from retail_dwh.generator.world import (
    CITIES,
    FIRST_NAMES,
    HISTORY_START,
    LAST_NAMES,
    Product,
    World,
    stable_rand,
)

# клиенты регистрируются равномерно с этой даты, ~CUSTOMERS_PER_DAY в день
CUSTOMER_EPOCH = datetime(2023, 12, 1)
CUSTOMERS_PER_DAY = 45
TIERS = ["basic", "silver", "gold", "platinum"]


def customers_created_by(ts: datetime) -> int:
    """Сколько клиентов уже зарегистрировано к моменту ts."""
    if ts <= CUSTOMER_EPOCH:
        return 0
    return math.floor((ts - CUSTOMER_EPOCH).total_seconds() / 86400 * CUSTOMERS_PER_DAY)


def customer_created_at(customer_id: int) -> datetime:
    base = CUSTOMER_EPOCH + timedelta(days=customer_id / CUSTOMERS_PER_DAY)
    # сдвиг только назад, чтобы customers_created_by оставалась верной
    jitter = stable_rand("customer-ts", customer_id).uniform(0, 86400 / CUSTOMERS_PER_DAY)
    return base - timedelta(seconds=jitter)


def _phone(rnd) -> str | None:
    digits = f"70{rnd.choice('0157')}{rnd.randint(1000000, 9999999)}"
    fmt = rnd.random()
    # в CRM телефоны вводят как попало — нормализация на стороне DWH
    if fmt < 0.5:
        return f"+{digits[0]} {digits[1:4]} {digits[4:7]} {digits[7:9]} {digits[9:]}"
    if fmt < 0.8:
        return "8" + digits[1:]
    if fmt < 0.95:
        return f"+{digits}"
    return None


def customer_row(customer_id: int) -> dict:
    rnd = stable_rand("customer", customer_id)
    first = rnd.choice(FIRST_NAMES)
    last = rnd.choice(LAST_NAMES)
    female = first in FIRST_NAMES[:10]
    if female and last.endswith(("ов", "ев", "ин")):
        last += "а"
    created = customer_created_at(customer_id)
    email = f"client{customer_id}@{rnd.choice(['mail.kz', 'gmail.com', 'mail.ru', 'yandex.kz'])}"
    if rnd.random() < 0.002 and customer_id > 1:
        # дубль e-mail у разных клиентов — одна из проверок качества это ловит
        email = f"client{customer_id - 1}@mail.kz"
    if rnd.random() < 0.03:
        email = None
    return {
        "customer_id": customer_id,
        "email": email.upper() if email and rnd.random() < 0.05 else email,
        "phone": _phone(rnd),
        "first_name": first,
        "last_name": last,
        "birth_date": date(1960, 1, 1) + timedelta(days=rnd.randint(0, 16000))
        if rnd.random() > 0.1
        else None,
        "gender": "F" if female else "M",
        "city": rnd.choices([c for c, _ in CITIES], weights=[w for _, w in CITIES])[0],
        "loyalty_tier": rnd.choices(TIERS, weights=[70, 20, 8, 2])[0],
        "marketing_consent": rnd.random() < 0.6,
        "created_at": created,
        "updated_at": created,
        "deleted_at": None,
    }


def customer_changes(hour_start: datetime, total: int) -> list[tuple[int, dict]]:
    """Изменения карточек клиентов за час: переезд, смена уровня, согласие, удаление."""
    rnd = stable_rand("customer-changes", hour_start.isoformat())
    changes = []
    for _ in range(max(0, round(rnd.gauss(total * 0.00025, 2)))):
        customer_id = rnd.randint(1, total)
        ts = hour_start + timedelta(seconds=rnd.randint(0, 3599))
        kind = rnd.random()
        if kind < 0.45:
            patch = {"loyalty_tier": rnd.choice(TIERS[1:])}
        elif kind < 0.75:
            patch = {"city": rnd.choice([c for c, _ in CITIES])}
        elif kind < 0.97:
            patch = {"marketing_consent": rnd.random() < 0.5, "phone": _phone(rnd)}
        else:
            patch = {"deleted_at": ts}
        patch["updated_at"] = ts
        changes.append((customer_id, patch))
    return changes


@lru_cache(maxsize=8)
def promo_for_week(seed: int, monday: date) -> tuple[tuple[str, int], ...]:
    world = World(seed)
    rnd = stable_rand(seed, "promo", monday.isoformat())
    active = world.active_products(monday)
    items = rnd.sample(active, k=min(40, len(active)))
    return tuple((p.sku, rnd.choice([5, 10, 15, 20, 25, 30])) for p in items)


@dataclass
class OrderBatch:
    orders: list[dict]
    lines: list[dict]


class OrderGenerator:
    def __init__(self, world: World, base_daily_orders: int = 700):
        self.world = world
        self.base = base_daily_orders
        self._day: date | None = None
        self._products: list[Product] = []
        self._cum_weights: list[float] = []
        self._promo: dict[str, int] = {}

    def _prepare_day(self, day: date) -> None:
        if self._day == day:
            return
        self._day = day
        self._products = self.world.active_products(day)
        self._cum_weights = list(accumulate(p.popularity for p in self._products))
        monday = day - timedelta(days=day.weekday())
        self._promo = dict(promo_for_week(self.world.seed, monday))

    def hour(self, hour_start: datetime, first_order_id: int, now: datetime) -> OrderBatch:
        day = hour_start.date()
        self._prepare_day(day)
        rnd = stable_rand(self.world.seed, "orders", hour_start.isoformat())
        stores = [s for s in self.world.stores if self.world.store_is_open(s, day)]
        store_weights = [s.weight for s in stores]
        orders, lines = [], []
        order_id = first_order_id
        for _ in range(self.world.hourly_orders(hour_start, self.base)):
            store = rnd.choices(stores, weights=store_weights)[0]
            online = store.format == "ONLINE"
            ts = hour_start + timedelta(seconds=rnd.randint(0, 3599), microseconds=rnd.randint(0, 999999))
            if ts > now:
                continue
            known = customers_created_by(ts)
            customer_id = None
            if rnd.random() < (0.95 if online else 0.6) and known:
                customer_id = rnd.randint(1, known)
                if rnd.random() < 0.001:
                    customer_id = known + rnd.randint(1000, 5000)  # клиента нет в CRM
            # кассы выгружают чеки в ERP пачками, поэтому updated_at отстаёт от продажи
            delay = (
                timedelta(seconds=rnd.randint(1, 30))
                if online
                else timedelta(minutes=rnd.choice([1, 5, 15, 30, 60]) * rnd.random())
            )
            updated = min(ts + delay, now)
            status = "CANCELLED" if rnd.random() < 0.02 else "PAID"
            orders.append(
                {
                    "order_id": order_id,
                    "order_no": f"{store.store_code}-{ts:%y%m%d}-{order_id}",
                    "store_id": store.store_id,
                    "customer_id": customer_id,
                    "order_ts": ts,
                    "channel": "WEB" if online else "STORE",
                    "status": status,
                    "payment_type": rnd.choices(
                        ["CARD", "QR", "CASH"], weights=[60, 30, 0 if online else 25]
                    )[0],
                    "updated_at": updated,
                }
            )
            n_lines = min(12, 1 + int(rnd.expovariate(0.45)))
            skus: set[str] = set()
            for _ in range(n_lines):
                product = rnd.choices(self._products, cum_weights=self._cum_weights)[0]
                if product.sku in skus:
                    continue
                skus.add(product.sku)
                sku = product.sku
                if rnd.random() < 0.0003:
                    sku = f"SKU-9{rnd.randint(10000, 99999)}"  # нет в каталоге
                qty = (
                    round(rnd.uniform(0.2, 2.5), 3)
                    if product.unit == "кг"
                    else rnd.choices([1, 2, 3, 4, 6], weights=[70, 18, 6, 4, 2])[0]
                )
                price = self.world.retail_price(product, day)
                pct = self._promo.get(sku, 0)
                if not pct and customer_id and rnd.random() < 0.05:
                    pct = 3  # скидка по карте лояльности
                lines.append(
                    {
                        "order_id": order_id,
                        "line_no": len(skus),
                        "sku": sku,
                        "qty": qty,
                        "unit_price": price,
                        "discount_amt": round(price * qty * pct / 100, 2),
                        "updated_at": updated,
                    }
                )
            order_id += 1
        return OrderBatch(orders, lines)


def return_time(seed: int, order_no: str, order_ts: datetime) -> datetime | None:
    """Если заказ будет возвращён, то когда. Решение зависит только от номера заказа."""
    rnd = stable_rand(seed, "return", order_no)
    if rnd.random() >= 0.015:
        return None
    return order_ts + timedelta(days=rnd.uniform(1, 14))


def hours_between(start: datetime, end: datetime):
    hour = start.replace(minute=0, second=0, microsecond=0)
    while hour <= end:
        yield hour
        hour += timedelta(hours=1)


__all__ = [
    "HISTORY_START",
    "OrderGenerator",
    "customer_changes",
    "customer_row",
    "customers_created_by",
    "hours_between",
    "promo_for_week",
    "return_time",
]
