"""Симулятор источников: доводит ERP, CRM и landing до текущего момента.

Один и тот же вызов `run()` и заливает историю на пустых базах, и
досоздаёт данные за прошедший час — симулятор смотрит, где остановился в
прошлый раз (max(order_ts) в ERP, число клиентов в CRM, файлы в бакете).
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from retail_dwh import connections
from retail_dwh.generator import files
from retail_dwh.generator.entities import (
    HISTORY_START,
    OrderGenerator,
    customer_changes,
    customer_row,
    customers_created_by,
    hours_between,
    return_time,
)
from retail_dwh.generator.world import World
from retail_dwh.settings import PROJECT_DIR

log = logging.getLogger(__name__)
TZ = ZoneInfo("Asia/Almaty")
ORACLE_BATCH = 20_000


def local_now() -> datetime:
    """ERP хранит локальное время без зоны — так устроено большинство ERP."""
    return datetime.now(TZ).replace(tzinfo=None)


def _oracle_statements(path) -> list[str]:
    text = path.read_text(encoding="utf-8")
    return [s.strip() for s in text.split("\n/\n") if s.strip() and s.strip() != "/"]


class Simulator:
    def __init__(self, seed: int = 42, base_daily_orders: int = 700, stock_days: int = 45):
        self.world = World(seed)
        self.orders = OrderGenerator(self.world, base_daily_orders)
        self.stock_days = stock_days

    def run(self, now: datetime | None = None) -> dict[str, int]:
        now = now or local_now()
        stats = {}
        stats |= self.sync_crm(now)
        stats |= self.sync_erp(now)
        stats |= self.sync_files(now)
        return stats

    # -------------------------------------------------------------------- CRM
    def sync_crm(self, now: datetime) -> dict[str, int]:
        with connections.crm() as conn:
            conn.execute((PROJECT_DIR / "sql/sources/crm.sql").read_text(encoding="utf-8"))
            existing = conn.execute("SELECT coalesce(max(customer_id), 0) FROM customers").fetchone()[0]
            # служебная отметка симулятора: до какого часа уже накатаны правки
            conn.execute(
                "CREATE TABLE IF NOT EXISTS simulator_state (key text PRIMARY KEY, value timestamp)"
            )
            row = conn.execute(
                "SELECT value FROM simulator_state WHERE key = 'changes_until'"
            ).fetchone()
            changes_until = row[0] if row else None
            total = customers_created_by(now)
            new_rows = [customer_row(i) for i in range(existing + 1, total + 1)]
            cols = list(customer_row(1).keys())
            with conn.cursor() as cur, cur.copy(
                f"COPY customers ({', '.join(cols)}) FROM STDIN"
            ) as copy:
                for row in new_rows:
                    row["created_at"] = row["created_at"].replace(tzinfo=TZ)
                    row["updated_at"] = row["updated_at"].replace(tzinfo=TZ)
                    copy.write_row([row[c] for c in cols])

            # правки карточек накатываем только в «живом» режиме — на заливке
            # истории у источника есть лишь текущее состояние, как в жизни
            changed = 0
            last_full_hour = now.replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)
            if existing and changes_until:
                start = max(changes_until + timedelta(hours=1), now - timedelta(days=2))
                for hour in hours_between(start, last_full_hour):
                    for customer_id, patch in customer_changes(hour, total):
                        patch = {
                            k: v.replace(tzinfo=TZ) if isinstance(v, datetime) else v
                            for k, v in patch.items()
                        }
                        sets = ", ".join(f"{k} = %({k})s" for k in patch)
                        conn.execute(
                            f"UPDATE customers SET {sets} WHERE customer_id = %(id)s "
                            "AND deleted_at IS NULL",
                            patch | {"id": customer_id},
                        )
                        changed += 1
            conn.execute(
                "INSERT INTO simulator_state VALUES ('changes_until', %s) "
                "ON CONFLICT (key) DO UPDATE SET value = excluded.value",
                (last_full_hour,),
            )
            conn.commit()
        log.info("crm: +%s клиентов, %s правок", len(new_rows), changed)
        return {"crm_new_customers": len(new_rows), "crm_changes": changed}

    # -------------------------------------------------------------------- ERP
    def sync_erp(self, now: datetime) -> dict[str, int]:
        with connections.oracle() as conn:
            cur = conn.cursor()
            for stmt in _oracle_statements(PROJECT_DIR / "sql/sources/oracle_erp.sql"):
                cur.execute(stmt)
            self._sync_stores(cur)
            cur.execute("SELECT max(order_ts), coalesce(max(order_id), 0) FROM orders")
            last_ts, last_id = cur.fetchone()
            # генерируем только завершённые часы: так не приходится удалять
            # строки, которые хранилище, возможно, уже забрало
            if last_ts is None:
                start = HISTORY_START_DT
            else:
                start = last_ts.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
            end = now.replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)

            n_orders = n_lines = 0
            orders_buf: list[dict] = []
            lines_buf: list[dict] = []
            next_id = last_id + 1
            for hour in hours_between(start, end):
                batch = self.orders.hour(hour, next_id, now)
                for order in batch.orders:
                    returned_at = return_time(self.world.seed, order["order_no"], order["order_ts"])
                    if order["status"] == "PAID" and returned_at and returned_at <= now:
                        order["status"] = "RETURNED"
                        order["updated_at"] = returned_at
                next_id += len(batch.orders)
                orders_buf += batch.orders
                lines_buf += batch.lines
                if len(lines_buf) >= ORACLE_BATCH:
                    self._insert_orders(cur, orders_buf, lines_buf)
                    conn.commit()
                    n_orders += len(orders_buf)
                    n_lines += len(lines_buf)
                    orders_buf, lines_buf = [], []
                    log.info("erp: записано %s заказов, дошли до %s", n_orders, hour)
            self._insert_orders(cur, orders_buf, lines_buf)
            n_orders += len(orders_buf)
            n_lines += len(lines_buf)
            returns = self._apply_returns(cur, now)
            conn.commit()
        log.info("erp: +%s заказов, +%s строк, %s возвратов", n_orders, n_lines, returns)
        return {"erp_orders": n_orders, "erp_lines": n_lines, "erp_returns": returns}

    def _sync_stores(self, cur) -> None:
        cur.execute("SELECT store_id FROM stores")
        have = {r[0] for r in cur.fetchall()}
        rows = [
            dict(store_id=s.store_id, store_code=s.store_code, name=s.name, city=s.city,
                 format=s.format, area_sqm=s.area_sqm, opened_on=s.opened_on)
            for s in self.world.stores if s.store_id not in have
        ]
        if rows:
            cur.executemany(
                "INSERT INTO stores (store_id, store_code, name, city, format, area_sqm, opened_on) "
                "VALUES (:store_id, :store_code, :name, :city, :format, :area_sqm, :opened_on)",
                rows,
            )

    @staticmethod
    def _insert_orders(cur, orders: list[dict], lines: list[dict]) -> None:
        if not orders:
            return
        cur.executemany(
            "INSERT INTO orders (order_id, order_no, store_id, customer_id, order_ts, channel, "
            "status, payment_type, updated_at) VALUES (:order_id, :order_no, :store_id, "
            ":customer_id, :order_ts, :channel, :status, :payment_type, :updated_at)",
            orders,
        )
        cur.executemany(
            "INSERT INTO order_lines (order_id, line_no, sku, qty, unit_price, discount_amt, "
            "updated_at) VALUES (:order_id, :line_no, :sku, :qty, :unit_price, :discount_amt, "
            ":updated_at)",
            lines,
        )

    def _apply_returns(self, cur, now: datetime) -> int:
        """Возвраты по свежим заказам, чьё время возврата уже наступило."""
        cur.execute(
            "SELECT order_id, order_no, order_ts FROM orders "
            "WHERE status = 'PAID' AND order_ts > :since",
            since=now - timedelta(days=15),
        )
        due = []
        for order_id, order_no, order_ts in cur.fetchall():
            returned_at = return_time(self.world.seed, order_no, order_ts)
            if returned_at and returned_at <= now:
                due.append({"id": order_id, "ts": returned_at})
        if due:
            cur.executemany(
                "UPDATE orders SET status = 'RETURNED', updated_at = :ts WHERE order_id = :id", due
            )
        return len(due)

    # ------------------------------------------------------------------ файлы
    def sync_files(self, now: datetime) -> dict[str, int]:
        s3 = connections.s3()
        existing = set()
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=files.LANDING_BUCKET):
            existing |= {o["Key"] for o in page.get("Contents", [])}
        # что уже обработано и ушло в архив, второй раз не кладём
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket="archive"):
            existing |= {o["Key"] for o in page.get("Contents", [])}

        today = now.date()
        written = 0

        def put(key: str, build) -> None:
            nonlocal written
            if key not in existing:
                s3.put_object(Bucket=files.LANDING_BUCKET, Key=key, Body=build())
                written += 1

        # каталог: история помесячно (цены меняются раз в месяц) и каждый день последние 2 недели
        day = HISTORY_START
        while day <= today:
            if day.day == 1 or (today - day).days < 14:
                put(files.catalog_key(day), lambda d=day: files.catalog_xml(self.world, d))
            day += timedelta(days=1)

        for back in range(self.stock_days):
            day = today - timedelta(days=back + 1)  # остатки выгружаются за вчера
            put(files.stock_key(day), lambda d=day: files.stock_csv(self.world, d))

        monday = HISTORY_START - timedelta(days=HISTORY_START.weekday())
        while monday <= today:
            put(files.promo_key(monday), lambda m=monday: files.promo_json(self.world, m))
            monday += timedelta(days=7)
        log.info("landing: +%s файлов", written)
        return {"landing_files": written}


HISTORY_START_DT = datetime.combine(HISTORY_START, datetime.min.time())
