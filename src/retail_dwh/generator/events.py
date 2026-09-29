"""Кликстрим сайта: сессии пользователей -> Kafka, топик web.events.

Сессия — цепочка view -> (add_to_cart) -> (checkout) -> (purchase) с
отвалами на каждом шаге. Чтобы потоковой обработке было что делать, поток
специально «грязный», как в жизни:
- ~2% событий отправляются дважды (ретраи мобильного SDK);
- ~1% приходят с опозданием до 20 минут (телефон был без сети);
- ~0.1% — битый JSON.
"""

from __future__ import annotations

import json
import logging
import os
import random
import time
import uuid
from datetime import UTC, datetime, timedelta

from retail_dwh.generator.entities import customers_created_by
from retail_dwh.generator.world import World

log = logging.getLogger(__name__)
TOPIC = "web.events"
DEVICES = ["android", "ios", "web-desktop", "web-mobile"]
SOURCES = ["direct", "google", "instagram", "email", "2gis"]


def _session(world: World, rnd: random.Random, now: datetime) -> list[dict]:
    known = customers_created_by(now.replace(tzinfo=None))
    customer_id = rnd.randint(1, known) if known and rnd.random() < 0.45 else None
    session_id = uuid.uuid4().hex
    device, source = rnd.choice(DEVICES), rnd.choice(SOURCES)
    products = world.active_products(now.date())
    ts = now
    events = []

    def emit(event_type: str, sku: str | None = None, **extra) -> None:
        nonlocal ts
        ts += timedelta(seconds=rnd.randint(3, 90))
        events.append(
            {
                "event_id": uuid.uuid4().hex,
                "event_type": event_type,
                "event_ts": ts.isoformat(timespec="milliseconds"),
                "session_id": session_id,
                "customer_id": customer_id,
                "device": device,
                "traffic_source": source,
                "sku": sku,
                **extra,
            }
        )

    viewed = rnd.sample(products, k=min(len(products), 1 + int(rnd.expovariate(0.3))))
    for p in viewed:
        emit("product_view", p.sku, price=world.retail_price(p, now.date()))
    if rnd.random() < 0.35:
        cart = [p for p in viewed if rnd.random() < 0.5] or viewed[:1]
        for p in cart:
            emit("add_to_cart", p.sku, qty=rnd.choice([1, 1, 1, 2]))
        if rnd.random() < 0.55:
            emit("checkout_start")
            if rnd.random() < 0.7:
                emit("purchase", order_value=sum(world.retail_price(p, now.date()) for p in cart))
    # паузы между событиями уводят длинную сессию в будущее — сдвигаем её
    # назад так, чтобы последнее событие случилось не позже «сейчас»
    overshoot = ts - now
    for event in events:
        event["event_ts"] = (datetime.fromisoformat(event["event_ts"]) - overshoot).isoformat(
            timespec="milliseconds"
        )
    return events


def run(bootstrap: str, events_per_second: float = 15.0, seed: int | None = None) -> None:
    from confluent_kafka import Producer

    producer = Producer(
        {
            "bootstrap.servers": bootstrap,
            "linger.ms": 50,
            "compression.type": "zstd",
            "enable.idempotence": True,
        }
    )
    world = World(42)
    rnd = random.Random(seed)
    sent = 0
    started = time.monotonic()
    while True:
        now = datetime.now(UTC)
        for event in _session(world, rnd, now):
            if rnd.random() < 0.01:
                late = timedelta(minutes=rnd.uniform(1, 20))
                event["event_ts"] = (datetime.fromisoformat(event["event_ts"]) - late).isoformat(
                    timespec="milliseconds"
                )
            payload = json.dumps(event, ensure_ascii=False).encode()
            if rnd.random() < 0.001:
                payload = payload[: len(payload) // 2]  # обрыв посреди JSON
            # ключ — сессия: все события сессии попадут в одну партицию по порядку
            key = event["session_id"].encode()
            producer.produce(TOPIC, payload, key=key)
            if rnd.random() < 0.02:
                producer.produce(TOPIC, payload, key=key)
            sent += 1
        producer.poll(0)
        # держим заданный темп
        expected = (time.monotonic() - started) * events_per_second
        if sent > expected:
            time.sleep((sent - expected) / events_per_second)
        if sent % 5000 < 10:
            log.info("отправлено событий: %s", sent)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    run(
        os.environ.get("RETAIL_KAFKA_BOOTSTRAP", "localhost:9092"),
        float(os.environ.get("EVENTS_PER_SECOND", "15")),
    )
