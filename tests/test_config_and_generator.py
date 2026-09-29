from datetime import date, datetime, timedelta

import pytest
from pydantic import ValidationError

from retail_dwh.dq import check_sql
from retail_dwh.generator.entities import OrderGenerator, customer_row, customers_created_by, return_time
from retail_dwh.generator.world import World
from retail_dwh.ingest.config import Check, Pipeline, load_pipelines, parse_duration


def test_all_pipelines_valid_and_stg_only():
    pipelines = load_pipelines()
    assert len(pipelines) >= 8
    assert all(p.target.startswith("stg.") for p in pipelines.values())


def test_pipeline_rejects_non_stg_target_and_typos():
    base = {
        "name": "x",
        "description": "d",
        "owner": "o",
        "schedule": None,
        "source": {"kind": "nbk_rates", "start_date": "2025-01-01"},
    }
    with pytest.raises(ValidationError):
        Pipeline.model_validate(base | {"target": "marts.x"})
    with pytest.raises(ValidationError):  # опечатка в ключе не должна проходить молча
        Pipeline.model_validate(base | {"target": "stg.x", "shedule": "@daily"})


def test_parse_duration():
    assert parse_duration("90m") == timedelta(minutes=90)
    assert parse_duration("1d") == timedelta(days=1)
    with pytest.raises(ValueError):
        parse_duration("1 hour")


def test_generator_is_deterministic():
    hour = datetime(2026, 3, 14, 18)
    a = OrderGenerator(World(42)).hour(hour, 1, hour + timedelta(hours=2))
    b = OrderGenerator(World(42)).hour(hour, 1, hour + timedelta(hours=2))
    assert a.orders == b.orders and a.lines == b.lines
    assert a.orders, "в вечерний час заказы должны быть"


def test_generator_prices_match_catalog():
    world = World(42)
    hour = datetime(2026, 3, 14, 18)
    batch = OrderGenerator(world).hour(hour, 1, hour + timedelta(hours=2))
    line = next(line for line in batch.lines if line["sku"] in world.product_by_sku)
    product = world.product_by_sku[line["sku"]]
    assert line["unit_price"] == world.retail_price(product, date(2026, 3, 14))


def test_customers_are_stable_and_ordered():
    assert customer_row(123) == customer_row(123)
    ts = datetime(2026, 1, 1)
    n = customers_created_by(ts)
    assert customer_row(n)["created_at"] <= ts


def test_return_decision_depends_only_on_order_no():
    ts = datetime(2026, 1, 1)
    assert return_time(42, "S001-260101-1", ts) == return_time(42, "S001-260101-1", ts)


def test_dq_sql_rendering():
    name, sql, _ = check_sql(Check(check="unique", columns=["order_no", "line_no"]), "t0")
    assert name == "unique(order_no, line_no)"
    assert "GROUP BY order_no, line_no HAVING count(*) > 1" in sql
    with pytest.raises(ValueError):
        check_sql(Check(check="not_null", columns=["x; DROP TABLE y"]), "t0")


def test_web_sessions_never_in_future():
    import random
    from datetime import UTC

    from retail_dwh.generator.events import _session

    now = datetime.now(UTC)
    world, rnd = World(42), random.Random(7)
    for _ in range(200):
        events = _session(world, rnd, now)
        stamps = [datetime.fromisoformat(e["event_ts"]) for e in events]
        assert max(stamps) <= now
        assert stamps == sorted(stamps), "порядок событий в сессии сохраняется"
