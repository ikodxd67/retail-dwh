"""Точка входа: retail-dwh <команда>."""

from __future__ import annotations

import argparse
import json
import logging


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="retail-dwh")
    sub = parser.add_subparsers(dest="cmd", required=True)

    sim = sub.add_parser("simulate", help="догнать источники до текущего момента")
    sim.add_argument("--seed", type=int, default=42)
    sim.add_argument("--orders-per-day", type=int, default=700)

    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    if args.cmd == "simulate":
        from retail_dwh.generator.simulator import Simulator

        stats = Simulator(args.seed, args.orders_per_day).run()
        print(json.dumps(stats, ensure_ascii=False))


if __name__ == "__main__":
    main()
