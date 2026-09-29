"""Точка входа: retail-dwh <команда>. Те же функции вызывают задачи Airflow."""

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

    sub.add_parser("migrate", help="применить миграции DWH")

    ing = sub.add_parser("ingest", help="загрузить источник в stg и проверить партии")
    ing.add_argument("pipelines", nargs="*", help="по умолчанию все")

    sub.add_parser("vault", help="разложить ожидающие партии stg по raw_vault")

    marts = sub.add_parser("marts", help="пересобрать витрины")
    marts.add_argument("--full", action="store_true", help="полная пересборка фактов")

    sub.add_parser("dq", help="проверки качества витрин")

    batch = sub.add_parser("batch", help="работа с партиями")
    batch.add_argument("action", choices=["release"])
    batch.add_argument("batch_id", type=int)

    vddl = sub.add_parser("vault-sql", help="показать сгенерированный SQL vault")
    vddl.add_argument("--table", help="stg-таблица; без неё — DDL")

    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    if args.cmd == "simulate":
        from retail_dwh.generator.simulator import Simulator

        print(json.dumps(Simulator(args.seed, args.orders_per_day).run(), ensure_ascii=False))

    elif args.cmd == "migrate":
        from retail_dwh.migrate import migrate

        print("применено:", migrate() or "ничего нового")

    elif args.cmd == "ingest":
        from retail_dwh import dq
        from retail_dwh.ingest.config import load_pipelines
        from retail_dwh.ingest.runner import run_pipeline

        pipelines = load_pipelines()
        for name in args.pipelines or list(pipelines):
            result = run_pipeline(name)
            dq.check_batches(pipelines[name], result.batches)
            print(f"{name}: партий {len(result.batches)}, строк {result.rows}")

    elif args.cmd == "vault":
        from retail_dwh.vault.loader import load_pending

        loads = load_pending()
        print(f"загружено партий: {len(loads)}")

    elif args.cmd == "marts":
        from retail_dwh.marts import build

        build(full=args.full)

    elif args.cmd == "dq":
        from retail_dwh import dq

        dq.check_marts()

    elif args.cmd == "batch":
        from retail_dwh import connections

        with connections.dwh() as conn:
            n = conn.execute(
                "UPDATE meta.batches SET status = 'loaded', error = error || ' [отпущена вручную]' "
                "WHERE batch_id = %s AND status = 'rejected'",
                (args.batch_id,),
            ).rowcount
            conn.commit()
        print("отпущена" if n else "партия не найдена или не в статусе rejected")

    elif args.cmd == "vault-sql":
        from retail_dwh.vault.model import load_model
        from retail_dwh.vault.render import ddl, load_statements

        model = load_model()
        print(ddl(model) if not args.table else "\n\n".join(load_statements(model, args.table)))


if __name__ == "__main__":
    main()
