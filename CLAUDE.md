# retail-dwh — инструкции для AI-агентов

Хранилище сети магазинов: Oracle ERP, CRM на PostgreSQL, файлы поставщиков,
API Нацбанка и Kafka сводятся в Data Vault (PostgreSQL), из него строится звезда,
продажи публикуются в ClickHouse, кликстрим живёт в Delta в S3 (SeaweedFS), всё видно
через Trino. Оркестрация — Airflow 3.

## Где что

| Что | Где |
|---|---|
| Источник = один YAML | `pipelines/*.yml`, схема в `src/retail_dwh/ingest/config.py` |
| Запросы к источникам | `sql/extract/` |
| Модель Data Vault (DDL и загрузка генерируются) | `models/vault.yml` |
| Миграции DWH | `sql/dwh/NNN_*.sql` |
| Сборка витрин, по шагу на файл | `sql/marts/NN_*.sql` |
| Проверки витрин | `catalog/dq_marts.yml` |
| Каталог: смысл, владелец, PII | `catalog/datasets.yml` |
| DAG-и | `dags/` (фабрика `ingest_factory.py` читает pipelines/) |
| Spark-задачи | `spark/jobs/` |

## Команды

```bash
docker compose up -d                        # источники, DWH, S3
retail-dwh migrate                          # миграции
retail-dwh ingest erp_orders                # один источник -> stg (+ проверки)
retail-dwh vault                            # stg -> raw_vault
retail-dwh marts                            # витрины (инкрементально)
retail-dwh dq                               # проверки витрин
retail-dwh vault-sql --table stg.erp_orders # посмотреть сгенерированный SQL
pytest -m "not integration"                 # быстрые тесты
pytest -m integration                       # против живого стенда
```

## Правила

- **Персональные данные.** ФИО, телефон и e-mail живут только в
  `raw_vault.sat_customer_pii`. В `marts`, ClickHouse и Delta их быть не должно;
  тест `test_pii_never_reaches_marts` проверяет это по lineage. Не выводи их
  в новые витрины и не показывай в ответах.
- **Данные смотри через MCP-сервер `retail-dwh`**, а не прямым подключением.
  Он пускает только SELECT и только в каталоги dwh, ch и lake. Диалект — Trino,
  имена пишутся полностью: `dwh.marts.fact_sales`.
- **Особые ключи:** `customer_sk = -1` — анонимная покупка, `-2` — клиента нет в CRM,
  `product_sk = -1` — неизвестный товар. Не отбрасывай их молча в отчётах.
- **Время:** ERP пишет местное время без зоны (Asia/Almaty), в DWH всё хранится
  в `timestamptz`, база настроена на Asia/Almaty.
- **Сборка витрин идёт одной транзакцией**, SCD2 при этом закрывает версии. Если
  меняешь `sql/marts/`, прогони `retail-dwh marts --full` на стенде и `retail-dwh dq`.
- **Не правь ничего внутри `raw_vault` руками.** Vault хранит только добавления;
  ошибку исправляют новой версией из источника.
- Коммиты короткие, от первого лица, по факту сделанного.

## Типовые задачи

- Подключить новый источник — skill `add-source`.
- Разобраться с упавшей проверкой качества — skill `dq-triage`.
