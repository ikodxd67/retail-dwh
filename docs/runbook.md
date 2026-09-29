# Runbook

Что делать, когда сработал алерт. Сначала смотри, потом чини. Данные в
`raw_vault` руками не правятся никогда.

Быстрый обзор состояния одним вызовом: MCP-инструмент `pipeline_status` или

```sql
SELECT pipeline, status, count(*), max(finished_at)
FROM meta.batches
WHERE started_at > now() - interval '1 day'
GROUP BY 1, 2 ORDER BY 1, 2;
```

## Источник не грузится

Алерты `IngestStaleFrequent`, `IngestStaleDaily`, `IngestFailed`.

1. Лог последней задачи `ingest__<pipeline>` в Airflow.
2. Ошибка в журнале: `SELECT * FROM meta.batches WHERE pipeline = '...' ORDER BY batch_id DESC LIMIT 5`.
3. Частые причины:
   - **Источник недоступен** (Oracle, CRM, MinIO, API НБРК) — проверь сервис
     (`docker compose ps`). После восстановления ничего делать не нужно:
     водяной знак не сдвигался, следующий запуск заберёт всё пропущенное.
   - **Файловый пайплайн «молчит»** — это не обязательно авария: сенсор ждёт
     файл до 6 часов и потом помечает запуск как skipped. Проверь, прислал
     ли поставщик файл в `landing/`.
   - **API НБРК вернул пустой ответ на сегодня** — курс ещё не опубликован,
     водяной знак остановится на вчерашнем дне, и следующий запуск повторит.
4. Если источник отдал данные за прошлый период задним числом (правка в
   ERP старше окна перекрытия), сдвинь водяной знак назад:
   `UPDATE meta.watermarks SET value = '<время>' WHERE pipeline = '...'`.
   Повторная загрузка безопасна, vault отбросит неизменённые строки.

## Провалена проверка качества

Алерты `DataQualityFailed`, `DataQualityWarning`. Подробный порядок разбора
описан в skill `.claude/skills/dq-triage`. Коротко:

- **stg (проверки пайплайна)** — партия в статусе `rejected`, в vault не
  попала, дальше ничего не сломано. Найди причину в источнике. Если данные
  на самом деле верны (например, порог проверки устарел), исправь проверку и
  отпусти партию: `retail-dwh batch release <id>`.
- **Витрины (`catalog/dq_marts.yml`)** — задача `dwh_build.dq_marts` красная.
  Витрины уже пересобраны, публикация в ClickHouse не запустилась, потому что
  Asset не обновился. Разберись, почини и перезапусти `dwh_build`.

| Проверка | Что обычно значит |
|---|---|
| `lines_match_vault` | строка заказа потерялась между vault и фактом: чаще всего новый код в `sql/marts/40_fact_sales.sql` |
| `revenue_matches_vault` | расхождение в деньгах: ошибка в формуле или статусе |
| `scd2_single_current`, `scd2_no_gaps_or_overlaps` | история клиента сломана: повторная сборка с тем же водяным знаком после частичного сбоя. Лечится `retail-dwh marts --full` |
| `customers_missing_in_crm_pct` | CRM отстаёт от ERP; обычно проходит само после загрузки CRM |
| `freshness_hours` | заказы не доезжают: смотри раздел выше |

## Витрины устарели

Алерт `MartsStale`.

1. Запускается ли `dwh_build`? Он стартует по Asset-ам: если загрузки идут,
   а сборка нет, проверь, не на паузе ли DAG.
2. Не держит ли кто-то блокировку:
   `SELECT pid, state, query_start, query FROM pg_stat_activity WHERE query ILIKE '%advisory%'`.
   Зависшую сессию можно завершить через `pg_terminate_backend(pid)`:
   сборка идёт одной транзакцией и откатится целиком.
3. Если вся история витрин под сомнением: `retail-dwh marts --full`. Это
   занимает примерно 2,5 минуты на стенде, идёт одной транзакцией, и читатели
   до конца видят старые данные.

## ClickHouse расходится с PostgreSQL

Публикация сама сверяет число строк месяца перед подменой партиции и
падает, если оно не совпало. Повторный запуск `publish_clickhouse`
безопасен. Полная перепубликация:

```python
from retail_dwh.publish import publish_sales
publish_sales(full=True)
```

Сверка по дням с источником: `sql/trino/02_reconcile_erp_vs_clickhouse.sql`.

## Kafka, Spark и кликстрим

- `web_events` красный, в логе ошибка подключения к `sc://spark:15002` —
  Spark Connect не запущен или упал по памяти: `docker compose --profile lake
  up -d spark`. Смещения Kafka лежат в чекпоинте, поэтому ничего не
  потеряется, пока сообщения не старше retention (72 часа).
- Битые сообщения копятся в `lake.web.events_quarantine`, поток при этом не
  останавливается. Посмотреть их можно так:
  `SELECT raw, kafka_ts FROM lake.web.events_quarantine ORDER BY kafka_ts DESC LIMIT 20`.
