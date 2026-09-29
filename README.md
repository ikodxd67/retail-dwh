# retail-dwh

Хранилище данных для сети из 37 магазинов и интернет-магазина. Данные
приходят из пяти источников разной природы и собираются в Data Vault, из
которого строятся звёздные витрины. Продажи публикуются в ClickHouse,
кликстрим сайта идёт через Kafka и Spark в Delta Lake, а поверх всего
работает Trino. Оркестрация — Airflow 3, мониторинг — Prometheus и Grafana.
Агенты (Claude Code и подобные) работают с хранилищем через собственный
MCP-сервер.

```
Oracle ERP ─┐
CRM (PG)   ─┤                     ┌─> звезда (PG) ──> ClickHouse ──┐
XML/CSV/JSON┼─> stg ─> Data Vault ┤                                ├─> Trino ─> аналитики, MCP
API НБРК   ─┘                     └─> проверки качества            │
Kafka ──> Spark (Connect) ──> Delta в S3 ──────────────────────────┘
```

Подробнее: [архитектура](docs/architecture.md) · [решения и компромиссы](docs/decisions.md) ·
[замеры](docs/performance.md) · [runbook](docs/runbook.md)

## Что внутри

| | |
|---|---|
| **Источники** | Oracle Free 23ai (ERP: заказы, строки, магазины), PostgreSQL (CRM с изменениями клиентов), файлы поставщика и склада в S3: каталог в **XML**, остатки в **CSV.gz** (разделитель `;`, запятая в дробях, BOM), промо в **JSON**; настоящее **XML API Нацбанка РК**; поток событий сайта в **Kafka** |
| **Загрузка** | Один источник описывается одним YAML (`pipelines/`), по нему фабрика строит DAG. Инкремент по водяному знаку с перекрытием, атомарные партии, карантин партий, не прошедших проверку, реестр файлов по ETag |
| **DWH** | **Data Vault 2.0**: DDL и SQL загрузки генерируются из `models/vault.yml`. Витрины — **звезда** со **снежинкой** по товарам, **SCD2** по клиентам, секционированный факт, MERGE |
| **SQL** | CTE, оконные функции (LAG, NTILE, накопительные суммы), LATERAL, MERGE, self-join, as-of соединения по интервалам. Аналитика в [`sql/analytics/`](sql/analytics), оптимизация с замерами в [`docs/performance.md`](docs/performance.md) |
| **Airflow 3** | фабрика DAG из YAML, свои `IngestOperator`, `LandingFilesSensor` (reschedule), `DataQualityOperator`; расписание по Asset-ам (data-aware) |
| **Стриминг** | Kafka → Spark 3.5 через **Spark Connect** → **Delta** (bronze, дедупликация по событию в пределах водяного знака, карантин битых сообщений) → сессии (silver) → воронка в ClickHouse |
| **ClickHouse** | публикация помесячно: ClickHouse сам читает PostgreSQL через `postgresql()`, месяц подменяется атомарно `REPLACE PARTITION` со сверкой числа строк |
| **Trino** | один SQL поверх Oracle, PostgreSQL, ClickHouse и Delta ([`sql/trino/`](sql/trino)) |
| **Качество данных** | проверки партий stg и витрин: сверка с vault до тенге, ссылочная целостность, непрерывность SCD2, свежесть |
| **Мониторинг** | метрики Airflow (StatsD), метрики загрузок и проверок (Pushgateway), алерты о свежести данных, дашборд Grafana |
| **Governance** | каталог с владельцами и классами PII, **lineage, вычисленный из кода** (sqlglot); тест, что персональные данные не доходят до витрин |
| **AI-агенты** | MCP-сервер: поиск по каталогу, lineage, состояние пайплайнов, DQ, SQL только на чтение (права в Trino, а не только парсер). Skills для типовых задач: подключить источник, разобрать упавшую проверку |
| **CI** | GitHub Actions: линтер и юнит-тесты; тесты DAG-ов в том же образе Airflow; интеграционные тесты на живых Oracle и PostgreSQL |

## Данные

Источники наполняет генератор (`src/retail_dwh/generator/`). Он
детерминированный, поэтому цены в заказах ERP совпадают с каталогом
поставщика. В данных есть рост, недельная и годовая сезонность, акции,
возвраты и опоздания выгрузки с касс. Грязь заложена намеренно: клиенты из
заказов, которых нет в CRM, артикулы, которых нет в каталоге, дубли e-mail,
телефоны в разных форматах, битые и повторные события в Kafka.

На стенде: **543 тыс. заказов, 1,45 млн строк заказов, 46,5 тыс. клиентов,
2 млн строк остатков**, история с января 2025. Курсы валют — настоящие, с API НБРК.

## Запуск

Нужен Docker (память для Docker Desktop — 8 ГБ) и Python 3.12.

```bash
python -m venv .venv && .venv/Scripts/activate      # Linux/macOS: source .venv/bin/activate
pip install -e ".[dev,lake,mcp]"
cp .env.example .env

docker compose up -d                                 # Oracle, CRM, DWH, S3
retail-dwh migrate
retail-dwh simulate                                  # история с 2025 года, несколько минут
retail-dwh ingest && retail-dwh vault && retail-dwh marts && retail-dwh dq
```

Дальше по желанию, профилями (целиком стек в 8 ГБ не помещается):

```bash
docker compose --profile airflow up -d      # http://localhost:8080
docker compose --profile lake up -d         # Kafka, Spark :4040, Trino :8085, ClickHouse :8123
docker compose --profile monitoring up -d   # Grafana :3000, Prometheus :9090
```

Тесты:

```bash
pytest -m "not integration"   # 42 теста, секунды
pytest -m integration         # против поднятого стенда
```

## Структура

```
pipelines/          по YAML на источник -> DAG ingest__<имя>
models/vault.yml    модель Data Vault (из неё DDL и загрузка)
sql/
  extract/          запросы к источникам
  dwh/              миграции хранилища
  marts/            сборка витрин по шагам
  analytics/        аналитические запросы
  trino/            федеративные запросы
catalog/            каталог данных и проверки витрин
dags/               DAG-и Airflow
spark/jobs/         Spark: Kafka -> Delta -> сессии
src/retail_dwh/     код: загрузка, vault, витрины, DQ, публикация, каталог, MCP
docker/             образы и конфиги сервисов
.claude/skills/     инструкции для AI-агентов
```

## Чего здесь нет

Здесь нет Greenplum и Vertica: вместо них PostgreSQL и ClickHouse, причины в
[decisions.md](docs/decisions.md). Нет Kubernetes и Helm: стенд работает на одной
машине в docker compose. Вместо AWS S3 — SeaweedFS с тем же API (MinIO был, но в 2026 его образы
перестали быть доступны — см. [decisions.md](docs/decisions.md)). Нет
Alertmanager: алерты видны в Prometheus, но никуда не отправляются.
