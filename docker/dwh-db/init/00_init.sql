-- Выполняется один раз, при первом запуске пустого тома.

-- метаданные Airflow живут в отдельной базе этой же инстанции
CREATE ROLE airflow LOGIN PASSWORD 'airflow';
CREATE DATABASE airflow OWNER airflow;

CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

-- Роль для аналитиков и MCP-сервера: только чтение и только витрин.
-- Права на схемы выдаются в миграции marts, когда схемы уже есть.
CREATE ROLE analyst_ro LOGIN PASSWORD 'analyst_ro';
ALTER ROLE analyst_ro SET default_transaction_read_only = on;
ALTER ROLE analyst_ro SET statement_timeout = '30s';
