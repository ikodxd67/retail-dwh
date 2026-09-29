-- Роль для Grafana: журнал загрузок, результаты проверок и витрины.
DO $$ BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'monitoring_ro') THEN
        CREATE ROLE monitoring_ro LOGIN PASSWORD 'monitoring_ro';
    END IF;
END $$;
ALTER ROLE monitoring_ro SET default_transaction_read_only = on;
GRANT USAGE ON SCHEMA meta, marts TO monitoring_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA meta, marts TO monitoring_ro;
ALTER DEFAULT PRIVILEGES IN SCHEMA meta GRANT SELECT ON TABLES TO monitoring_ro;
ALTER DEFAULT PRIVILEGES IN SCHEMA marts GRANT SELECT ON TABLES TO monitoring_ro;
