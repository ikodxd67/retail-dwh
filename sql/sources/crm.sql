-- CRM (PostgreSQL): клиенты и программа лояльности.
CREATE TABLE IF NOT EXISTS customers (
    customer_id        bigint PRIMARY KEY,
    email              text,
    phone              text,
    first_name         text NOT NULL,
    last_name          text NOT NULL,
    birth_date         date,
    gender             char(1),
    city               text,
    loyalty_tier       text NOT NULL DEFAULT 'basic',
    marketing_consent  boolean NOT NULL DEFAULT false,
    created_at         timestamptz NOT NULL,
    updated_at         timestamptz NOT NULL,
    deleted_at         timestamptz
);
CREATE INDEX IF NOT EXISTS customers_updated_ix ON customers (updated_at);
