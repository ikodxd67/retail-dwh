-- Staging: данные источников как есть, плюс технические поля партии.
-- Типы приведены, смысл не менялся. Время ERP (локальное, без зоны)
-- переведено в timestamptz уже при загрузке.
CREATE SCHEMA IF NOT EXISTS stg;

CREATE TABLE stg.erp_stores (
    store_id    integer,
    store_code  text,
    name        text,
    city        text,
    format      text,
    area_sqm    numeric(7,1),
    opened_on   date,
    closed_on   date,
    updated_at  timestamptz,
    _batch_id   bigint NOT NULL,
    _loaded_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE stg.erp_orders (
    order_id      bigint,
    order_no      text,
    store_code    text,
    customer_id   bigint,
    order_ts      timestamptz,
    channel       text,
    status        text,
    payment_type  text,
    updated_at    timestamptz,
    _batch_id     bigint NOT NULL,
    _loaded_at    timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE stg.erp_order_lines (
    order_no      text,
    line_no       integer,
    sku           text,
    qty           numeric(10,3),
    unit_price    numeric(12,2),
    discount_amt  numeric(12,2),
    updated_at    timestamptz,
    _batch_id     bigint NOT NULL,
    _loaded_at    timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE stg.crm_customers (
    customer_id        bigint,
    email              text,
    phone              text,
    first_name         text,
    last_name          text,
    birth_date         date,
    gender             text,
    city               text,
    loyalty_tier       text,
    marketing_consent  boolean,
    created_at         timestamptz,
    updated_at         timestamptz,
    deleted_at         timestamptz,
    _batch_id          bigint NOT NULL,
    _loaded_at         timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE stg.catalog_products (
    snapshot_date      date,
    sku                text,
    status             text,
    name               text,
    brand              text,
    group_name         text,
    category_name      text,
    subcategory_name   text,
    unit               text,
    supplier_code      text,
    purchase_price     numeric(14,2),
    purchase_currency  text,
    retail_price       numeric(12,2),
    eans               text[],
    _batch_id          bigint NOT NULL,
    _loaded_at         timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE stg.stock_snapshots (
    snapshot_date  date,
    store_code     text,
    sku            text,
    qty_on_hand    numeric(12,3),
    stock_cost     numeric(14,2),
    _batch_id      bigint NOT NULL,
    _loaded_at     timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE stg.promo_items (
    campaign_id    text,
    title          text,
    channels       text[],
    valid_from     date,
    valid_to       date,
    sku            text,
    mechanics      text,
    discount_pct   numeric(5,2),
    _batch_id      bigint NOT NULL,
    _loaded_at     timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE stg.fx_rates (
    rate_date   date,
    currency    text,
    rate        numeric(14,4),
    quant       integer,
    _batch_id   bigint NOT NULL,
    _loaded_at  timestamptz NOT NULL DEFAULT now()
);

-- Все выборки из stg идут по номеру партии
DO $$
DECLARE t text;
BEGIN
    FOR t IN SELECT table_name FROM information_schema.tables WHERE table_schema = 'stg' LOOP
        EXECUTE format('CREATE INDEX %I ON stg.%I (_batch_id)', t || '_batch_ix', t);
    END LOOP;
END $$;
