-- ClickHouse — слой для быстрых дашбордов и ad-hoc аналитики поверх витрин.
-- Источник правды остаётся в PostgreSQL; сюда данные только публикуются.
CREATE DATABASE IF NOT EXISTS retail;

-- Денормализованная копия звезды: в колоночной базе JOIN-ы на лету дороже,
-- чем хранить атрибуты измерений рядом с фактами (они отлично сжимаются).
CREATE TABLE IF NOT EXISTS retail.sales_wide
(
    order_date         Date,
    order_ts           DateTime64(3, 'Asia/Almaty'),
    order_no           String,
    line_no            UInt16,
    store_code         LowCardinality(String),
    store_city         LowCardinality(String),
    store_format       LowCardinality(String),
    sku                String,
    product_name       String,
    brand              LowCardinality(String),
    group_name         LowCardinality(String),
    category_name      LowCardinality(String),
    subcategory_name   LowCardinality(String),
    customer_id        String,
    loyalty_tier       LowCardinality(String),
    customer_city      LowCardinality(String),
    channel            LowCardinality(String),
    payment_type       LowCardinality(String),
    order_status       LowCardinality(String),
    promo_campaign_id  String,
    qty                Decimal(10, 3),
    gross_amount       Decimal(14, 2),
    discount_amount    Decimal(14, 2),
    net_amount         Decimal(14, 2),
    cost_amount        Decimal(14, 2),
    margin_amount      Decimal(14, 2)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(order_date)
ORDER BY (order_date, store_code, sku);

-- Воронка сайта по дням из Delta (silver sessions). ReplacingMergeTree:
-- день пересчитывается несколько раз, оставляем последнюю версию.
CREATE TABLE IF NOT EXISTS retail.web_funnel_daily
(
    session_date      Date,
    device            LowCardinality(String),
    traffic_source    LowCardinality(String),
    sessions          UInt32,
    with_cart         UInt32,
    with_checkout     UInt32,
    with_purchase     UInt32,
    revenue           Float64,
    avg_duration_sec  Float64,
    updated_at        DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (session_date, device, traffic_source);
