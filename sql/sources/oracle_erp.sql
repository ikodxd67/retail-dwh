-- ERP сети магазинов (Oracle). Схема ERP создаётся образом gvenzl/oracle-free
-- через APP_USER. Каждая команда отделена строкой из одного "/".

CREATE TABLE IF NOT EXISTS stores (
    store_id    NUMBER(6)      PRIMARY KEY,
    store_code  VARCHAR2(10)   NOT NULL UNIQUE,
    name        VARCHAR2(100)  NOT NULL,
    city        VARCHAR2(50)   NOT NULL,
    format      VARCHAR2(20)   NOT NULL,
    area_sqm    NUMBER(7,1),
    opened_on   DATE           NOT NULL,
    closed_on   DATE,
    updated_at  TIMESTAMP      DEFAULT SYSTIMESTAMP NOT NULL
)
/
CREATE TABLE IF NOT EXISTS orders (
    order_id      NUMBER(12)    PRIMARY KEY,
    order_no      VARCHAR2(20)  NOT NULL UNIQUE,
    store_id      NUMBER(6)     NOT NULL REFERENCES stores,
    customer_id   NUMBER(10),
    order_ts      TIMESTAMP     NOT NULL,
    channel       VARCHAR2(10)  NOT NULL,
    status        VARCHAR2(12)  NOT NULL,
    payment_type  VARCHAR2(10),
    updated_at    TIMESTAMP     NOT NULL
)
/
CREATE INDEX IF NOT EXISTS orders_updated_ix ON orders (updated_at)
/
CREATE TABLE IF NOT EXISTS order_lines (
    order_id      NUMBER(12)    NOT NULL REFERENCES orders,
    line_no       NUMBER(4)     NOT NULL,
    sku           VARCHAR2(20)  NOT NULL,
    qty           NUMBER(10,3)  NOT NULL,
    unit_price    NUMBER(12,2)  NOT NULL,
    discount_amt  NUMBER(12,2)  DEFAULT 0 NOT NULL,
    updated_at    TIMESTAMP     NOT NULL,
    CONSTRAINT order_lines_pk PRIMARY KEY (order_id, line_no)
)
/
CREATE INDEX IF NOT EXISTS order_lines_updated_ix ON order_lines (updated_at)
/
