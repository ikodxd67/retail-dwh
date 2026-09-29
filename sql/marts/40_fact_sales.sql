-- Факт продаж, инкрементально: пересчитываются только заказы, у которых
-- с прошлой сборки появилась новая версия заказа или любой его строки.

CREATE TEMP TABLE _orders ON COMMIT DROP AS
SELECT hk_order FROM raw_vault.sat_order_erp WHERE load_dts > %(since)s
UNION
SELECT l.hk_order
FROM raw_vault.sat_order_line_erp s
JOIN raw_vault.lnk_order_line l USING (hk_order_line)
WHERE s.load_dts > %(since)s;
ALTER TABLE _orders ADD PRIMARY KEY (hk_order);
ANALYZE _orders;

-- Закупочная цена действует от снимка каталога до следующего снимка.
-- Интервалы строим один раз оконной функцией и соединяем по диапазону,
-- вместо того чтобы на каждую строку заказа искать «последний снимок до даты».
CREATE TEMP TABLE _supplier_price ON COMMIT DROP AS
SELECT hk_product, purchase_price, purchase_currency,
       CASE WHEN row_number() OVER w = 1 THEN '-infinity'::timestamptz ELSE applied_ts END
           AS valid_from,
       coalesce(lead(applied_ts) OVER w, 'infinity') AS valid_to
FROM raw_vault.sat_product_supplier
WINDOW w AS (PARTITION BY hk_product ORDER BY applied_ts, load_dts);
CREATE INDEX ON _supplier_price (hk_product, valid_from);
ANALYZE _supplier_price;

-- Курс на дату заказа. НБРК публикует курс и в выходные, но если дня нет,
-- действует последний известный.
CREATE TEMP TABLE _fx ON COMMIT DROP AS
SELECT currency, rate_per_unit, rate_date,
       coalesce(lead(rate_date) OVER (PARTITION BY currency ORDER BY rate_date), 'infinity')
           AS next_date
FROM raw_vault.ref_fx_rate;
CREATE INDEX ON _fx (currency, rate_date);
ANALYZE _fx;

-- действующая акция на товар в день заказа (если их несколько — с большей скидкой)
CREATE TEMP TABLE _promo ON COMMIT DROP AS
SELECT DISTINCT ON (l.hk_product, p.valid_from)
       l.hk_product, hp.campaign_id, p.valid_from, p.valid_to, pp.discount_pct
FROM raw_vault.lnk_promo_product l
JOIN raw_vault.hub_promo hp USING (hk_promo)
JOIN LATERAL (
    SELECT valid_from, valid_to FROM raw_vault.sat_promo s
    WHERE s.hk_promo = l.hk_promo ORDER BY s.load_dts DESC LIMIT 1
) p ON true
JOIN LATERAL (
    SELECT discount_pct FROM raw_vault.sat_promo_product s
    WHERE s.hk_promo_product = l.hk_promo_product ORDER BY s.load_dts DESC LIMIT 1
) pp ON true
ORDER BY l.hk_product, p.valid_from, pp.discount_pct DESC;
CREATE INDEX ON _promo (hk_product, valid_from);
ANALYZE _promo;

CREATE TEMP TABLE _sales ON COMMIT DROP AS
WITH ord AS (
    SELECT DISTINCT ON (s.hk_order) s.hk_order, s.order_ts, s.channel, s.status, s.payment_type
    FROM raw_vault.sat_order_erp s
    JOIN _orders USING (hk_order)
    ORDER BY s.hk_order, s.load_dts DESC
),
party AS (
    -- покупатель и магазин заказа; если ERP переписал заказ на другого
    -- клиента, в линке будет несколько строк — берём последнюю
    SELECT DISTINCT ON (l.hk_order) l.hk_order, l.hk_customer, l.hk_store, hc.customer_id
    FROM raw_vault.lnk_order_customer_store l
    JOIN _orders USING (hk_order)
    JOIN raw_vault.hub_customer hc USING (hk_customer)
    ORDER BY l.hk_order, l.load_dts DESC
),
lines AS (
    SELECT l.hk_order, l.hk_product, l.line_no::int AS line_no, v.qty, v.unit_price, v.discount_amt
    FROM raw_vault.lnk_order_line l
    JOIN _orders USING (hk_order)
    JOIN LATERAL (
        SELECT qty, unit_price, discount_amt FROM raw_vault.sat_order_line_erp s
        WHERE s.hk_order_line = l.hk_order_line
        ORDER BY s.load_dts DESC LIMIT 1
    ) v ON true
)
SELECT to_char(o.order_ts AT TIME ZONE 'Asia/Almaty', 'YYYYMMDD')::int AS date_key,
       ho.order_no,
       li.line_no,
       o.order_ts,
       coalesce(ds.store_sk, -1) AS store_sk,
       coalesce(dp.product_sk, -1) AS product_sk,
       coalesce(dc.customer_sk, CASE WHEN pa.customer_id = '-1' THEN -1 ELSE -2 END) AS customer_sk,
       CASE WHEN pa.customer_id = '-1' THEN NULL ELSE pa.hk_customer END AS hk_customer,
       o.channel,
       o.payment_type,
       o.status AS order_status,
       pr.campaign_id AS promo_campaign_id,
       li.qty,
       li.unit_price,
       round(li.qty * li.unit_price, 2) AS gross_amount,
       li.discount_amt AS discount_amount,
       -- отменённый или возвращённый заказ выручки не даёт, но строка остаётся:
       -- по ней считают долю возвратов
       CASE WHEN o.status = 'PAID' THEN round(li.qty * li.unit_price - li.discount_amt, 2) ELSE 0 END
           AS net_amount,
       round(sp.purchase_price
             * CASE WHEN sp.purchase_currency = 'KZT' THEN 1 ELSE fx.rate_per_unit END, 2)
           AS unit_cost_kzt,
       fx.rate_date AS fx_rate_date
FROM ord o
JOIN raw_vault.hub_order ho USING (hk_order)
JOIN party pa USING (hk_order)
JOIN lines li USING (hk_order)
LEFT JOIN marts.dim_store ds ON ds.hk_store = pa.hk_store
LEFT JOIN marts.dim_product dp ON dp.hk_product = li.hk_product
LEFT JOIN marts.dim_customer dc
       ON dc.hk_customer = pa.hk_customer
      AND o.order_ts >= dc.valid_from AND o.order_ts < dc.valid_to
LEFT JOIN _supplier_price sp
       ON sp.hk_product = li.hk_product
      AND o.order_ts >= sp.valid_from AND o.order_ts < sp.valid_to
LEFT JOIN _fx fx
       ON fx.currency = sp.purchase_currency
      AND (o.order_ts AT TIME ZONE 'Asia/Almaty')::date >= fx.rate_date
      AND (o.order_ts AT TIME ZONE 'Asia/Almaty')::date < fx.next_date
LEFT JOIN _promo pr
       ON pr.hk_product = li.hk_product
      AND (o.order_ts AT TIME ZONE 'Asia/Almaty')::date BETWEEN pr.valid_from AND pr.valid_to;

MERGE INTO marts.fact_sales t
USING (
    SELECT s.*,
           CASE WHEN s.order_status = 'PAID' THEN round(s.qty * s.unit_cost_kzt, 2) ELSE 0 END
               AS cost_amount,
           md5(row(s.*)::text)::uuid AS row_hash
    FROM _sales s
) s
ON t.date_key = s.date_key AND t.order_no = s.order_no AND t.line_no = s.line_no
WHEN MATCHED AND t.row_hash <> s.row_hash THEN UPDATE SET
    order_ts = s.order_ts, store_sk = s.store_sk, product_sk = s.product_sk,
    customer_sk = s.customer_sk, hk_customer = s.hk_customer, channel = s.channel,
    payment_type = s.payment_type, order_status = s.order_status,
    promo_campaign_id = s.promo_campaign_id, qty = s.qty, unit_price = s.unit_price,
    gross_amount = s.gross_amount, discount_amount = s.discount_amount,
    net_amount = s.net_amount, unit_cost_kzt = s.unit_cost_kzt, cost_amount = s.cost_amount,
    margin_amount = s.net_amount - s.cost_amount, fx_rate_date = s.fx_rate_date,
    row_hash = s.row_hash, updated_at = now()
WHEN NOT MATCHED THEN INSERT
    (date_key, order_no, line_no, order_ts, store_sk, product_sk, customer_sk, hk_customer,
     channel, payment_type, order_status, promo_campaign_id, qty, unit_price, gross_amount,
     discount_amount, net_amount, unit_cost_kzt, cost_amount, margin_amount, fx_rate_date, row_hash)
VALUES
    (s.date_key, s.order_no, s.line_no, s.order_ts, s.store_sk, s.product_sk, s.customer_sk,
     s.hk_customer, s.channel, s.payment_type, s.order_status, s.promo_campaign_id, s.qty,
     s.unit_price, s.gross_amount, s.discount_amount, s.net_amount, s.unit_cost_kzt,
     s.cost_amount, s.net_amount - s.cost_amount, s.fx_rate_date, s.row_hash);
