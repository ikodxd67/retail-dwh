-- Плоское представление звезды для выгрузки в ClickHouse и BI.
-- PII сюда не попадает: из клиента только id, город и уровень лояльности.
CREATE VIEW marts.v_sales_wide AS
SELECT d.date AS order_date,
       f.date_key,
       f.order_ts,
       f.order_no,
       f.line_no,
       s.store_code,
       coalesce(s.city, '') AS store_city,
       coalesce(s.format, '') AS store_format,
       p.sku,
       coalesce(p.name, '') AS product_name,
       coalesce(p.brand, '') AS brand,
       p.group_name,
       p.category_name,
       p.subcategory_name,
       c.customer_id,
       coalesce(c.loyalty_tier, '') AS loyalty_tier,
       coalesce(c.city, '') AS customer_city,
       f.channel,
       coalesce(f.payment_type, '') AS payment_type,
       f.order_status,
       coalesce(f.promo_campaign_id, '') AS promo_campaign_id,
       f.qty,
       f.gross_amount,
       f.discount_amount,
       f.net_amount,
       coalesce(f.cost_amount, 0) AS cost_amount,
       coalesce(f.margin_amount, 0) AS margin_amount,
       f.updated_at
FROM marts.fact_sales f
JOIN marts.dim_date d USING (date_key)
JOIN marts.dim_store s USING (store_sk)
JOIN marts.v_product p USING (product_sk)
JOIN marts.dim_customer c USING (customer_sk);

GRANT SELECT ON marts.v_sales_wide TO analyst_ro;
