-- Кто сидит на сайте: узнанные посетители (залогинены) против их покупок
-- в магазинах — RFM-сегмент, город последней офлайн-покупки, средний чек.
--
-- Один запрос через три системы:
--   lake.web.events   Delta в MinIO (кликстрим после Spark)
--   dwh.marts.*       PostgreSQL (звезда и RFM)
--   erp.erp.stores    Oracle (справочник магазинов прямо из ERP)
WITH visitors AS (
    SELECT CAST(customer_id AS varchar) AS customer_id,
           count(DISTINCT session_id) AS sessions,
           max(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS bought_online
    FROM lake.web.events
    WHERE customer_id IS NOT NULL
    GROUP BY 1
),
offline AS (
    SELECT c.customer_id,
           count(DISTINCT f.order_no) AS store_orders,
           sum(f.net_amount) / count(DISTINCT f.order_no) AS avg_check,
           max_by(s.store_code, f.order_ts) AS last_store
    FROM dwh.marts.fact_sales f
    JOIN dwh.marts.dim_customer c ON c.customer_sk = f.customer_sk
    JOIN dwh.marts.dim_store s ON s.store_sk = f.store_sk
    WHERE f.channel = 'STORE' AND f.order_status = 'PAID'
      AND f.order_ts >= current_timestamp - INTERVAL '180' DAY
    GROUP BY 1
)
SELECT coalesce(r.segment, 'нет покупок за год') AS rfm_segment,
       coalesce(st.city, '-') AS last_store_city,
       count(*) AS visitors,
       sum(v.bought_online) AS bought_online,
       round(avg(o.avg_check)) AS avg_store_check
FROM visitors v
LEFT JOIN offline o ON o.customer_id = v.customer_id
LEFT JOIN dwh.marts.customer_rfm r ON r.customer_id = v.customer_id
LEFT JOIN erp.erp.stores st ON st.store_code = o.last_store
GROUP BY 1, 2
ORDER BY visitors DESC
LIMIT 20;
