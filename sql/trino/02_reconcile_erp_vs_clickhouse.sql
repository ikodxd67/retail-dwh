-- Сверка «источник против конечной витрины» по дням за последнюю неделю:
-- число заказов в Oracle (ERP) и в ClickHouse после всего пути
-- ERP -> stg -> vault -> звезда -> ClickHouse. Расхождение за сегодня
-- нормально (загрузка раз в 30 минут), за прошлые дни — повод разбираться.
WITH erp AS (
    SELECT CAST(order_ts AS date) AS day, count(*) AS orders
    FROM erp.erp.orders
    WHERE order_ts >= current_date - INTERVAL '7' DAY
    GROUP BY 1
),
ch AS (
    SELECT order_date AS day, count(DISTINCT order_no) AS orders
    FROM ch.retail.sales_wide
    WHERE order_date >= current_date - INTERVAL '7' DAY
    GROUP BY 1
)
SELECT coalesce(e.day, c.day) AS day,
       e.orders AS erp_orders,
       c.orders AS clickhouse_orders,
       coalesce(e.orders, 0) - coalesce(c.orders, 0) AS diff
FROM erp e
FULL JOIN ch c ON c.day = e.day
ORDER BY day;
