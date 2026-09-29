-- На сколько дней хватит остатка: остаток на последнюю дату / средние
-- продажи в штуках за 28 дней. Товары, которые кончатся быстрее чем за
-- неделю, — кандидаты на срочный заказ.
-- Фильтр по date_key — это ключ секционирования fact_sales: планировщик
-- читает только последние секции (partition pruning), а не всю историю.
WITH last_snapshot AS (
    SELECT max(date_key) AS date_key FROM marts.fact_stock_daily
),
stock AS (
    SELECT s.store_sk, s.product_sk, s.qty_on_hand
    FROM marts.fact_stock_daily s
    JOIN last_snapshot l USING (date_key)
),
sales AS (
    SELECT store_sk, product_sk, sum(qty) / 28.0 AS avg_daily_qty
    FROM marts.fact_sales
    WHERE date_key >= to_char(current_date - 28, 'YYYYMMDD')::int
      AND order_status = 'PAID'
    GROUP BY 1, 2
)
SELECT st.store_code,
       p.sku,
       p.name,
       s.qty_on_hand,
       round(sa.avg_daily_qty, 2) AS avg_daily_qty,
       round(s.qty_on_hand / nullif(sa.avg_daily_qty, 0), 1) AS days_of_cover
FROM stock s
JOIN sales sa USING (store_sk, product_sk)
JOIN marts.dim_store st USING (store_sk)
JOIN marts.dim_product p USING (product_sk)
WHERE s.qty_on_hand / nullif(sa.avg_daily_qty, 0) < 7
ORDER BY days_of_cover
LIMIT 50;
