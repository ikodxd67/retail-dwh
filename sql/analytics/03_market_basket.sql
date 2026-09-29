-- Какие категории покупают вместе: пары в одном чеке и lift.
-- lift > 1 — пара встречается чаще, чем при случайном совпадении.
-- Приём: self-join строк заказа по номеру с условием a < b, чтобы не
-- считать пару дважды и не соединять строку саму с собой.
WITH lines AS (
    SELECT DISTINCT f.order_no, p.category_name
    FROM marts.fact_sales f
    JOIN marts.v_product p USING (product_sk)
    WHERE f.order_ts >= now() - interval '90 days'
      AND f.order_status = 'PAID'
      AND p.in_catalog
),
orders AS (SELECT count(DISTINCT order_no) AS n FROM lines),
single AS (
    SELECT category_name, count(*)::numeric / (SELECT n FROM orders) AS support
    FROM lines
    GROUP BY 1
),
pairs AS (
    SELECT a.category_name AS cat_a, b.category_name AS cat_b, count(*) AS together
    FROM lines a
    JOIN lines b ON a.order_no = b.order_no AND a.category_name < b.category_name
    GROUP BY 1, 2
)
SELECT p.cat_a,
       p.cat_b,
       p.together,
       round(p.together::numeric / o.n * 100, 2) AS support_pct,
       round((p.together::numeric / o.n) / (sa.support * sb.support), 2) AS lift
FROM pairs p
CROSS JOIN orders o
JOIN single sa ON sa.category_name = p.cat_a
JOIN single sb ON sb.category_name = p.cat_b
WHERE p.together >= 100
ORDER BY lift DESC
LIMIT 20;
