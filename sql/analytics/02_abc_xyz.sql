-- ABC/XYZ-анализ ассортимента за последние 26 недель.
-- ABC — вклад в выручку (накопленная доля, оконная сумма по убыванию).
-- XYZ — стабильность спроса (коэффициент вариации недельных продаж).
WITH weekly AS (
    SELECT f.product_sk,
           date_trunc('week', f.order_ts) AS week,
           sum(f.net_amount) AS revenue
    FROM marts.fact_sales f
    WHERE f.order_ts >= now() - interval '26 weeks'
      AND f.order_status = 'PAID'
    GROUP BY 1, 2
),
-- недели без продаж — это нули, а не отсутствие строки; без них спрос
-- выглядел бы стабильнее, чем он есть
weeks AS (
    SELECT generate_series(date_trunc('week', now() - interval '26 weeks'),
                           date_trunc('week', now()), interval '1 week') AS week
),
grid AS (
    SELECT p.product_sk, w.week, coalesce(x.revenue, 0) AS revenue
    FROM (SELECT DISTINCT product_sk FROM weekly) p
    CROSS JOIN weeks w
    LEFT JOIN weekly x USING (product_sk, week)
),
per_product AS (
    SELECT product_sk,
           sum(revenue) AS revenue,
           stddev_pop(revenue) / nullif(avg(revenue), 0) AS cv
    FROM grid
    GROUP BY product_sk
),
ranked AS (
    SELECT *,
           sum(revenue) OVER (ORDER BY revenue DESC ROWS UNBOUNDED PRECEDING)
               / sum(revenue) OVER () AS cum_share
    FROM per_product
)
SELECT p.sku,
       p.name,
       p.category_name,
       round(r.revenue) AS revenue,
       round(r.cum_share * 100, 1) AS cum_share_pct,
       CASE WHEN r.cum_share <= 0.8 THEN 'A' WHEN r.cum_share <= 0.95 THEN 'B' ELSE 'C' END AS abc,
       CASE WHEN r.cv <= 0.25 THEN 'X' WHEN r.cv <= 0.5 THEN 'Y' ELSE 'Z' END AS xyz
FROM ranked r
JOIN marts.v_product p USING (product_sk)
ORDER BY r.revenue DESC;
