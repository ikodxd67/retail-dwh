-- Эффект промо: продажи товара в неделю акции против средней недели без
-- акции за 8 недель до неё. Приём: LATERAL-подзапрос «окно назад» для
-- каждой акции.
WITH promo_weeks AS (
    SELECT DISTINCT f.product_sk, f.promo_campaign_id, date_trunc('week', f.order_ts) AS week
    FROM marts.fact_sales f
    WHERE f.promo_campaign_id IS NOT NULL
      AND f.order_ts >= now() - interval '180 days'
),
promo_sales AS (
    SELECT pw.product_sk, pw.promo_campaign_id, pw.week, sum(f.qty) AS promo_qty
    FROM promo_weeks pw
    JOIN marts.fact_sales f
      ON f.product_sk = pw.product_sk
     AND f.order_ts >= pw.week AND f.order_ts < pw.week + interval '1 week'
     AND f.order_status = 'PAID'
    GROUP BY 1, 2, 3
)
SELECT p.sku,
       p.category_name,
       ps.promo_campaign_id,
       ps.promo_qty,
       round(base.avg_week_qty, 1) AS base_week_qty,
       round(100 * (ps.promo_qty / nullif(base.avg_week_qty, 0) - 1)) AS uplift_pct
FROM promo_sales ps
JOIN marts.v_product p USING (product_sk)
CROSS JOIN LATERAL (
    SELECT sum(f.qty) / 8.0 AS avg_week_qty
    FROM marts.fact_sales f
    WHERE f.product_sk = ps.product_sk
      AND f.order_ts >= ps.week - interval '8 weeks' AND f.order_ts < ps.week
      AND f.promo_campaign_id IS NULL
      AND f.order_status = 'PAID'
) base
WHERE base.avg_week_qty > 1
ORDER BY uplift_pct DESC
LIMIT 30;
