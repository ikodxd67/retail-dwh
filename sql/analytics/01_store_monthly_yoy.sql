-- Выручка магазинов по месяцам: рост к прошлому месяцу и к тому же месяцу
-- прошлого года, место магазина в своём городе.
-- Приёмы: CTE, LAG со смещением 1 и 12, RANK в разрезе города.
WITH monthly AS (
    SELECT s.store_code,
           s.city,
           date_trunc('month', d.date)::date AS month,
           sum(f.net_amount) AS revenue
    FROM marts.fact_sales f
    JOIN marts.dim_date d USING (date_key)
    JOIN marts.dim_store s USING (store_sk)
    WHERE s.format <> 'ONLINE'
    GROUP BY 1, 2, 3
)
SELECT store_code,
       city,
       month,
       revenue,
       round(100 * (revenue / nullif(lag(revenue) OVER w, 0) - 1), 1) AS mom_pct,
       round(100 * (revenue / nullif(lag(revenue, 12) OVER w, 0) - 1), 1) AS yoy_pct,
       rank() OVER (PARTITION BY city, month ORDER BY revenue DESC) AS rank_in_city
FROM monthly
WINDOW w AS (PARTITION BY store_code ORDER BY month)
ORDER BY month DESC, city, rank_in_city;
