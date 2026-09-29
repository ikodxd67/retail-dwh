-- Удержание когорт: месяц первой покупки -> доля клиентов, вернувшихся
-- через 1, 2, 3 и 6 месяцев. Приёмы: MIN() OVER для когорты, разница в
-- месяцах через age(), агрегаты с FILTER вместо pivot.
WITH purchases AS (
    SELECT DISTINCT hk_customer, date_trunc('month', order_ts)::date AS month
    FROM marts.fact_sales
    WHERE hk_customer IS NOT NULL AND order_status = 'PAID'
),
cohorted AS (
    SELECT hk_customer,
           month,
           min(month) OVER (PARTITION BY hk_customer) AS cohort
    FROM purchases
),
offsets AS (
    SELECT cohort,
           hk_customer,
           (extract(year FROM age(month, cohort)) * 12 + extract(month FROM age(month, cohort)))::int
               AS month_no
    FROM cohorted
)
SELECT cohort,
       count(DISTINCT hk_customer) FILTER (WHERE month_no = 0) AS customers,
       round(100.0 * count(DISTINCT hk_customer) FILTER (WHERE month_no = 1)
             / count(DISTINCT hk_customer) FILTER (WHERE month_no = 0), 1) AS m1_pct,
       round(100.0 * count(DISTINCT hk_customer) FILTER (WHERE month_no = 2)
             / count(DISTINCT hk_customer) FILTER (WHERE month_no = 0), 1) AS m2_pct,
       round(100.0 * count(DISTINCT hk_customer) FILTER (WHERE month_no = 3)
             / count(DISTINCT hk_customer) FILTER (WHERE month_no = 0), 1) AS m3_pct,
       round(100.0 * count(DISTINCT hk_customer) FILTER (WHERE month_no = 6)
             / count(DISTINCT hk_customer) FILTER (WHERE month_no = 0), 1) AS m6_pct
FROM offsets
GROUP BY cohort
ORDER BY cohort;
