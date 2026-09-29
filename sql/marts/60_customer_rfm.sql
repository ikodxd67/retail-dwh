-- RFM-сегментация по оплаченным заказам за последние 365 дней.
-- Таблица маленькая, её дешевле пересчитать целиком, чем инкрементально.
TRUNCATE marts.customer_rfm;

INSERT INTO marts.customer_rfm
    (customer_sk, customer_id, last_order_date, recency_days, orders, revenue,
     r_score, f_score, m_score, segment)
WITH orders AS (
    SELECT f.hk_customer, f.order_no, max(f.order_ts) AS order_ts, sum(f.net_amount) AS amount
    FROM marts.fact_sales f
    WHERE f.hk_customer IS NOT NULL
      AND f.order_status = 'PAID'
      AND f.order_ts >= now() - interval '365 days'
    GROUP BY f.hk_customer, f.order_no
),
per_customer AS (
    SELECT hk_customer,
           max(order_ts)::date AS last_order_date,
           count(*) AS orders,
           sum(amount) AS revenue
    FROM orders
    GROUP BY hk_customer
),
scored AS (
    SELECT p.*,
           current_date - p.last_order_date AS recency_days,
           -- NTILE делит клиентов на пять равных групп
           ntile(5) OVER (ORDER BY p.last_order_date) AS r_score,
           ntile(5) OVER (ORDER BY p.orders) AS f_score,
           ntile(5) OVER (ORDER BY p.revenue) AS m_score
    FROM per_customer p
)
SELECT d.customer_sk, d.customer_id, s.last_order_date, s.recency_days, s.orders, s.revenue,
       s.r_score, s.f_score, s.m_score,
       CASE
           WHEN s.r_score >= 4 AND s.f_score >= 4 AND s.m_score >= 4 THEN 'чемпионы'
           WHEN s.r_score >= 3 AND s.f_score >= 3 THEN 'лояльные'
           WHEN s.r_score >= 4 AND s.f_score <= 2 THEN 'новички'
           WHEN s.r_score <= 2 AND s.f_score >= 3 THEN 'уходящие'
           WHEN s.r_score <= 2 THEN 'спящие'
           ELSE 'остальные'
       END
FROM scored s
JOIN marts.dim_customer d ON d.hk_customer = s.hk_customer AND d.is_current;
