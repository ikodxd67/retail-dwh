-- Заказы, изменённые в окне водяного знака. Код магазина подтягиваем
-- здесь же: в хранилище бизнес-ключ магазина — код, а не внутренний id ERP.
SELECT o.order_id,
       o.order_no,
       s.store_code,
       o.customer_id,
       o.order_ts,
       o.channel,
       o.status,
       o.payment_type,
       o.updated_at
FROM orders o
JOIN stores s ON s.store_id = o.store_id
WHERE o.updated_at > :wm_from
  AND o.updated_at <= :wm_to
