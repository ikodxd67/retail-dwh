-- Строки заказов. Номер заказа нужен как бизнес-ключ, поэтому JOIN с orders.
-- Индекс order_lines_updated_ix делает выборку по окну дешёвой.
SELECT o.order_no,
       l.line_no,
       l.sku,
       l.qty,
       l.unit_price,
       l.discount_amt,
       l.updated_at
FROM order_lines l
JOIN orders o ON o.order_id = l.order_id
WHERE l.updated_at > :wm_from
  AND l.updated_at <= :wm_to
