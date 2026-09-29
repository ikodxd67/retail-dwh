-- Отчёты считают дни и месяцы по времени Алматы, а не по UTC: иначе
-- ночные покупки 1-го числа уезжают в прошлый месяц.
ALTER DATABASE dwh SET timezone TO 'Asia/Almaty';

-- Запросы вида «продажи товара за период» (промо-эффект, окно назад)
-- шли по индексу product_sk через всю историю товара и фильтровали дату
-- уже потом. Составной индекс даёт диапазонный поиск, а INCLUDE —
-- index-only scan без обращения к таблице. Замер в docs/performance.md.
DROP INDEX marts.fact_sales_product_ix;
CREATE INDEX fact_sales_product_ts_ix ON marts.fact_sales (product_sk, order_ts)
    INCLUDE (qty, order_status, promo_campaign_id);
