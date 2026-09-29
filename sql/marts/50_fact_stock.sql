-- Остатки: периодический снимок, неисторизуемый линк переносится как есть.
INSERT INTO marts.fact_stock_daily AS f (date_key, store_sk, product_sk, qty_on_hand, stock_cost)
SELECT to_char(n.snapshot_date, 'YYYYMMDD')::int,
       coalesce(ds.store_sk, -1),
       coalesce(dp.product_sk, -1),
       n.qty_on_hand,
       n.stock_cost
FROM raw_vault.nhl_stock_snapshot n
LEFT JOIN marts.dim_store ds ON ds.hk_store = n.hk_store
LEFT JOIN marts.dim_product dp ON dp.hk_product = n.hk_product
WHERE n.load_dts > %(since)s
ON CONFLICT (date_key, store_sk, product_sk) DO UPDATE SET
    qty_on_hand = excluded.qty_on_hand, stock_cost = excluded.stock_cost;
