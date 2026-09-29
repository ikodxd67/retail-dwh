-- Товары: сначала иерархия снежинки, потом сами товары (SCD1).

CREATE TEMP TABLE _product_last ON COMMIT DROP AS
SELECT h.hk_product, h.sku, c.name, c.brand, c.unit, c.status, c.retail_price,
       c.group_name, c.category_name, c.subcategory_name,
       sup.supplier_code, sup.purchase_currency,
       greatest(c.load_dts, sup.load_dts) AS load_dts
FROM raw_vault.hub_product h
LEFT JOIN LATERAL (
    SELECT * FROM raw_vault.sat_product_catalog t
    WHERE t.hk_product = h.hk_product ORDER BY t.load_dts DESC LIMIT 1
) c ON true
LEFT JOIN LATERAL (
    SELECT * FROM raw_vault.sat_product_supplier t
    WHERE t.hk_product = h.hk_product ORDER BY t.load_dts DESC LIMIT 1
) sup ON true
-- товары, пришедшие только из заказов (нет в каталоге), тоже нужны измерению
WHERE coalesce(greatest(c.load_dts, sup.load_dts), h.load_dts) > %(since)s;

INSERT INTO marts.dim_product_group (group_name)
SELECT DISTINCT group_name FROM _product_last WHERE group_name IS NOT NULL
ON CONFLICT (group_name) DO NOTHING;

INSERT INTO marts.dim_product_category (group_sk, category_name)
SELECT DISTINCT g.group_sk, p.category_name
FROM _product_last p JOIN marts.dim_product_group g USING (group_name)
WHERE p.category_name IS NOT NULL
ON CONFLICT (group_sk, category_name) DO NOTHING;

INSERT INTO marts.dim_product_subcategory (category_sk, subcategory_name)
SELECT DISTINCT c.category_sk, p.subcategory_name
FROM _product_last p
JOIN marts.dim_product_group g USING (group_name)
JOIN marts.dim_product_category c ON c.group_sk = g.group_sk AND c.category_name = p.category_name
WHERE p.subcategory_name IS NOT NULL
ON CONFLICT (category_sk, subcategory_name) DO NOTHING;

INSERT INTO marts.dim_product AS d
    (hk_product, sku, name, brand, unit, status, subcategory_sk, supplier_code,
     purchase_currency, retail_price, in_catalog, updated_at)
SELECT p.hk_product, p.sku, coalesce(p.name, '(нет в каталоге)'), p.brand, p.unit, p.status,
       coalesce(s.subcategory_sk, -1), p.supplier_code, p.purchase_currency, p.retail_price,
       p.name IS NOT NULL, now()
FROM _product_last p
LEFT JOIN marts.dim_product_group g USING (group_name)
LEFT JOIN marts.dim_product_category c
       ON c.group_sk = g.group_sk AND c.category_name = p.category_name
LEFT JOIN marts.dim_product_subcategory s
       ON s.category_sk = c.category_sk AND s.subcategory_name = p.subcategory_name
ON CONFLICT (hk_product) DO UPDATE SET
    name = excluded.name, brand = excluded.brand, unit = excluded.unit, status = excluded.status,
    subcategory_sk = excluded.subcategory_sk, supplier_code = excluded.supplier_code,
    purchase_currency = excluded.purchase_currency, retail_price = excluded.retail_price,
    in_catalog = excluded.in_catalog, updated_at = now();
