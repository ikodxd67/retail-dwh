-- Магазины: SCD1, берём последнюю версию сателлита.
INSERT INTO marts.dim_store AS d
    (hk_store, store_code, name, city, format, area_sqm, opened_on, closed_on, updated_at)
SELECT h.hk_store, h.store_code, s.name, s.city, s.format, s.area_sqm, s.opened_on, s.closed_on, now()
FROM raw_vault.hub_store h
JOIN LATERAL (
    SELECT * FROM raw_vault.sat_store_erp t
    WHERE t.hk_store = h.hk_store
    ORDER BY t.load_dts DESC LIMIT 1
) s ON true
WHERE s.load_dts > %(since)s
ON CONFLICT (hk_store) DO UPDATE SET
    name = excluded.name, city = excluded.city, format = excluded.format,
    area_sqm = excluded.area_sqm, opened_on = excluded.opened_on,
    closed_on = excluded.closed_on, updated_at = now();
