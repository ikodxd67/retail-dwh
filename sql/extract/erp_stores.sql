-- Справочник магазинов маленький, забираем целиком.
SELECT store_id, store_code, name, city, format, area_sqm, opened_on, closed_on, updated_at
FROM stores
