SELECT customer_id, email, phone, first_name, last_name, birth_date, gender, city,
       loyalty_tier, marketing_consent, created_at, updated_at, deleted_at
FROM customers
WHERE updated_at > %(wm_from)s
  AND updated_at <= %(wm_to)s
