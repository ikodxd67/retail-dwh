-- Клиенты: SCD2. Версия измерения = версия сателлита sat_customer_crm.
-- valid_from берём из applied_ts (когда изменилось в CRM), а не из load_dts
-- (когда мы это увидели): отчёт «продажи по уровню лояльности» должен
-- относить заказ к уровню, который у клиента был на момент покупки.

CREATE TEMP TABLE _cust_new ON COMMIT DROP AS
SELECT s.hk_customer, h.customer_id, s.applied_ts, s.city, s.loyalty_tier,
       s.marketing_consent, s.gender, s.birth_year, s.email_domain, s.created_at,
       s.deleted_at IS NOT NULL AS is_deleted,
       NOT EXISTS (
           SELECT 1 FROM marts.dim_customer d WHERE d.hk_customer = s.hk_customer
       ) AS is_new_customer,
       row_number() OVER (PARTITION BY s.hk_customer ORDER BY s.applied_ts, s.load_dts) AS rn,
       lead(s.applied_ts) OVER (PARTITION BY s.hk_customer ORDER BY s.applied_ts, s.load_dts)
           AS next_from
FROM raw_vault.sat_customer_crm s
JOIN raw_vault.hub_customer h USING (hk_customer)
WHERE s.load_dts > %(since)s;

-- закрываем текущие версии тех, у кого пришло изменение
UPDATE marts.dim_customer d
SET valid_to = n.applied_ts, is_current = false
FROM _cust_new n
WHERE n.rn = 1 AND NOT n.is_new_customer
  AND d.hk_customer = n.hk_customer AND d.is_current;

INSERT INTO marts.dim_customer
    (hk_customer, customer_id, city, loyalty_tier, marketing_consent, gender, birth_year,
     email_domain, registered_at, is_deleted, valid_from, valid_to, is_current)
SELECT hk_customer, customer_id, city, loyalty_tier, marketing_consent, gender, birth_year,
       email_domain, created_at, is_deleted,
       -- первая версия действует «с начала времён», иначе заказы, сделанные до
       -- первой загрузки CRM, не нашли бы клиента
       CASE WHEN is_new_customer AND rn = 1 THEN '-infinity'::timestamptz ELSE applied_ts END,
       coalesce(next_from, 'infinity'),
       next_from IS NULL
FROM _cust_new
ON CONFLICT (hk_customer, valid_from) DO NOTHING;

-- Заказы, уже лежащие в факте, переводим на правильную версию клиента.
-- Сюда же попадает «опоздавшее измерение»: заказ пришёл из ERP раньше, чем
-- клиент из CRM, и лёг с ключом -2 — теперь он получит настоящий.
UPDATE marts.fact_sales f
SET customer_sk = d.customer_sk, updated_at = now()
FROM marts.dim_customer d
WHERE d.hk_customer IN (SELECT DISTINCT hk_customer FROM _cust_new)
  AND f.hk_customer = d.hk_customer
  AND f.order_ts >= d.valid_from AND f.order_ts < d.valid_to
  AND f.customer_sk IS DISTINCT FROM d.customer_sk;
