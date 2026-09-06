-- BRONZE orders — cleaned/deduped (CTAS = one statement). Catalog/database come
-- from the statement's sql.current-catalog/database properties (set in main.tf).
CREATE TABLE bronze_orders (
  PRIMARY KEY (order_id) NOT ENFORCED
) DISTRIBUTED BY (order_id) INTO 6 BUCKETS AS
SELECT
  order_id,
  customer_id,
  UPPER(channel) AS channel,
  currency,
  order_total,
  UPPER(status)  AS status,
  order_ts
FROM dswt_orders;
