-- BRONZE payments — cleaned/deduped (CTAS). authorized_ts is now a real
-- timestamp on the shared per-order clock (see templates/dswt_orders.j2).
CREATE TABLE medal_bronze_payments (
  PRIMARY KEY (order_id) NOT ENFORCED
) DISTRIBUTED BY (order_id) INTO 6 BUCKETS AS
SELECT order_id, payment_method, amount, currency, UPPER(status) AS status, authorized_ts
FROM dswt_payments;
