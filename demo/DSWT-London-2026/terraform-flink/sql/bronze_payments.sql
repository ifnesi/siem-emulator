-- BRONZE payments — cleaned/deduped (CTAS).
CREATE TABLE bronze_payments (
  PRIMARY KEY (order_id) NOT ENFORCED
) DISTRIBUTED BY (order_id) INTO 6 BUCKETS AS
SELECT order_id, payment_method, amount, currency, UPPER(status) AS status
FROM dswt_payments;
