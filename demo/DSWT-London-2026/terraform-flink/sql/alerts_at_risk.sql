-- ANOMALY / at-risk detector (CTAS). Needs silver_order_fulfillment first.
CREATE TABLE alerts_at_risk (
  PRIMARY KEY (order_id) NOT ENFORCED
) DISTRIBUTED BY (order_id) INTO 6 BUCKETS AS
SELECT
  order_id,
  customer_id,
  CASE
    WHEN payment_status = 'DECLINED' THEN 'PAYMENT_DECLINED'
    WHEN NOT amount_matches          THEN 'AMOUNT_MISMATCH'
    WHEN is_late                     THEN 'LATE_DELIVERY'
    ELSE 'OTHER'
  END AS reason,
  payment_status,
  delivery_status,
  is_late,
  amount_matches,
  order_total
FROM silver_order_fulfillment
WHERE payment_status = 'DECLINED'
   OR is_late = TRUE
   OR amount_matches = FALSE;
