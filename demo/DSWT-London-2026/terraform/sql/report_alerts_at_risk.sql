-- ANOMALY / at-risk detector (CTAS). Needs medal_silver_order_fulfillment first.
CREATE TABLE report_alerts_at_risk (
  PRIMARY KEY (order_id) NOT ENFORCED
) DISTRIBUTED BY (order_id) INTO 6 BUCKETS AS
SELECT
  order_id,
  customer_id,
  CASE
    WHEN delivered_despite_decline   THEN 'DELIVERED_DESPITE_DECLINE'
    WHEN payment_status = 'DECLINED' THEN 'PAYMENT_DECLINED'
    WHEN is_late                     THEN 'LATE_DELIVERY'
    ELSE 'OTHER'
  END AS reason,
  payment_status,
  delivery_status,
  is_late,
  delivered_despite_decline,
  order_total
FROM medal_silver_order_fulfillment
WHERE payment_status = 'DECLINED'
   OR is_late = TRUE;
