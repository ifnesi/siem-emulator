-- =============================================================================
-- DSWT London 2026 — ANOMALY / AT-RISK detector
-- Streams the orders that need attention into an alerts topic: declined
-- payments, late deliveries, or an order_total that does not match the captured
-- payment amount (a data-quality / fraud signal). Run after silver.sql.
--     USE CATALOG `<env display name>`;  USE `<cluster display name>`;
-- =============================================================================

CREATE TABLE alerts_at_risk (
  order_id        STRING,
  customer_id     STRING,
  reason          STRING,
  payment_status  STRING,
  delivery_status STRING,
  is_late         BOOLEAN,
  amount_matches  BOOLEAN,
  order_total     DOUBLE,
  PRIMARY KEY (order_id) NOT ENFORCED
) DISTRIBUTED BY (order_id) INTO 6 BUCKETS;

INSERT INTO alerts_at_risk
SELECT
  order_id,
  customer_id,
  CASE
    WHEN payment_status = 'DECLINED'      THEN 'PAYMENT_DECLINED'
    WHEN NOT amount_matches               THEN 'AMOUNT_MISMATCH'
    WHEN is_late                          THEN 'LATE_DELIVERY'
    ELSE 'OTHER'
  END                                     AS reason,
  payment_status,
  delivery_status,
  is_late,
  amount_matches,
  order_total
FROM silver_order_fulfillment
WHERE payment_status = 'DECLINED'
   OR is_late = TRUE
   OR amount_matches = FALSE;
