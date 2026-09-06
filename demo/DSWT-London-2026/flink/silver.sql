-- =============================================================================
-- DSWT London 2026 — SILVER layer
-- The "new data product": one enriched, query-ready row per order that stitches
-- orders + payments + shipments + delivery status together and enriches with
-- the customer dimension. Crucially, LATENESS IS COMPUTED HERE (the analytics
-- layer) from the shipment promise and the delivery event timestamps — the
-- stateless delivery-status service never reports actual_days or is_late.
-- This is the topic Claude re-reads over MCP to prove the product is live.
-- Run after bronze.sql, same catalog/database context:
--     USE CATALOG `<env display name>`;  USE `<cluster display name>`;
-- =============================================================================

CREATE TABLE silver_order_fulfillment (
  order_id          STRING,
  customer_id       STRING,
  customer_segment  STRING,
  customer_country  STRING,
  loyalty_tier      STRING,
  channel           STRING,
  order_total       DOUBLE,
  currency          STRING,
  payment_method    STRING,
  payment_status    STRING,
  amount_matches    BOOLEAN,   -- payment.amount == order.order_total (data-quality signal)
  carrier           STRING,
  delivery_status   STRING,    -- current status (DELIVERED once complete)
  promised_days     INT,
  actual_days       INT,       -- derived: DELIVERED event_ts - shipped_ts
  is_late           BOOLEAN,   -- derived: actual_days > promised_days
  PRIMARY KEY (order_id) NOT ENFORCED
) DISTRIBUTED BY (order_id) INTO 6 BUCKETS;

INSERT INTO silver_order_fulfillment
SELECT
  o.order_id,
  o.customer_id,
  c.segment                                   AS customer_segment,
  c.country                                   AS customer_country,
  c.loyalty_tier,
  o.channel,
  o.order_total,
  o.currency,
  p.payment_method,
  p.status                                    AS payment_status,
  (p.amount = o.order_total)                  AS amount_matches,
  s.carrier,
  d.status                                    AS delivery_status,
  s.promised_days,
  -- Only meaningful once delivered; NULL while still in transit.
  CASE WHEN d.status = 'DELIVERED'
       THEN TIMESTAMPDIFF(DAY, s.shipped_ts, d.event_ts) END AS actual_days,
  CASE WHEN d.status = 'DELIVERED'
       THEN TIMESTAMPDIFF(DAY, s.shipped_ts, d.event_ts) > s.promised_days
       END                                    AS is_late
FROM bronze_orders o
LEFT JOIN bronze_payments         p ON o.order_id = p.order_id
LEFT JOIN bronze_shipments        s ON o.order_id = s.order_id
LEFT JOIN bronze_delivery_current d ON o.order_id = d.order_id
LEFT JOIN dswt_customers          c ON o.customer_id = c.customer_id;
