-- SILVER order fulfillment — the enriched data product (CTAS). Joins the bronze
-- facts + the customer dimension, COMPUTES lateness from the timestamps (the
-- delivery stream never says whether it was late), and flags the
-- shipped-despite-declined-payment revenue leak. Needs the four bronze_* + the
-- raw dswt_customers to exist first.
CREATE TABLE medal_silver_order_fulfillment (
  PRIMARY KEY (order_id) NOT ENFORCED
) DISTRIBUTED BY (order_id) INTO 6 BUCKETS AS
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
  CASE WHEN d.status = 'DELIVERED'
       THEN TIMESTAMPDIFF(DAY, s.shipped_ts, d.event_ts) END AS actual_days,
  CASE WHEN d.status = 'DELIVERED'
       THEN TIMESTAMPDIFF(DAY, s.shipped_ts, d.event_ts) > s.promised_days
       END                                    AS is_late,
  -- the revenue leak: payment failed but we shipped/delivered anyway
  (p.status = 'DECLINED' AND d.status = 'DELIVERED') AS shipped_despite_decline
FROM medal_bronze_orders o
LEFT JOIN medal_bronze_payments         p ON o.order_id = p.order_id
LEFT JOIN medal_bronze_shipments        s ON o.order_id = s.order_id
LEFT JOIN medal_bronze_delivery_current d ON o.order_id = d.order_id
LEFT JOIN dswt_customers          c ON o.customer_id = c.customer_id;
