-- BRONZE delivery status — collapsed to the CURRENT status per order (CTAS).
-- Events for an order_id share the key so they arrive ordered; upsert keeps the
-- last (DELIVERED once complete, with event_ts = the delivery time).
CREATE TABLE bronze_delivery_current (
  PRIMARY KEY (order_id) NOT ENFORCED
) DISTRIBUTED BY (order_id) INTO 6 BUCKETS AS
SELECT order_id, status, event_ts
FROM dswt_delivery_status;
