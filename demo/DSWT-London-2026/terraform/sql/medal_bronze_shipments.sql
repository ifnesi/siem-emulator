-- BRONZE shipments — the promise (CTAS). promised_days is Avro long -> CAST INT.
CREATE TABLE medal_bronze_shipments (
  PRIMARY KEY (order_id) NOT ENFORCED
) DISTRIBUTED BY (order_id) INTO 6 BUCKETS AS
SELECT
  order_id,
  carrier,
  tracking_number,
  CAST(promised_days AS INT) AS promised_days,
  shipped_ts
FROM dswt_shipments;
