-- EXECUTIVE SUMMARY — hourly revenue/volume/AOV by channel (CTAS). Reads the
-- raw dswt_orders directly (uses the Kafka record time $rowtime as event time),
-- so it has no bronze dependency. Windowed aggregate = append, so no PRIMARY KEY.
CREATE TABLE exec_summary_hourly AS
SELECT
  window_start,
  window_end,
  channel,
  COUNT(*)         AS orders,
  SUM(order_total) AS revenue,
  AVG(order_total) AS avg_order_value
FROM TABLE(
  TUMBLE(TABLE dswt_orders, DESCRIPTOR($rowtime), INTERVAL '1' HOUR)
)
GROUP BY window_start, window_end, channel;
