-- =============================================================================
-- DSWT London 2026 — EXECUTIVE SUMMARY aggregates
-- Pre-aggregates the numbers behind Claude's natural-language weekly summary:
-- hourly revenue / order volume / average order value by channel, using the
-- Kafka record time ($rowtime) as event time. Claude can also just consume
-- silver_order_fulfillment over MCP and summarize in prose — this table makes
-- the headline numbers exact.
--     USE CATALOG `<env display name>`;  USE `<cluster display name>`;
-- =============================================================================

CREATE TABLE exec_summary_hourly (
  window_start    TIMESTAMP_LTZ(3),
  window_end      TIMESTAMP_LTZ(3),
  channel         STRING,
  orders          BIGINT,
  revenue         DOUBLE,
  avg_order_value DOUBLE,
  PRIMARY KEY (window_start, window_end, channel) NOT ENFORCED
) DISTRIBUTED BY (channel) INTO 3 BUCKETS;

INSERT INTO exec_summary_hourly
SELECT
  window_start,
  window_end,
  channel,
  COUNT(*)              AS orders,
  SUM(order_total)      AS revenue,
  AVG(order_total)      AS avg_order_value
FROM TABLE(
  TUMBLE(TABLE dswt_orders, DESCRIPTOR($rowtime), INTERVAL '1' HOUR)
)
GROUP BY window_start, window_end, channel;

-- Ad-hoc queries Claude / the presenter can run live for the summary narrative:
--
--   -- Headline last-hour numbers:
--   SELECT window_start, SUM(orders) AS orders, SUM(revenue) AS revenue
--   FROM exec_summary_hourly
--   GROUP BY window_start ORDER BY window_start DESC LIMIT 1;
--
--   -- At-risk rate (needs anomalies.sql + silver.sql running):
--   SELECT
--     (SELECT COUNT(*) FROM alerts_at_risk) AS at_risk,
--     (SELECT COUNT(*) FROM silver_order_fulfillment) AS total;
