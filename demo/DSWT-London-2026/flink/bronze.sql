-- =============================================================================
-- DSWT London 2026 — BRONZE layer
-- Cleaned, typed, de-duplicated versions of the raw topics.
--
-- On Confluent Cloud Flink each Kafka topic is already a table in the current
-- catalog (environment) + database (cluster), so the raw `dswt_*` topics are
-- queryable directly. Set your context first in the SQL workspace / shell:
--     USE CATALOG `<env display name>`;
--     USE `<cluster display name>`;
-- These statements are what Claude SUGGESTS after reading the streams over MCP;
-- they are kept here pre-tested as the fallback the presenter applies.
-- =============================================================================

-- Orders: dedupe on order_id (upsert = last write wins), normalize enums.
CREATE TABLE bronze_orders (
  order_id     STRING,
  customer_id  STRING,
  channel      STRING,
  currency     STRING,
  order_total  DOUBLE,
  status       STRING,
  order_ts     STRING,
  PRIMARY KEY (order_id) NOT ENFORCED
) DISTRIBUTED BY (order_id) INTO 6 BUCKETS;

INSERT INTO bronze_orders
SELECT
  order_id,
  customer_id,
  UPPER(channel)        AS channel,
  currency,
  order_total,
  UPPER(status)         AS status,
  order_ts
FROM dswt_orders;

-- Payments: dedupe on order_id, normalize status.
CREATE TABLE bronze_payments (
  order_id        STRING,
  payment_method  STRING,
  amount          DOUBLE,
  currency        STRING,
  status          STRING,
  PRIMARY KEY (order_id) NOT ENFORCED
) DISTRIBUTED BY (order_id) INTO 6 BUCKETS;

INSERT INTO bronze_payments
SELECT order_id, payment_method, amount, currency, UPPER(status) AS status
FROM dswt_payments;

-- Shipments: the promise. Dedupe on order_id (one shipment per order).
CREATE TABLE bronze_shipments (
  order_id        STRING,
  carrier         STRING,
  tracking_number STRING,
  promised_days   INT,
  shipped_ts      TIMESTAMP(3),
  PRIMARY KEY (order_id) NOT ENFORCED
) DISTRIBUTED BY (order_id) INTO 6 BUCKETS;

INSERT INTO bronze_shipments
-- promised_days is Avro long (the emulator infers ints as long) → CAST to INT.
SELECT order_id, carrier, tracking_number, CAST(promised_days AS INT), shipped_ts
FROM dswt_shipments;

-- Delivery status: the raw event stream carries CREATED/IN_TRANSIT/
-- OUT_FOR_DELIVERY/DELIVERED per order (no lateness — that's computed in
-- silver). Collapse the changelog to the CURRENT status per order: events for
-- an order_id share the key so they arrive ordered, and upsert keeps the last,
-- i.e. once delivered, status='DELIVERED' and event_ts = the delivery time.
CREATE TABLE bronze_delivery_current (
  order_id  STRING,
  status    STRING,
  event_ts  TIMESTAMP(3),
  PRIMARY KEY (order_id) NOT ENFORCED
) DISTRIBUTED BY (order_id) INTO 6 BUCKETS;

INSERT INTO bronze_delivery_current
SELECT order_id, status, event_ts
FROM dswt_delivery_status;
