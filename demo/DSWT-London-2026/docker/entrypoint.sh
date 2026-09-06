#!/usr/bin/env bash
# =============================================================================
# DSWT London 2026 — datagen container entrypoint.
# Real-time flow with bounded, referentially-safe keyspaces:
#   1. Bulk-loads the dimensions FIRST (1000 customers, 100 products) fast, so
#      every order's customer/product already exists.
#   2. Streams the five fact producers at RATE-MATCHED speeds so they progress
#      through the same order_id keyspace together (order_items ~3.5x, delivery
#      4x the order rate). Each is bounded with -n and stops at its limit.
#
# Sizing: ORDERS_PER_SEC (default 5) x RUN_HOURS (default 24) => NUM_ORDERS, so
# the data flows continuously for ~RUN_HOURS then stops. Every producer walks
# the closed keyspace [0, NUM_ORDERS-1]; children only reference ids inside it,
# so nothing is orphaned. Fan-out streams get exact counts:
#   order_items      -> sum of item_count over the orders (deterministic)
#   delivery_status  -> 4 * NUM_ORDERS  (4-step lifecycle per order)
# =============================================================================
set -euo pipefail

: "${CC_BOOTSTRAP:?set CC_BOOTSTRAP (host:9092, no scheme)}"
: "${CC_KAFKA_KEY:?set CC_KAFKA_KEY}"
: "${CC_KAFKA_SECRET:?set CC_KAFKA_SECRET}"
: "${CC_SR_URL:?set CC_SR_URL}"
: "${CC_SR_KEY:?set CC_SR_KEY}"
: "${CC_SR_SECRET:?set CC_SR_SECRET}"

# Tuning knobs (overridable via env / compose).
: "${DIM_PARTITIONS:=1}"
: "${FACT_PARTITIONS:=6}"
: "${REPLICATION:=3}"          # Confluent Cloud minimum RF is 3
: "${ORDERS_PER_SEC:=5}"       # order throughput; other streams scale off this
: "${RUN_HOURS:=24}"           # how long the flow should last before stopping
# NUM_ORDERS defaults to ORDERS_PER_SEC * RUN_HOURS * 3600; override to pin it.
: "${NUM_ORDERS:=$(( ORDERS_PER_SEC * RUN_HOURS * 3600 ))}"

# Fixed dimension keyspaces (must match the templates' seeded ranges:
# customer_id in [0, NUM_CUSTOMERS-1], product_id in [0, NUM_PRODUCTS-1]).
NUM_CUSTOMERS=1000
NUM_PRODUCTS=100

CONF="/app/conf"
mkdir -p "${CONF}"
KAFKA_CONFIG="${CONF}/kafka.properties"
REGISTRY_CONFIG="${CONF}/registry.properties"

cat > "${KAFKA_CONFIG}" <<EOF
bootstrap.servers=${CC_BOOTSTRAP}
security.protocol=SASL_SSL
sasl.mechanisms=PLAIN
sasl.username=${CC_KAFKA_KEY}
sasl.password=${CC_KAFKA_SECRET}
EOF

cat > "${REGISTRY_CONFIG}" <<EOF
schemaRegistryURL=${CC_SR_URL}
basic.auth.user.info=${CC_SR_KEY}:${CC_SR_SECRET}
auto.register.schemas=true
EOF

log() { echo "[$(date +%H:%M:%S)] $*"; }

# Exact order_items count (reuse the SAME seeded helper the template uses) and
# the rate-matched batch so order_items keeps pace with orders.
ITEMS_INFO="$(python - "${NUM_ORDERS}" "${ORDERS_PER_SEC}" <<'PYEOF'
import sys
from siem_producer import TemplateRenderer
n, ops = int(sys.argv[1]), int(sys.argv[2])
r = TemplateRenderer()
items = sum(r._seeded_integer("ic-%d" % i, 2, 5) for i in range(n))
# Floor (not round) so order_items lags orders slightly rather than leading —
# every item then references an order that already exists (no leading orphans).
rate = max(1, int(items * ops / n))     # ~3.5 * ops
print(items, rate)
PYEOF
)"
ITEMS_N="${ITEMS_INFO%% *}"
ITEMS_RATE="${ITEMS_INFO##* }"
DS_N=$(( NUM_ORDERS * 4 ))
DS_RATE=$(( ORDERS_PER_SEC * 4 ))
log "Bounded run for ~${RUN_HOURS}h @ ${ORDERS_PER_SEC} orders/s: ${NUM_ORDERS} orders, ${ITEMS_N} items, ${DS_N} delivery events."

common=(--kafka-config "${KAFKA_CONFIG}" --registry-config "${REGISTRY_CONFIG}" -rf "${REPLICATION}")

# ── Dimensions first: bulk-load fast (-f 0, big batches) BEFORE any facts ─────
log "Bulk-loading ${NUM_CUSTOMERS} customers -> dswt_customers ..."
python siem_producer.py dswt_customers -t dswt_customers \
       -n "${NUM_CUSTOMERS}" -f 0 -b 1000 -p "${DIM_PARTITIONS}" -k customer_id \
       --schema schemas/dswt_customers.avsc "${common[@]}"

log "Bulk-loading ${NUM_PRODUCTS} products -> dswt_products ..."
python siem_producer.py dswt_products -t dswt_products \
       -n "${NUM_PRODUCTS}" -f 0 -b 100 -p "${DIM_PARTITIONS}" -k product_id \
       --schema schemas/dswt_products.avsc "${common[@]}"

# ── Facts: rate-matched streams (-f 1, batch = records/sec) ──────────────────
pids=()
cleanup() { log "Shutting down fact producers..."; kill "${pids[@]}" 2>/dev/null || true; wait 2>/dev/null || true; }
trap cleanup TERM INT

start_fact() {
  local template="$1" topic="$2" nrecords="$3" batch="$4"
  log "Streaming ${template} -> ${topic} (-n ${nrecords}, ~${batch}/s)"
  python siem_producer.py "${template}" -t "${topic}" \
         -n "${nrecords}" -f 1 -b "${batch}" \
         -p "${FACT_PARTITIONS}" -k order_id \
         --schema "schemas/${template}.avsc" "${common[@]}" &
  pids+=("$!")
  sleep 1
}

start_fact dswt_orders          dswt_orders          "${NUM_ORDERS}" "${ORDERS_PER_SEC}"
start_fact dswt_order_items     dswt_order_items     "${ITEMS_N}"    "${ITEMS_RATE}"
start_fact dswt_shipments       dswt_shipments       "${NUM_ORDERS}" "${ORDERS_PER_SEC}"
start_fact dswt_delivery_status dswt_delivery_status "${DS_N}"       "${DS_RATE}"
start_fact dswt_payments        dswt_payments        "${NUM_ORDERS}" "${ORDERS_PER_SEC}"

log "All five fact streams flowing (${#pids[@]} processes) for ~${RUN_HOURS}h."
wait
log "Done — bounded run complete."
