#!/usr/bin/env bash
# =============================================================================
# Starts all SIEM producers + streaming apps in one container, in the same
# order as the manual commands in the repo README, staggered 2s apart. Every
# one of these processes loops forever (continuous producer / continuous
# consumer) so "in order, 2s apart" only affects *start* order — after
# startup they all run concurrently for as long as the container is alive.
#
# Kafka/Schema Registry are also verified reachable before anything starts
# (belt-and-suspenders on top of the `depends_on: condition: service_healthy`
# gate in docker-compose.yml).
# =============================================================================
set -euo pipefail

: "${KAFKA_BOOTSTRAP:=broker:29092}"
: "${SCHEMA_REGISTRY_URL:=http://schema-registry:8081}"
: "${START_DELAY:=2}"

CONFIG_DIR="/app/kafka-docker"
mkdir -p "${CONFIG_DIR}"
KAFKA_CONFIG="${CONFIG_DIR}/config.properties"
REGISTRY_CONFIG="${CONFIG_DIR}/registry.properties"

# The committed kafka/config.properties + kafka/registry.properties point at
# localhost (for running the scripts straight from your Mac) — write
# container-appropriate versions instead of relying on those.
cat > "${KAFKA_CONFIG}" <<EOF
bootstrap.servers=${KAFKA_BOOTSTRAP}
security.protocol=PLAINTEXT
EOF

cat > "${REGISTRY_CONFIG}" <<EOF
schemaRegistryURL=${SCHEMA_REGISTRY_URL}
auto.register.schemas=true
EOF

log() { echo "[$(date +%H:%M:%S)] $*"; }

wait_for_kafka() {
  log "Waiting for Kafka at ${KAFKA_BOOTSTRAP}..."
  python3 - "${KAFKA_BOOTSTRAP}" <<'PYEOF'
import socket, sys, time
host, port = sys.argv[1].split(":")
port = int(port)
for _ in range(60):
    try:
        with socket.create_connection((host, port), timeout=2):
            sys.exit(0)
    except OSError:
        time.sleep(2)
sys.exit(1)
PYEOF
}

wait_for_schema_registry() {
  log "Waiting for Schema Registry at ${SCHEMA_REGISTRY_URL}..."
  python3 - "${SCHEMA_REGISTRY_URL}" <<'PYEOF'
import sys, time, urllib.request
url = sys.argv[1].rstrip("/") + "/subjects"
for _ in range(60):
    try:
        urllib.request.urlopen(url, timeout=2)
        sys.exit(0)
    except OSError:
        time.sleep(2)
sys.exit(1)
PYEOF
}

wait_for_kafka || { log "ERROR: Kafka never became reachable at ${KAFKA_BOOTSTRAP}"; exit 1; }
wait_for_schema_registry || { log "ERROR: Schema Registry never became reachable at ${SCHEMA_REGISTRY_URL}"; exit 1; }
log "Kafka + Schema Registry are up."

pids=()
cleanup() {
  log "Shutting down..."
  kill "${pids[@]}" 2>/dev/null || true
  wait 2>/dev/null || true
}
trap cleanup TERM INT

start() {
  log "Starting: $*"
  "$@" &
  pids+=("$!")
  sleep "${START_DELAY}"
}

# ── Producers (raw events) ───────────────────────────────────────────────────
start python siem_producer.py fortigate_log -t siem_poc_fortigate_logs -f 1 -b 10 -p 1 --no-schema --kafka-config "${KAFKA_CONFIG}"
start python siem_producer.py paloalto_log -t siem_poc_paloalto_logs -f 1 -b 10 -p 1 --no-schema --kafka-config "${KAFKA_CONFIG}"
start python siem_producer.py dns_log -t siem_poc_dns_logs -f 1 -b 10 -p 1 -k src_ip --kafka-config "${KAFKA_CONFIG}" --registry-config "${REGISTRY_CONFIG}"
start python siem_producer.py windows_event_log -t siem_poc_windows_eventlog_logs -f 1 -b 10 -p 1 -k Computer --kafka-config "${KAFKA_CONFIG}" --registry-config "${REGISTRY_CONFIG}"

# ── Stream processing (raw events -> parsed sub-topics) ─────────────────────
cd demo

start python fortigate_streaming_app.py --schema-dir ./schemas/ --source-topic siem_poc_fortigate_logs --kafka-config "${KAFKA_CONFIG}" --registry-config "${REGISTRY_CONFIG}"
start python paloalto_streaming_app.py --schema-dir ./schemas/ --source-topic siem_poc_paloalto_logs --kafka-config "${KAFKA_CONFIG}" --registry-config "${REGISTRY_CONFIG}"
start python dns_streaming_app.py --kafka-config "${KAFKA_CONFIG}" --registry-config "${REGISTRY_CONFIG}" --schema-dir ./schemas/ --source-topic siem_poc_dns_logs --window-seconds 300

log "All producers/streaming apps started (${#pids[@]} processes)."
wait
