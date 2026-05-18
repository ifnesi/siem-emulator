#!/usr/bin/env bash
#
# Run all five SIEM templates concurrently against Kafka.
# Each template produces to a topic named after the template (e.g. dns_log -> dns_log).
# Press Ctrl+C to stop all producers.
#
set -euo pipefail

FREQUENCY=0.1
NUM_RECORDS=0
BATCH_SIZE=250
KAFKA_CONFIG=./kafka/config.properties
REGISTRY_CONFIG=./kafka/registry.properties
TEMPLATES_DIR=./templates
NAMESPACE=io.confluent.siem

# Per-template Kafka message key field. Must align 1-to-1 with TEMPLATES.
# Empty string ("") means no key — message produced with a null key.
KEYS=(     "src_ip" "device_name" "capture_interface" "event_id" "hostname")
TEMPLATES=(dns_log  net_device    pcap_data           siem_log   syslog_log)
PIDS=()

usage() {
    cat <<EOF
Usage: $0 [options]

Runs the following ${#TEMPLATES[@]} SIEM templates concurrently against Kafka:
  ${TEMPLATES[*]}

Options:
  -f, --frequency SEC       Seconds between records (default: $FREQUENCY)
  -n, --num-records N       Total records per template, 0 = continuous (default: $NUM_RECORDS)
  -b, --batch-size N        Records per batch (default: $BATCH_SIZE)
      --kafka-config FILE   Kafka config file (default: $KAFKA_CONFIG)
      --registry-config FILE  Schema Registry config (default: $REGISTRY_CONFIG)
      --templates-dir DIR   Templates directory (default: $TEMPLATES_DIR)
      --namespace NS        Avro schema namespace (default: $NAMESPACE)
  -h, --help                Show this help and exit
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        -f|--frequency)        FREQUENCY="$2";        shift 2 ;;
        -n|--num-records)      NUM_RECORDS="$2";      shift 2 ;;
        -b|--batch-size)       BATCH_SIZE="$2";       shift 2 ;;
        --kafka-config)        KAFKA_CONFIG="$2";     shift 2 ;;
        --registry-config)     REGISTRY_CONFIG="$2";  shift 2 ;;
        --templates-dir)       TEMPLATES_DIR="$2";    shift 2 ;;
        --namespace)           NAMESPACE="$2";        shift 2 ;;
        -h|--help)             usage; exit 0 ;;
        *) echo "Unknown option: $1" >&2; usage; exit 2 ;;
    esac
done

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

if [[ ! -f .venv/bin/activate ]]; then
    echo "Error: .venv not found at $SCRIPT_DIR/.venv" >&2
    echo "Create it with: python3 -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt" >&2
    exit 1
fi
# shellcheck disable=SC1091
source .venv/bin/activate

if [[ ${#KEYS[@]} -ne ${#TEMPLATES[@]} ]]; then
    echo "Error: KEYS (${#KEYS[@]}) and TEMPLATES (${#TEMPLATES[@]}) must have the same length" >&2
    exit 1
fi

cleanup() {
    trap '' INT TERM  # ignore further Ctrl+C so cleanup runs to completion
    echo
    echo "Stopping all producers (SIGINT for clean flush)..."
    # SIGINT triggers Python's KeyboardInterrupt handler (graceful flush).
    # Match by name in addition to tracked PIDs — process substitution and
    # librdkafka background threads can leave orphans the script doesn't see.
    pkill -INT -f "siem_producer.py" 2>/dev/null || true
    for pid in "${PIDS[@]}"; do
        kill -INT "$pid" 2>/dev/null || true
    done
    # Give producers a chance to flush; escalate to SIGKILL if they hang
    # (librdkafka can block in C code that ignores Python signals).
    for _ in 1 2 3 4 5 6 7 8 9 10; do
        sleep 1
        pgrep -f "siem_producer.py" >/dev/null || break
    done
    if pgrep -f "siem_producer.py" >/dev/null; then
        echo "Some producers didn't exit on SIGINT — sending SIGKILL"
        pkill -KILL -f "siem_producer.py" 2>/dev/null || true
    fi
    wait 2>/dev/null || true
    echo "All producers stopped."
    exit 0
}
trap cleanup INT TERM

echo "Starting ${#TEMPLATES[@]} producers (frequency=${FREQUENCY}s, batch=${BATCH_SIZE}, num_records=${NUM_RECORDS})"
echo

for i in "${!TEMPLATES[@]}"; do
    tpl="${TEMPLATES[$i]}"
    key="${KEYS[$i]}"
    key_args=()
    [[ -n "$key" ]] && key_args=(-k "$key")
    # Process substitution so $! captures python's PID directly (not a
    # subshell wrapper), making cleanup able to signal it.
    python siem_producer.py "$tpl" \
        -t "$tpl" \
        ${key_args[@]+"${key_args[@]}"} \
        -f "$FREQUENCY" \
        -n "$NUM_RECORDS" \
        -b "$BATCH_SIZE" \
        --kafka-config "$KAFKA_CONFIG" \
        --registry-config "$REGISTRY_CONFIG" \
        --templates-dir "$TEMPLATES_DIR" \
        --namespace "$NAMESPACE" \
        --schema "./schemas/${tpl}.avsc" \
        > >(while IFS= read -r line; do printf '[%s] %s\n' "$tpl" "$line"; done) \
        2>&1 &
    echo ""
    sleep 2
    PIDS+=($!)
done

wait
