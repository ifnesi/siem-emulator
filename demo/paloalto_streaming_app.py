#!/usr/bin/env python3
"""Palo Alto (PAN-OS) streaming router.

Consumes raw PAN-OS syslog (positional CSV, UTF-8 strings) from a single source
Kafka topic, parses each event into JSON, and re-produces it as Avro to a
per-(log_type/subtype) destination topic.

  source:  siem_poc_paloalto_logs            (value = raw UTF-8 CSV string)
  dest:    siem_poc_paloalto_logs-<type>-<subtype>   (type lower-cased)
           value = Avro (schema in demo/schemas/paloalto_<type>_<subtype>.avsc)
           key   = devname (raw UTF-8 string), the PAN device hostname

PAN-OS logs are positional, so fields are extracted by index (see LAYOUTS).
All PAN-OS datetime fields ("YYYY/MM/DD HH:MM:SS") — including the TRAFFIC
session start_time and the receive/generated (end) times — are converted to
Unix epoch milliseconds (Avro logical type timestamp-millis).

Kafka + Schema Registry credentials are read from property files passed on the
command line. Endpoints may use self-signed TLS, so the CA bundle is taken from
the Kafka config's `ssl.ca.location`.
"""
import os
import sys
import argparse
from datetime import datetime, timezone

from confluent_kafka import Consumer, Producer, KafkaError
from confluent_kafka.admin import AdminClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import (
    MessageField,
    SerializationContext,
)

from utils import (
    AUTO_OFFSET_RESET,
    DEFAULT_RETENTION_MS,
    DEFAULT_SCHEMA_DIR,
    POLL_TIMEOUT,
    MAX_POLL_INTERVAL_MS,
    build_sr_client,
    ensure_topics,
    load_properties,
    setup_logging,
    graceful_shutdown,
    string_serializer,
)

# ── Tunables ─────────────────────────────────────────────────────────────────
SOURCE_TOPIC = "siem_poc_paloalto_logs"
TOPIC_PREFIX = "siem_poc_paloalto_logs"  # dest = PREFIX-<type>-<subtype>
CONSUMER_GROUP = "paloalto-streaming-app"
KEY_FIELD = "devname"  # message key, the PAN device hostname
PAN_TS_FMT = "%Y/%m/%d %H:%M:%S"  # PAN-OS datetime string format

# ── Positional field layouts ─────────────────────────────────────────────────
# Single source of truth for both parsing (index -> value) and Avro schema
# generation (see gen_paloalto_schemas.py). Each entry: (name, csv_index, kind)
# where kind is "string" | "long" | "ts" (ts = datetime string -> epoch millis).
#
# Common header shared by TRAFFIC and THREAT logs (CSV idx 0..30). idx 0 is the
# syslog "<time> 1" prefix and is intentionally skipped.
_PAN_COMMON = [
    ("receive_time", 1, "ts"),
    ("serial", 2, "string"),
    ("type", 3, "string"),
    ("subtype", 4, "string"),
    ("generated_time", 6, "ts"),
    ("src_ip", 7, "string"),
    ("dst_ip", 8, "string"),
    ("nat_src_ip", 9, "string"),
    ("nat_dst_ip", 10, "string"),
    ("rule", 11, "string"),
    ("src_user", 12, "string"),
    ("dst_user", 13, "string"),
    ("app", 14, "string"),
    ("vsys", 15, "string"),
    ("src_zone", 16, "string"),
    ("dst_zone", 17, "string"),
    ("inbound_if", 18, "string"),
    ("outbound_if", 19, "string"),
    ("log_action", 20, "string"),
    ("session_id", 22, "long"),
    ("repeat_count", 23, "long"),
    ("src_port", 24, "long"),
    ("dst_port", 25, "long"),
    ("nat_src_port", 26, "long"),
    ("nat_dst_port", 27, "long"),
    ("flags", 28, "string"),
    ("protocol", 29, "string"),
    ("action", 30, "string"),
]

_PAN_TRAFFIC = _PAN_COMMON + [
    ("bytes", 31, "long"),
    ("bytes_sent", 32, "long"),
    ("bytes_received", 33, "long"),
    ("packets", 34, "long"),
    ("start_time", 35, "ts"),
    ("elapsed_time", 36, "long"),
    ("category", 37, "string"),
    ("seqno", 39, "long"),
    ("src_location", 41, "string"),
    ("dst_location", 42, "string"),
    ("pkts_sent", 44, "long"),
    ("pkts_received", 45, "long"),
    ("session_end_reason", 46, "string"),
    ("device_group", 47, "string"),
    ("devname", 52, "string"),
    ("action_source", 53, "string"),
]

_PAN_THREAT = _PAN_COMMON + [
    ("threat_name", 31, "string"),
    ("threat_id", 32, "long"),
    ("thr_category", 33, "string"),
    ("severity", 34, "string"),
    ("direction", 35, "string"),
    ("seqno", 36, "long"),
    ("src_location", 38, "string"),
    ("dst_location", 39, "string"),
    ("content_type", 41, "string"),
    ("devname", 55, "string"),
    ("action_source", 56, "string"),
]

_PAN_SYSTEM = [
    ("receive_time", 1, "ts"),
    ("serial", 2, "string"),
    ("type", 3, "string"),
    ("subtype", 4, "string"),
    ("generated_time", 6, "ts"),
    ("vsys", 7, "string"),
    ("severity", 8, "string"),
    ("category", 9, "string"),
    ("description", 10, "string"),
    ("seqno", 11, "long"),
    ("devname", 18, "string"),
]

_PAN_AUTH = [
    ("receive_time", 1, "ts"),
    ("serial", 2, "string"),
    ("type", 3, "string"),
    ("subtype", 4, "string"),
    ("generated_time", 6, "ts"),
    ("src_ip", 7, "string"),
    ("src_user", 8, "string"),
    ("action", 9, "string"),
    ("auth_method", 10, "string"),
    ("seqno", 11, "long"),
    ("devname", 14, "string"),
]

_PAN_GLOBALPROTECT = [
    ("receive_time", 1, "ts"),
    ("serial", 2, "string"),
    ("type", 3, "string"),
    ("subtype", 4, "string"),
    ("generated_time", 6, "ts"),
    ("src_ip", 7, "string"),
    ("src_user", 8, "string"),
    ("action", 9, "string"),
    ("stage", 10, "string"),
    ("seqno", 11, "long"),
    ("client_os", 13, "string"),
    ("gateway", 14, "string"),
    ("devname", 16, "string"),
]

# (log_type, subtype) -> positional layout. type is upper-case as in the CSV.
LAYOUTS = {
    ("TRAFFIC", "end"): _PAN_TRAFFIC,
    ("TRAFFIC", "start"): _PAN_TRAFFIC,
    ("TRAFFIC", "deny"): _PAN_TRAFFIC,
    ("THREAT", "virus"): _PAN_THREAT,
    ("THREAT", "spyware"): _PAN_THREAT,
    ("THREAT", "vulnerability"): _PAN_THREAT,
    ("THREAT", "url"): _PAN_THREAT,
    ("SYSTEM", "general"): _PAN_SYSTEM,
    ("AUTH", "auth"): _PAN_AUTH,
    ("GLOBALPROTECT", "globalprotect"): _PAN_GLOBALPROTECT,
}

# Fields that must always be present (non-nullable in the Avro schema).
REQUIRED = {
    "serial",
    "type",
    "subtype",
    "devname",
    "receive_time",
    "generated_time",
}

logger = setup_logging("paloalto-streaming-app")


def schema_filename(
    log_type,
    subtype,
):
    """demo/schemas/paloalto_<type>_<subtype>.avsc (type lower-cased)."""
    return f"paloalto_{log_type.lower()}_{subtype}.avsc"


def topic_name(
    log_type,
    subtype,
):
    """siem_poc_paloalto_logs-<type>-<subtype> (type lower-cased)."""
    return f"{TOPIC_PREFIX}-{log_type.lower()}-{subtype}"


def to_epoch_millis(value):
    """Convert a PAN-OS 'YYYY/MM/DD HH:MM:SS' string to Unix epoch millis (UTC)."""
    try:
        dt = datetime.strptime(value, PAN_TS_FMT).replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)
    except (ValueError, TypeError):
        return None


def coerce(
    value,
    kind,
):
    """Coerce a raw CSV token to the Avro type implied by its layout kind."""
    if value is None or value == "":
        return None
    if kind == "ts":
        return to_epoch_millis(value)
    if kind == "long":
        try:
            return int(value)
        except ValueError:
            return None
    return value


def build_record(
    layout,
    fields,
):
    """Build an Avro-ready dict from positional CSV tokens via a layout."""
    record = dict()
    for name, idx, kind in layout:
        raw = fields[idx] if idx < len(fields) else None
        record[name] = coerce(raw, kind)
    return record


def main():
    ap = argparse.ArgumentParser(
        description="Palo Alto (PAN-OS) Kafka streaming router"
    )
    ap.add_argument(
        "--kafka-config",
        required=True,
        help="librdkafka properties file (e.g. ../kafka/config.properties)",
    )
    ap.add_argument(
        "--registry-config",
        required=True,
        help="Schema Registry properties file (e.g. ../kafka/registry.properties)",
    )
    ap.add_argument(
        "--schema-dir",
        default=DEFAULT_SCHEMA_DIR,
        help="Directory holding the .avsc schemas",
    )
    ap.add_argument(
        "--source-topic",
        default=SOURCE_TOPIC,
        help="Source topic to consume raw PAN-OS logs from",
    )
    ap.add_argument(
        "-p",
        "--partitions",
        type=int,
        default=1,
        help="Number of partitions when creating topics (default: 1). "
        "Ignored if topics already exist.",
    )
    ap.add_argument(
        "-rf",
        "--replication-factor",
        type=int,
        default=1,
        help="Replication factor when creating topics (default: 1). "
        "Ignored if topics already exist.",
    )
    ap.add_argument(
        "--retention-ms",
        type=int,
        default=DEFAULT_RETENTION_MS,
        help="retention.ms for topics created by this app (default: 1 day)",
    )
    ap.add_argument(
        "--max-poll-interval-ms",
        type=int,
        default=MAX_POLL_INTERVAL_MS,
        help=f"max.poll.interval.ms for the consumer (default: {MAX_POLL_INTERVAL_MS/(60 * 1000):.0f} min); increase if emit phase is slow",
    )
    args = ap.parse_args()

    kafka_conf = load_properties(args.kafka_config)
    sr_conf = load_properties(args.registry_config)
    auto_register = sr_conf.get("auto.register.schemas", "true").lower() == "true"

    sr_client = build_sr_client(sr_conf, kafka_conf)

    # Build per-route: positional layout, dest topic, and Avro serializer.
    routes = dict()
    dest_topics = list()
    for (log_type, subtype), layout in LAYOUTS.items():
        path = os.path.join(args.schema_dir, schema_filename(log_type, subtype))
        with open(path) as fh:
            schema_str = fh.read()
        topic = topic_name(log_type, subtype)
        serializer = AvroSerializer(
            sr_client,
            schema_str,
            conf={"auto.register.schemas": auto_register},
        )
        routes[(log_type, subtype)] = {
            "topic": topic,
            "layout": layout,
            "serializer": serializer,
        }
        dest_topics.append(topic)
    logger.info("Loaded %d schema route(s) from %s", len(routes), args.schema_dir)

    # Ensure source + all destination topics exist with the right partition count.
    admin = AdminClient(kafka_conf)
    ensure_topics(
        admin,
        [args.source_topic] + dest_topics,
        args.retention_ms,
        args.partitions,
        args.replication_factor,
    )

    key_serializer = string_serializer()
    producer = Producer(kafka_conf)

    consumer_conf = dict(kafka_conf)
    consumer_conf.update(
        {
            "group.id": CONSUMER_GROUP,
            "client.id": f"{CONSUMER_GROUP}-001",
            "auto.offset.reset": AUTO_OFFSET_RESET,
            "enable.auto.commit": True,
            "max.poll.interval.ms": args.max_poll_interval_ms,
        }
    )
    consumer = Consumer(consumer_conf)
    consumer.subscribe([args.source_topic])

    stats = {"routed": 0, "skipped": 0, "errors": 0}
    logger.info(
        "Routing from '%s' -> '%s-<type>-<subtype>' (Ctrl+C to stop)",
        args.source_topic,
        TOPIC_PREFIX,
    )
    with graceful_shutdown("Shutdown signal received, stopping") as running:
        try:
            while running["flag"]:
                msg = consumer.poll(POLL_TIMEOUT)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    logger.error("Consumer error: %s", msg.error())
                    continue

                raw = msg.value().decode("utf-8", errors="replace").strip()
                fields = raw.split(",")
                # PAN-OS CSV: idx 3 = log type, idx 4 = subtype.
                log_type = fields[3] if len(fields) > 4 else None
                subtype = fields[4] if len(fields) > 4 else None
                route = routes.get((log_type, subtype))
                if route is None:
                    logger.warning(
                        "No route for type=%s subtype=%s — skipping", log_type, subtype
                    )
                    stats["skipped"] += 1
                    continue

                record = build_record(route["layout"], fields)
                key = record.get(KEY_FIELD) or "unknown"
                try:
                    value = route["serializer"](
                        record,
                        SerializationContext(
                            route["topic"],
                            MessageField.VALUE,
                        ),
                    )
                    producer.produce(
                        topic=route["topic"],
                        key=key_serializer(key),
                        value=value,
                    )
                    producer.poll(0)
                    stats["routed"] += 1
                except BufferError:
                    producer.flush(5)
                    stats["errors"] += 1
                except Exception as e:  # noqa: BLE001
                    logger.error(
                        "Failed to route event (type=%s subtype=%s): %s",
                        log_type,
                        subtype,
                        e,
                    )
                    stats["errors"] += 1

                if stats["routed"] % 1000 == 0 and stats["routed"]:
                    logger.info(
                        "Routed=%(routed)d skipped=%(skipped)d errors=%(errors)d",
                        stats,
                    )
        finally:
            logger.info("Flushing producer...")
            remaining = producer.flush(30)
            if remaining:
                logger.warning(
                    "%d message(s) still in queue after flush timeout",
                    remaining,
                )
            consumer.close()
            logger.info(
                "Done. Routed=%(routed)d skipped=%(skipped)d errors=%(errors)d",
                stats,
            )


if __name__ == "__main__":
    sys.exit(main())
