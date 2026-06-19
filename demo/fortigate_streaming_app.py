#!/usr/bin/env python3
"""FortiGate streaming router.

Consumes raw FortiGate syslog (key=value, UTF-8 strings) from a single source
Kafka topic, parses each event into JSON, and re-produces it as Avro to a
per-(type/subtype) destination topic.

  source:  siem-poc-fortigate-logs            (value = raw UTF-8 string)
  dest:    siem-poc-fortigate-logs-<type>-<subtype>
           value = Avro (schema in demo/schemas/fortigate_<type>_<subtype>.avsc)
           key   = devname (raw UTF-8 string), common to every event

Kafka + Schema Registry credentials are read from property files passed on the
command line (see demo/config/). Endpoints may use self-signed TLS, so the CA
bundle is taken from the Kafka config's `ssl.ca.location`.
"""
import os
import re
import sys
import json
import argparse

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
    build_sr_client,
    ensure_topics,
    load_properties,
    setup_logging,
    graceful_shutdown,
    string_serializer,
)

# ── Tunables ─────────────────────────────────────────────────────────────────
SOURCE_TOPIC = "siem_poc_fortigate_logs"
TOPIC_PREFIX = "siem_poc_fortigate_logs"  # dest = PREFIX-<type>-<subtype>
CONSUMER_GROUP = "fortigate-streaming-app"
KEY_FIELD = "devname"  # message key, common to all events

# Known FortiGate (type, subtype) combinations → Avro schema file (in schema dir).
# Destination topic name is derived as TOPIC_PREFIX-<type>-<subtype>.
EVENT_ROUTES = {
    ("traffic", "forward"): "fortigate_traffic_forward.avsc",
    ("traffic", "local"): "fortigate_traffic_local.avsc",
    ("utm", "webfilter"): "fortigate_utm_webfilter.avsc",
    ("utm", "virus"): "fortigate_utm_virus.avsc",
    ("utm", "dns"): "fortigate_utm_dns.avsc",
    ("utm", "ips"): "fortigate_utm_ips.avsc",
    ("event", "system"): "fortigate_event_system.avsc",
    ("event", "vpn"): "fortigate_event_vpn.avsc",
    ("event", "user"): "fortigate_event_user.avsc",
}

logger = setup_logging("fortigate-streaming-app")

# key=value parser: value is either "quoted (may contain spaces)" or a bare token.
_KV_RE = re.compile(r'(\w+)=(?:"([^"]*)"|(\S+))')


def avro_type_of(field):
    """Return the non-null Avro primitive name for a schema field.

    Handles unions (e.g. ["null", "long"]) and logical types
    (e.g. {"type": "long", "logicalType": "timestamp-millis"}).
    """
    t = field["type"]
    if isinstance(t, list):  # union, e.g. ["null", "long"]
        t = next((x for x in t if x != "null"), "string")
    if isinstance(t, dict):  # logical type wrapper
        t = t.get("type", "string")
    return t


def coerce(
    value,
    avro_type,
):
    """Coerce a parsed string value to the type the Avro schema expects."""
    if value is None or value == "":
        return (
            None
            if value is None
            else (None if avro_type in ("long", "int", "double", "float") else "")
        )
    if avro_type in ("long", "int"):
        try:
            return int(value)
        except ValueError:
            return None
    if avro_type in ("double", "float"):
        try:
            return float(value)
        except ValueError:
            return None
    return value


def parse_event(raw):
    """Parse a raw FortiGate key=value line into a flat dict of strings."""
    parsed = {}
    for m in _KV_RE.finditer(raw):
        key = m.group(1)
        parsed[key] = m.group(2) if m.group(2) is not None else m.group(3)
    return parsed


def to_record(
    parsed,
    schema_fields,
):
    """Build an Avro-ready dict, coercing values and nulling absent fields."""
    record = {}
    for name, avro_type in schema_fields.items():
        record[name] = coerce(parsed.get(name), avro_type)
    return record


def main():
    ap = argparse.ArgumentParser(description="FortiGate Kafka streaming router")
    ap.add_argument(
        "--kafka-config",
        required=True,
        help="librdkafka properties file (e.g. demo/config/edge.python.properties)",
    )
    ap.add_argument(
        "--registry-config",
        required=True,
        help="Schema Registry properties file (e.g. demo/config/sr-edge.properties)",
    )
    ap.add_argument(
        "--schema-dir",
        default=DEFAULT_SCHEMA_DIR,
        help="Directory holding the .avsc schemas",
    )
    ap.add_argument(
        "--source-topic",
        default=SOURCE_TOPIC,
        help="Source topic to consume raw FortiGate logs from",
    )
    ap.add_argument(
        "--retention-ms",
        type=int,
        default=DEFAULT_RETENTION_MS,
        help="retention.ms for topics created by this app (default: 1 day)",
    )
    args = ap.parse_args()

    kafka_conf = load_properties(args.kafka_config)
    sr_conf = load_properties(args.registry_config)
    auto_register = sr_conf.get("auto.register.schemas", "true").lower() == "true"

    sr_client = build_sr_client(sr_conf, kafka_conf)

    # Build per-route: schema field-type map, dest topic, and Avro serializer.
    routes = {}
    dest_topics = []
    for (typ, sub), filename in EVENT_ROUTES.items():
        path = os.path.join(args.schema_dir, filename)
        with open(path) as fh:
            schema_str = fh.read()
        schema = json.loads(schema_str)
        field_types = {f["name"]: avro_type_of(f) for f in schema["fields"]}
        topic = f"{TOPIC_PREFIX}-{typ}-{sub}"
        serializer = AvroSerializer(
            sr_client,
            schema_str,
            conf={
                "auto.register.schemas": auto_register,
            },
        )
        routes[(typ, sub)] = {
            "topic": topic,
            "fields": field_types,
            "serializer": serializer,
        }
        dest_topics.append(topic)
    logger.info("Loaded %d schema route(s) from %s", len(routes), args.schema_dir)

    # Ensure source + all destination topics exist with the right partition count.
    admin = AdminClient(kafka_conf)
    ensure_topics(admin, [args.source_topic] + dest_topics, args.retention_ms)

    key_serializer = string_serializer()
    producer = Producer(kafka_conf)

    consumer_conf = dict(kafka_conf)
    consumer_conf.update(
        {
            "group.id": CONSUMER_GROUP,
            "client.id": f"{CONSUMER_GROUP}-001",
            "auto.offset.reset": AUTO_OFFSET_RESET,
            "enable.auto.commit": True,
        }
    )
    consumer = Consumer(consumer_conf)
    consumer.subscribe([args.source_topic])

    stats = {
        "routed": 0,
        "skipped": 0,
        "errors": 0,
    }
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

                raw = msg.value().decode("utf-8", errors="replace")
                parsed = parse_event(raw)
                # Derive epoch-millis `timestamp` from `eventtime` (epoch seconds),
                # replacing the source's separate date/time strings.
                if parsed.get("eventtime"):
                    try:
                        parsed["timestamp"] = str(int(parsed["eventtime"]) * 1000)
                    except ValueError:
                        pass
                typ, sub = parsed.get("type"), parsed.get("subtype")
                route = routes.get((typ, sub))
                if route is None:
                    logger.warning(
                        "No route for type=%s subtype=%s — skipping",
                        typ,
                        sub,
                    )
                    stats["skipped"] += 1
                    continue

                record = to_record(parsed, route["fields"])
                key = parsed.get(KEY_FIELD, "unknown")
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
                except Exception as e:
                    logger.error(
                        "Failed to route event (type=%s subtype=%s): %s",
                        typ,
                        sub,
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
