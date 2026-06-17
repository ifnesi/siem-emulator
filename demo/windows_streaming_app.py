#!/usr/bin/env python3
"""Windows Event Log streaming router.

Consumes raw Windows events (flattened JSON, UTF-8 strings — Winlogbeat/NXLog
style) from a single source Kafka topic, parses each event, and re-produces it
as Avro to a per-(Channel/EventID) destination topic.

  source:  siem_poc_windows_logs               (value = raw UTF-8 JSON string)
  dest:    siem_poc_windows_logs-<channel>-<eventid>
           value = Avro (schema in demo/schemas/windows_<channel>_<eventid>.avsc)
           key   = Computer (raw UTF-8 string)

Routing is by Channel + EventID because each EventID carries a distinct
EventData payload. Channel names are slugged for Kafka topic legality
(e.g. "Microsoft-Windows-Sysmon/Operational" -> "sysmon"). The JSON "@timestamp"
(ISO-8601) is converted to Unix epoch millis and stored in the Avro field
"timestamp" (@ is not a legal Avro field name).

Kafka + Schema Registry credentials are read from property files passed on the
command line. Endpoints may use self-signed TLS, so the CA bundle is taken from
the Kafka config's `ssl.ca.location`.
"""
import os
import re
import sys
import glob
import json
import signal
import logging
import argparse
from datetime import datetime, timezone

from confluent_kafka import Consumer, Producer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import (
    MessageField,
    SerializationContext,
    StringSerializer,
)

# ── Tunables ─────────────────────────────────────────────────────────────────
SOURCE_TOPIC = "siem_poc_windows_logs"
TOPIC_PREFIX = "siem_poc_windows_logs"  # dest = PREFIX-<channel>-<eventid>
NUM_PARTITIONS = 1
REPLICATION_FACTOR = 1
CONSUMER_GROUP = "windows-streaming-app"
AUTO_OFFSET_RESET = "earliest"
DEFAULT_SCHEMA_DIR = os.path.join(os.path.dirname(__file__), "schemas")
KEY_FIELD = "Computer"  # message key
POLL_TIMEOUT = 1.0
ADMIN_OP_TIMEOUT = 30.0
ISO_TS_FMT = "%Y-%m-%dT%H:%M:%S.%fZ"  # Windows @timestamp format

# Channel -> short slug for legal, readable Kafka topic names. Any channel not
# listed falls back to a generic slugifier.
CHANNEL_SLUGS = {
    "Security": "security",
    "System": "system",
    "Application": "application",
    "Microsoft-Windows-PowerShell/Operational": "powershell",
    "Microsoft-Windows-Sysmon/Operational": "sysmon",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("windows-streaming-app")


def channel_slug(channel):
    """Map a Windows Channel to a Kafka-legal slug."""
    if channel in CHANNEL_SLUGS:
        return CHANNEL_SLUGS[channel]
    slug = re.sub(r"[^a-z0-9]+", "-", (channel or "unknown").lower()).strip("-")
    return slug or "unknown"


def schema_filename(
    slug,
    eventid,
):
    """demo/schemas/windows_<slug>_<eventid>.avsc."""
    return f"windows_{slug}_{eventid}.avsc"


def topic_name(
    slug,
    eventid,
):
    """siem_poc_windows_logs-<slug>-<eventid>."""
    return f"{TOPIC_PREFIX}-{slug}-{eventid}"


def route_from_filename(path):
    """Parse 'windows_<slug>_<eventid>.avsc' -> (slug, eventid)."""
    stem = os.path.basename(path)[len("windows_") : -len(".avsc")]
    slug, eventid = stem.rsplit("_", 1)
    return slug, eventid


def load_properties(path):
    """Read a simple java-style key=value properties file into a dict."""
    conf = {}
    with open(path) as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, val = line.split("=", 1)
            conf[key.strip()] = val.strip()
    return conf


def build_sr_client(
    sr_conf,
    kafka_conf,
):
    """Build a SchemaRegistryClient from registry properties.

    Maps `schemaRegistryURL` → `url`, carries `basic.auth.user.info` when set,
    and falls back to the Kafka CA bundle for self-signed Schema Registry TLS.
    """
    client_conf = {"url": sr_conf["schemaRegistryURL"]}
    if sr_conf.get("basic.auth.user.info"):
        client_conf["basic.auth.user.info"] = sr_conf["basic.auth.user.info"]
    ca = sr_conf.get("ssl.ca.location") or kafka_conf.get("ssl.ca.location")
    if ca:
        client_conf["ssl.ca.location"] = ca
    return SchemaRegistryClient(client_conf)


def iso_to_millis(value):
    """Convert a Windows ISO-8601 '@timestamp' to Unix epoch millis (UTC)."""
    try:
        dt = datetime.strptime(value, ISO_TS_FMT).replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)
    except (ValueError, TypeError):
        return None


def resolve_type(ftype):
    """Resolve an Avro field type to ('record', subfields) | (kind, None).

    kind ∈ {'ts', 'long', 'int', 'string', ...}. Handles unions and logical
    types (timestamp-millis) and nested records.
    """
    if isinstance(ftype, list):  # union, e.g. ["null", {...}]
        ftype = next((x for x in ftype if x != "null"), "string")
    if isinstance(ftype, dict):
        if ftype.get("logicalType") == "timestamp-millis":
            return "ts", None
        if ftype.get("type") == "record":
            return "record", ftype["fields"]
        return ftype.get("type", "string"), None
    return ftype, None


def coerce_scalar(
    value,
    kind,
):
    """Coerce a JSON value to the Avro type implied by `kind`."""
    if value is None or value == "":
        return None
    if kind == "ts":
        return iso_to_millis(value)
    if kind in ("long", "int"):
        try:
            return int(value)
        except (ValueError, TypeError):
            return None
    if kind in ("double", "float"):
        try:
            return float(value)
        except (ValueError, TypeError):
            return None
    return value if isinstance(value, str) else str(value)


def build_record(
    schema_fields,
    source,
):
    """Build an Avro-ready dict from a parsed JSON object, driven by the schema."""
    record = {}
    src = source or {}
    for field in schema_fields:
        name = field["name"]
        kind, subfields = resolve_type(field["type"])
        if kind == "record":
            record[name] = build_record(subfields, src.get(name))
        else:
            record[name] = coerce_scalar(src.get(name), kind)
    return record


def ensure_topics(
    admin,
    topics,
):
    """Create any missing topics with NUM_PARTITIONS partitions."""
    existing = set(admin.list_topics(timeout=ADMIN_OP_TIMEOUT).topics.keys())
    to_create = [
        NewTopic(
            t,
            num_partitions=NUM_PARTITIONS,
            replication_factor=REPLICATION_FACTOR,
        )
        for t in topics
        if t not in existing
    ]
    if not to_create:
        logger.info("All %d topic(s) already exist", len(topics))
        return
    for topic, fut in admin.create_topics(to_create).items():
        try:
            fut.result()
            logger.info(
                "Created topic '%s' (%d partitions)",
                topic,
                NUM_PARTITIONS,
            )
        except Exception as e:  # noqa: BLE001 - already-exists races are fine
            if "already exists" in str(e).lower():
                logger.info("Topic '%s' already exists", topic)
            else:
                logger.error("Failed to create topic '%s': %s", topic, e)


def main():
    ap = argparse.ArgumentParser(description="Windows Event Log Kafka streaming router")
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
        help="Source topic to consume raw Windows logs from",
    )
    args = ap.parse_args()

    kafka_conf = load_properties(args.kafka_config)
    sr_conf = load_properties(args.registry_config)
    auto_register = sr_conf.get("auto.register.schemas", "true").lower() == "true"

    sr_client = build_sr_client(sr_conf, kafka_conf)

    # Discover every windows_<slug>_<eventid>.avsc and build a route per file.
    routes = {}
    dest_topics = []
    for path in sorted(glob.glob(os.path.join(args.schema_dir, "windows_*.avsc"))):
        slug, eventid = route_from_filename(path)
        with open(path) as fh:
            schema_str = fh.read()
        schema = json.loads(schema_str)
        topic = topic_name(slug, eventid)
        serializer = AvroSerializer(
            sr_client,
            schema_str,
            conf={"auto.register.schemas": auto_register},
        )
        routes[(slug, eventid)] = {
            "topic": topic,
            "fields": schema["fields"],
            "serializer": serializer,
        }
        dest_topics.append(topic)
    if not routes:
        logger.error("No windows_*.avsc schemas found in %s", args.schema_dir)
        return 1
    logger.info("Loaded %d schema route(s) from %s", len(routes), args.schema_dir)

    # Ensure source + all destination topics exist with the right partition count.
    admin = AdminClient(kafka_conf)
    ensure_topics(admin, [args.source_topic] + dest_topics)

    key_serializer = StringSerializer("utf_8")
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

    running = {"flag": True}

    def _stop(signum, frame):  # noqa: ARG001
        logger.info("Shutdown signal received, stopping...")
        running["flag"] = False

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    stats = {"routed": 0, "skipped": 0, "errors": 0}
    logger.info(
        "Routing from '%s' -> '%s-<channel>-<eventid>' (Ctrl+C to stop)",
        args.source_topic,
        TOPIC_PREFIX,
    )
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
            try:
                event = json.loads(raw)
            except json.JSONDecodeError as e:
                logger.warning("Skipping non-JSON message: %s", e)
                stats["skipped"] += 1
                continue

            # @timestamp is not a legal Avro field name -> mirror to 'timestamp'.
            event["timestamp"] = event.get("@timestamp")
            slug = channel_slug(event.get("Channel"))
            eventid = str(event.get("EventID"))
            route = routes.get((slug, eventid))
            if route is None:
                logger.warning(
                    "No route for Channel=%s EventID=%s — skipping",
                    event.get("Channel"),
                    eventid,
                )
                stats["skipped"] += 1
                continue

            record = build_record(route["fields"], event)
            key = record.get(KEY_FIELD) or "unknown"
            try:
                value = route["serializer"](
                    record,
                    SerializationContext(route["topic"], MessageField.VALUE),
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
                    "Failed to route event (Channel=%s EventID=%s): %s",
                    event.get("Channel"),
                    eventid,
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
