#!/usr/bin/env python3
"""DNS log windowed-aggregation streaming app (Quix Streams).

A Quix Streams re-implementation of `dns_streaming_app.py`. Consumes DNS logs
(Avro) from a source topic and aggregates them into fixed, clock-aligned
*event-time* tumbling windows (default 5 minutes). Each event is bucketed by its
own `ts` (via a timestamp extractor), so replayed/old events fall into their
correct historical window rather than "now".

Quix Streams handles the parts that were hand-rolled in the plain-consumer
version:
  * windowing + grace/lateness        -> `tumbling_window(...).reduce(...).final()`
  * per-key aggregation state          -> RocksDB-backed state (./state) + changelog
  * offset commits / crash recovery    -> managed by the framework

`.final()` emits one record per (group, window) only after the window has closed
(window end + grace), which matches the original's "release at the boundary,
then drop late events" behaviour — late events fall outside the grace and are
ignored by the windowing operator.

  source:  siem_poc_dns_logs                 (value = Avro; writer schema fetched
           from Schema Registry by the schema id embedded in each message)
  sink:    siem_poc_dns_logs-aggregate
           value = Avro (schema in demo/schemas/dns_aggregate.avsc)
           key   = "event_type|src_ip|qtype|rcode|protocol" (the group key)

Aggregation per (event_type, src_ip, qtype, rcode, protocol):
  * ts_first / ts_last     — min / max event ts seen in the window
  * event_count            — number of raw events
  * latency_ms_avg         — mean latency_ms
  * answer/authority/additional_count — sums

Kafka + Schema Registry credentials are read from property files passed on the
command line. Endpoints may use self-signed TLS, so the CA bundle is taken from
the Kafka config's `ssl.ca.location`.

Quix Streams wraps confluent-kafka's Avro serializers/Schema Registry client, so
the wire format is the standard Confluent magic-byte + schema-id-in-payload.
"""
import os
import sys
import json
import argparse
from datetime import datetime, timezone

from quixstreams import Application
from quixstreams.kafka.configuration import ConnectionConfig
from quixstreams.models import TopicConfig
from quixstreams.models.serializers.avro import AvroSerializer, AvroDeserializer
from quixstreams.models.serializers.schema_registry import (
    SchemaRegistryClientConfig,
    SchemaRegistrySerializationConfig,
)

from utils import (
    AUTO_OFFSET_RESET,
    DEFAULT_RETENTION_MS,
    DEFAULT_SCHEMA_DIR,
    NUM_PARTITIONS,
    REPLICATION_FACTOR,
    load_properties,
    setup_logging,
)

# ── Tunables ─────────────────────────────────────────────────────────────────
SOURCE_TOPIC = "siem_poc_dns_logs"
SINK_TOPIC = "siem_poc_dns_logs-aggregate"
WINDOW_SECONDS = 300  # tumbling window size (5 minutes)
# Grace period: extra seconds past a window's end before it is emitted, giving
# slightly-late events a chance to land. Events later than this are ignored.
ALLOWED_LATENESS_SECONDS = 10
CONSUMER_GROUP = "dns-quix-app"
SCHEMA_FILE = "dns_aggregate.avsc"

# Fields the aggregation groups by (composite key). Order defines the message key.
# `query`/`query_class` are intentionally excluded: they are high-cardinality and
# would defeat the volume reduction (near 1 aggregate per raw event).
GROUP_FIELDS = [
    "event_type",
    "src_ip",
    "qtype",
    "rcode",
    "protocol",
]
# Fields summed across the group.
SUM_FIELDS = [
    "answer_count",
    "authority_count",
    "additional_count",
]
# Field averaged across the group.
AVG_FIELD = "latency_ms"
EVENT_TYPE = "dns"

logger = setup_logging("dns-quix-app")


def build_sr_config(
    sr_conf,
    kafka_conf,
):
    """Build a SchemaRegistryClientConfig from registry properties.

    Maps `schemaRegistryURL` → `url`, carries `basic.auth.user.info` when set,
    and falls back to the Kafka CA bundle for self-signed Schema Registry TLS.
    """
    ca = sr_conf.get("ssl.ca.location") or kafka_conf.get("ssl.ca.location")
    return SchemaRegistryClientConfig(
        url=sr_conf["schemaRegistryURL"],
        basic_auth_user_info=sr_conf.get("basic.auth.user.info"),
        ssl_ca_location=ca,
    )


def _to_int(
    value,
    default=0,
):
    try:
        return int(value)
    except (ValueError, TypeError):
        return default


def _ts_millis(value):
    """Normalise an event 'ts' to epoch millis.

    The Avro deserializer returns timestamp-millis as a (tz-aware) datetime;
    raw ints/strings are also accepted for safety.
    """
    if isinstance(value, datetime):
        return int(value.timestamp() * 1000)
    return _to_int(value)


def group_key(value):
    """Composite group key, e.g. 'dns|1.2.3.4|A|NOERROR|udp'."""
    return "|".join(str(value.get(f, "")) for f in GROUP_FIELDS)


def output_key(record):
    """Message key for the sink, rebuilt from the aggregate record's group fields.

    The post-`group_by` message key arrives at `to_topic` already serialized to
    bytes, which the sink's string key serializer can't re-encode; deriving the
    key here from the record keeps it a clean 'dns|src_ip|...' string.
    """
    return "|".join(str(record[f]) for f in GROUP_FIELDS)


def ts_extractor(value, headers, timestamp, timestamp_type):  # noqa: ARG001
    """Use each event's own `ts` as its event time (epoch millis)."""
    return _ts_millis(value.get("ts"))


def initializer(value):
    """Seed a group's aggregate from the first event in its window."""
    ts = _ts_millis(value.get("ts"))
    agg = {f: str(value.get(f, "")) for f in GROUP_FIELDS}
    agg.update(
        {
            "ts_first": ts,
            "ts_last": ts,
            "event_count": 1,
            "latency_sum": _to_int(value.get(AVG_FIELD)),
        }
    )
    for f in SUM_FIELDS:
        agg[f] = _to_int(value.get(f))
    return agg


def reducer(agg, value):
    """Fold one more event into an existing group aggregate."""
    ts = _ts_millis(value.get("ts"))
    out = {f: agg[f] for f in GROUP_FIELDS}
    out.update(
        {
            "ts_first": min(agg["ts_first"], ts),
            "ts_last": max(agg["ts_last"], ts),
            "event_count": agg["event_count"] + 1,
            "latency_sum": agg["latency_sum"] + _to_int(value.get(AVG_FIELD)),
        }
    )
    for f in SUM_FIELDS:
        out[f] = agg[f] + _to_int(value.get(f))
    return out


def to_aggregate(windowed):
    """Map a closed window {start,end,value} into the Avro aggregate record."""
    agg = windowed["value"]
    count = agg["event_count"]
    record = {f: agg[f] for f in GROUP_FIELDS}
    record.update(
        {
            "window_start": windowed["start"],
            # Quix window end is exclusive (start + size); make it inclusive,
            # e.g. 10:04:59.999, to match the original sink contract.
            "window_end": windowed["end"] - 1,
            "ts_first": agg["ts_first"],
            "ts_last": agg["ts_last"],
            "event_type": EVENT_TYPE,
            "event_count": count,
            "latency_ms_avg": agg["latency_sum"] / count if count else 0.0,
        }
    )
    for f in SUM_FIELDS:
        record[f] = agg[f]
    return record


def main():
    ap = argparse.ArgumentParser(
        description="DNS windowed-aggregation streaming app (Quix Streams)"
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
        help="Source topic to consume raw DNS logs from",
    )
    ap.add_argument(
        "--window-seconds",
        type=int,
        default=WINDOW_SECONDS,
        help="Tumbling window size in seconds",
    )
    ap.add_argument(
        "--allowed-lateness",
        type=int,
        default=ALLOWED_LATENESS_SECONDS,
        help="Grace seconds past window end before the window is emitted",
    )
    ap.add_argument(
        "--retention-ms",
        type=int,
        default=DEFAULT_RETENTION_MS,
        help="retention.ms for the source/sink topics (default: 1 day)",
    )
    ap.add_argument(
        "--session-timeout-ms",
        type=int,
        default=60000,
        help="Kafka session.timeout.ms (consumer kicked out after this ms without heartbeat)",
    )
    ap.add_argument(
        "--heartbeat-interval-ms",
        type=int,
        default=5000,
        help="Kafka heartbeat.interval.ms (how often to send heartbeats)",
    )
    args = ap.parse_args()

    kafka_conf = load_properties(args.kafka_config)
    kafka_conf["session.timeout.ms"] = str(args.session_timeout_ms)
    kafka_conf["heartbeat.interval.ms"] = str(args.heartbeat_interval_ms)

    sr_conf = load_properties(args.registry_config)
    auto_register = sr_conf.get("auto.register.schemas", "true").lower() == "true"

    sr_config = build_sr_config(sr_conf, kafka_conf)
    # Quix's AvroSerializer feeds the schema to fastavro.parse_schema, which
    # expects a parsed dict (unlike confluent's AvroSerializer, which takes the
    # raw JSON string). Passing a string makes fastavro treat it as a type name.
    with open(os.path.join(args.schema_dir, SCHEMA_FILE)) as fh:
        schema_dict = json.load(fh)

    # No reader schema on the deserializer: it reads the writer schema from the
    # Schema Registry using the schema id embedded in each Avro message.
    value_deserializer = AvroDeserializer(schema_registry_client_config=sr_config)
    value_serializer = AvroSerializer(
        schema_dict,
        schema_registry_client_config=sr_config,
        schema_registry_serialization_config=SchemaRegistrySerializationConfig(
            auto_register_schemas=auto_register,
        ),
    )

    # Carry all librdkafka settings (bootstrap.servers, SASL/SSL, self-signed CA)
    # into the Quix connection config.
    connection = ConnectionConfig.from_librdkafka_dict(
        kafka_conf,
        ignore_extras=True,
    )

    app = Application(
        broker_address=connection,
        consumer_group=CONSUMER_GROUP,
        auto_offset_reset=AUTO_OFFSET_RESET,
    )

    # retention.ms applies to the source/sink topics we declare here; Quix's
    # internal changelog/repartition topics keep their own (compacted) config.
    topic_config = TopicConfig(
        num_partitions=NUM_PARTITIONS,
        replication_factor=REPLICATION_FACTOR,
        extra_config={"retention.ms": str(args.retention_ms)},
    )
    input_topic = app.topic(
        args.source_topic,
        value_deserializer=value_deserializer,
        timestamp_extractor=ts_extractor,
        config=topic_config,
    )
    output_topic = app.topic(
        SINK_TOPIC,
        key_serializer="string",
        value_serializer=value_serializer,
        config=topic_config,
    )

    window_ms = args.window_seconds * 1000
    grace_ms = args.allowed_lateness * 1000

    logger.info(
        "Aggregating '%s' -> '%s' over %ds clock-aligned event-time tumbling "
        "windows (grace %ds). State in ./state. Ctrl+C to stop.",
        args.source_topic,
        SINK_TOPIC,
        args.window_seconds,
        args.allowed_lateness,
    )

    sdf = app.dataframe(topic=input_topic)
    # Re-key by the composite group key so the windowed aggregation buckets per
    # group (Quix windows are keyed by the Kafka message key).
    sdf = sdf.group_by(group_key, name="dns-group")
    sdf = (
        sdf.tumbling_window(
            duration_ms=window_ms,
            grace_ms=grace_ms,
        ).reduce(
            reducer=reducer,
            initializer=initializer,
        )
        # Emit each window once, after it closes (event-time end + grace).
        # "partition": any event advances time and closes every ended window in
        # the partition together, mirroring the original's boundary release
        # (vs. the default "key", which only closes windows for the same key).
        .final(closing_strategy="partition")
    )
    sdf = sdf.apply(to_aggregate)

    def _log(record):
        logger.info(
            "Window %s: %d event(s) -> %s|%s|%s|%s|%s",
            datetime.fromtimestamp(
                record["window_start"] / 1000, timezone.utc
            ).isoformat(),
            record["event_count"],
            record["event_type"],
            record["src_ip"],
            record["qtype"],
            record["rcode"],
            record["protocol"],
        )

    sdf = sdf.update(_log)
    sdf.to_topic(
        output_topic,
        key=output_key,
    )

    app.run()


if __name__ == "__main__":
    sys.exit(main())
