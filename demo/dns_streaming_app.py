#!/usr/bin/env python3
"""DNS log windowed-aggregation streaming app (stateful).

A Python stand-in for a Kafka Streams windowed aggregation. Consumes DNS logs
(Avro) from a source topic and aggregates them in memory into fixed, clock-aligned
*event-time* tumbling windows (default 5 minutes: 10:00:00–10:05:00, 10:05:00–
10:10:00, ...). Each event is bucketed by its own `ts`, so replayed/old events
fall into their correct historical window rather than "now".

Emission is gated on wall-clock window boundaries: nothing is released until the
*current* window ends (+ grace). So on startup, all backlog (old + current) is
buffered and released together at that first boundary; thereafter each window is
released at its boundary. Once a window has been released it is closed — events
arriving for it afterwards are discarded as late. Offsets are committed only
after a window's records are produced (at-least-once).

  source:  siem_poc_dns_logs                 (value = Avro; writer schema is
           fetched from Schema Registry by the schema id embedded in each message)
  sink:    siem_poc_dns_logs-aggregate
           value = Avro (schema in demo/schemas/dns_aggregate.avsc)
           key   = "src_ip|query|qtype|rcode|query_class|protocol" (the group key)

Aggregation per (src_ip, query, qtype, rcode, query_class, protocol):
  * ts_start / ts_end      — min / max event ts seen in the window
  * event_count            — number of raw events
  * latency_ms_avg         — mean latency_ms
  * answer/authority/additional_count — sums

Offsets are committed manually and conservatively: we commit only up to the
earliest offset still held by an *open* window, so a crash replays the open
windows (duplicate aggregate rows possible), the standard at-least-once trade-off.

Kafka + Schema Registry credentials are read from property files passed on the
command line. Endpoints may use self-signed TLS, so the CA bundle is taken from
the Kafka config's `ssl.ca.location`.
"""
import os
import sys
import time
import argparse
from datetime import datetime, timezone

from confluent_kafka import Consumer, Producer, KafkaError, KafkaException, TopicPartition
from confluent_kafka.admin import AdminClient
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer
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
SOURCE_TOPIC = "siem_poc_dns_logs"
SINK_TOPIC = "siem_poc_dns_logs-aggregate"
WINDOW_SECONDS = 300  # tumbling window size (5 minutes)
# Grace period: extra seconds after the current window's end before it (and any
# older buffered windows) are released, giving slightly-late events a chance to
# land before the boundary closes. After release, late events are discarded.
ALLOWED_LATENESS_SECONDS = 10
CONSUMER_GROUP = "dns-streaming-app"
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

logger = setup_logging("dns-streaming-app")


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


def window_floor(ts_millis, window_ms):
    """Align an event timestamp down to its tumbling-window start (epoch millis)."""
    return (ts_millis // window_ms) * window_ms


def update_state(
    windows,
    event,
    window_ms,
):
    """Fold one DNS event into its event-time window bucket. Returns window_start."""
    ts = _ts_millis(event.get("ts"))
    window_start = window_floor(ts, window_ms)
    groups = windows.setdefault(window_start, dict())
    group = tuple(str(event.get(f, "")) for f in GROUP_FIELDS)
    agg = groups.get(group)
    if agg is None:
        agg = {
            "ts_first": ts,
            "ts_last": ts,
            "event_count": 0,
            "latency_sum": 0,
            **{f: 0 for f in SUM_FIELDS},
        }
        groups[group] = agg
    agg["ts_first"] = min(agg["ts_first"], ts) if agg["event_count"] else ts
    agg["ts_last"] = max(agg["ts_last"], ts)
    agg["event_count"] += 1
    agg["latency_sum"] += _to_int(event.get(AVG_FIELD))
    for f in SUM_FIELDS:
        agg[f] += _to_int(event.get(f))
    return window_start


def to_record(group, agg, window_start, window_ms):
    """Build the Avro-ready aggregate record for one group in a window."""
    record = {f: group[i] for i, f in enumerate(GROUP_FIELDS)}
    record.update(
        {
            "window_start": window_start,
            "window_end": window_start
            + window_ms
            - 1,  # inclusive end, e.g. 10:04:59.999
            "ts_first": agg["ts_first"],
            "ts_last": agg["ts_last"],
            "event_type": EVENT_TYPE,
            "event_count": agg["event_count"],
            "latency_ms_avg": (
                round(agg["latency_sum"] / agg["event_count"], 4) if agg["event_count"] else 0.0
            ),
        }
    )
    for f in SUM_FIELDS:
        record[f] = agg[f]
    return record


def flush_window(
    window_start,
    window_ms,
    groups,
    producer,
    serializer,
    key_serializer,
):
    """Produce one aggregate per group for a window; return records produced."""
    produced = 0
    for group, agg in groups.items():
        record = to_record(group, agg, window_start, window_ms)
        key = "|".join(group)
        try:
            value = serializer(
                record, SerializationContext(SINK_TOPIC, MessageField.VALUE)
            )
            producer.produce(
                topic=SINK_TOPIC,
                key=key_serializer(key),
                value=value,
            )
            producer.poll(0)
            produced += 1
        except BufferError:
            producer.flush(5)
            producer.produce(
                topic=SINK_TOPIC,
                key=key_serializer(key),
                value=value,
            )
            produced += 1
        except Exception as e:  # noqa: BLE001
            logger.error("Failed to produce aggregate for group %s: %s", group, e)
    return produced


def main():
    ap = argparse.ArgumentParser(description="DNS windowed-aggregation streaming app")
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
        help="retention.ms for topics created by this app (default: 1 day)",
    )
    ap.add_argument(
        "--max-poll-interval-ms",
        type=int,
        default=MAX_POLL_INTERVAL_MS,
        help=f"max.poll.interval.ms for the consumer (default: {MAX_POLL_INTERVAL_MS/(60 * 1000):.0f} min); increase if emit phase is slow",
    )
    args = ap.parse_args()

    window_seconds = args.window_seconds
    kafka_conf = load_properties(args.kafka_config)
    sr_conf = load_properties(args.registry_config)
    auto_register = sr_conf.get("auto.register.schemas", "true").lower() == "true"

    sr_client = build_sr_client(sr_conf, kafka_conf)
    with open(os.path.join(args.schema_dir, SCHEMA_FILE)) as fh:
        schema_str = fh.read()
    serializer = AvroSerializer(
        sr_client,
        schema_str,
        conf={
            "auto.register.schemas": auto_register,
        },
    )
    # No reader schema passed: the deserializer reads the writer schema from the
    # Schema Registry using the schema id embedded in each Avro message.
    deserializer = AvroDeserializer(sr_client)
    key_serializer = string_serializer()

    admin = AdminClient(kafka_conf)
    ensure_topics(
        admin,
        [args.source_topic, SINK_TOPIC],
        args.retention_ms,
        args.partitions,
        args.replication_factor,
    )

    producer = Producer(kafka_conf)

    consumer_conf = dict(kafka_conf)
    consumer_conf.update(
        {
            "group.id": CONSUMER_GROUP,
            "client.id": f"{CONSUMER_GROUP}-001",
            "auto.offset.reset": AUTO_OFFSET_RESET,
            "enable.auto.commit": False,  # we commit only after producing aggregates
            "max.poll.interval.ms": args.max_poll_interval_ms,
        }
    )
    consumer = Consumer(consumer_conf)
    consumer.subscribe([args.source_topic])

    window_ms = window_seconds * 1000
    grace_ms = args.allowed_lateness * 1000

    # Open windows and the source offsets they hold (for conservative commits).
    windows = dict()  # window_start_ms -> {group_tuple: agg}
    window_offsets = dict()  # window_start_ms -> {partition: [offset, ...]}
    buffered = dict()  # partition -> {offset: window_start_ms} (still-open offsets)
    last_offset = dict()  # partition -> highest consumed offset
    stats = {
        "windows": 0,
        "aggregates": 0,
        "events": 0,
        "discarded": 0,
    }

    # Emission is gated on wall-clock window boundaries (not per-window-age):
    #  * `current_window_start` is the window we are currently inside.
    #  * Nothing is emitted until that window ends (+grace) — so on startup all
    #    backlog (old + current) accumulates and is released together at the
    #    first boundary.
    #  * `closed_through` is the exclusive boundary below which windows are
    #    already emitted; events landing there afterwards are discarded (late).
    closed_through = None
    current_window_start = window_floor(int(time.time() * 1000), window_ms)
    logger.info(
        "Aggregating '%s' -> '%s' over %ds clock-aligned event-time windows "
        "(grace %ds); first release at %s (Ctrl+C to stop)",
        args.source_topic,
        SINK_TOPIC,
        window_seconds,
        args.allowed_lateness,
        datetime.fromtimestamp(
            (current_window_start + window_ms + grace_ms) / 1000,
            timezone.utc,
        ).isoformat(),
    )

    def commit_offsets():
        """Commit up to the earliest offset still held by an open window."""
        tps = list()
        for partition, last in last_offset.items():
            pending = buffered.get(partition)
            commit_off = min(pending) if pending else last + 1
            tps.append(TopicPartition(args.source_topic, partition, commit_off))
        if tps:
            try:
                consumer.commit(offsets=tps, asynchronous=False)
            except KafkaException as exc:
                err = exc.args[0]
                if err.code() == KafkaError.ILLEGAL_GENERATION:
                    logger.warning(
                        "Offset commit skipped — consumer rejoined group during emit "
                        "(ILLEGAL_GENERATION); offsets will be recommitted after rejoin"
                    )
                else:
                    raise

    def release_offsets(window_start):
        """Drop the emitted window's offsets from the open-offset bookkeeping."""
        for partition, offsets in window_offsets.pop(window_start, dict()).items():
            pmap = buffered.get(partition)
            if pmap:
                for off in offsets:
                    pmap.pop(off, None)

    def emit_windows(due):
        """Produce + commit the given windows (in time order)."""
        committed_any = False
        for window_start in sorted(due):
            groups = windows[window_start]
            produced = flush_window(
                window_start,
                window_ms,
                groups,
                producer,
                serializer,
                key_serializer,
            )
            remaining = producer.flush(30)
            if remaining:
                logger.error(
                    "Window %s: %d record(s) undelivered after flush — NOT committing "
                    "(window stays open, will retry)",
                    datetime.fromtimestamp(
                        window_start / 1000, timezone.utc
                    ).isoformat(),
                    remaining,
                )
                continue  # keep window + its offsets open; retry next tick
            event_total = sum(a["event_count"] for a in groups.values())
            del windows[window_start]
            release_offsets(window_start)
            stats["windows"] += 1
            stats["aggregates"] += produced
            stats["events"] += event_total
            committed_any = True
            logger.info(
                "Window %s: %d event(s) -> %d aggregate(s) produced",
                datetime.fromtimestamp(window_start / 1000, timezone.utc).isoformat(),
                event_total,
                produced,
            )
        if committed_any:
            commit_offsets()

    def maybe_close_boundary():
        """If the current window has ended (+grace), release every ended window."""
        nonlocal closed_through, current_window_start
        now_ms = int(time.time() * 1000)
        if now_ms < current_window_start + window_ms + grace_ms:
            return  # still inside the current window — keep accumulating
        cutoff = window_floor(now_ms, window_ms)  # all windows < cutoff have ended
        emit_windows([w for w in windows if w < cutoff])
        closed_through = cutoff  # windows below this are now closed → late = drop
        current_window_start = cutoff

    with graceful_shutdown("Shutdown signal received, stopping after current window") as running:
        try:
            while running["flag"]:
                maybe_close_boundary()

                msg = consumer.poll(POLL_TIMEOUT)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    logger.error("Consumer error: %s", msg.error())
                    continue

                partition, offset = msg.partition(), msg.offset()
                last_offset[partition] = max(last_offset.get(partition, -1), offset)

                try:
                    # Pass the message headers: the producer stores the Avro schema
                    # id in the Kafka headers (schema-id-location=headers), so the
                    # deserializer must read it from there (it falls back to the
                    # magic-byte payload prefix when no header is present).
                    event = deserializer(
                        msg.value(),
                        SerializationContext(
                            args.source_topic,
                            MessageField.VALUE,
                            msg.headers(),
                        ),
                    )
                except Exception as e:  # noqa: BLE001
                    logger.warning("Skipping undeserializable message: %s", e)
                    continue
                if event is None:  # tombstone / null value
                    continue

                window_start = window_floor(_ts_millis(event.get("ts")), window_ms)
                # Late event for an already-emitted window -> discard (offset already
                # tracked above so it won't block commits).
                if closed_through is not None and window_start < closed_through:
                    stats["discarded"] += 1
                    continue

                update_state(windows, event, window_ms)
                buffered.setdefault(partition, dict())[offset] = window_start
                window_offsets.setdefault(window_start, dict()).setdefault(
                    partition, list()
                ).append(offset)
        finally:
            logger.info("Final flush of all open windows...")
            emit_windows(list(windows.keys()))
            consumer.close()
            logger.info(
                "Done. windows=%(windows)d aggregates=%(aggregates)d events=%(events)d "
                "discarded=%(discarded)d",
                stats,
            )


if __name__ == "__main__":
    sys.exit(main())
