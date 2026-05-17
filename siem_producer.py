#!/usr/bin/env python3
"""
SIEM Data Producer for Kafka with Avro Serialization
Produces data from templates to Kafka topics with automatic Avro schema inference
"""

import sys
import json
import time
import uuid
import exrex
import jinja2
import random
import logging
import argparse
import ipaddress

from typing import Dict, Any
from pathlib import Path
from datetime import datetime, timezone
from concurrent.futures import Future

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import (
    SchemaRegistryClient,
    header_schema_id_serializer,
    prefix_schema_id_serializer,
)
from confluent_kafka.schema_registry.avro import AvroSerializer

logger = logging.getLogger(__name__)


class TemplateRenderer:
    """Renders Jinja2 templates with random-data helpers registered as globals."""

    _LOGICALTYPE_PREFIX = "__logicaltype_"
    _LOGICALTYPE_SUFFIX = "__"

    def __init__(self, data_dir: Path | None = None) -> None:
        self.counters: dict[str, int] = {}
        self.logical_types: dict[str, str] = {}
        self.env = jinja2.Environment(
            autoescape=False,
            undefined=jinja2.StrictUndefined,
            keep_trailing_newline=False,
        )
        self.env.globals.update({
            "now": lambda: (
                f'{{"__logicaltype_iso-8601-timestamp__": '
                f'"{datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")}"}}'
            ),
            "unix_time_stamp": lambda max_ago: (
                f'{{"__logicaltype_timestamp-millis__": '
                f'{int(time.time() * 1000) - random.randint(0, int(max_ago) * 1000)}}}'
            ),
            "ip": self._random_ip,
            "guid": lambda: str(uuid.uuid4()),
            "randoms": self._randoms,
            "integer": random.randint,
            "random_string": self._random_string,
            "random_string_vocabulary": self._random_string_vocab,
            "counter": self._counter,
            "floating": self._floating,
            "regex": self._generate_from_regex,
            "data": self._load_data(data_dir) if data_dir else {},
        })

    @staticmethod
    def _load_data(data_dir: Path) -> Dict[str, list[str]]:
        """Load each file under data_dir as a list of stripped, non-empty,
        non-comment lines. Lines starting with `#` (after stripping) are
        ignored, so files can be self-documented. A file named `known_ports`
        becomes `data.known_ports` in templates."""
        if not data_dir.is_dir():
            return {}
        loaded: dict[str, list[str]] = {}
        for path in sorted(data_dir.iterdir()):
            if not path.is_file() or path.name.startswith("."):
                continue
            lines = [line.strip() for line in path.read_text(encoding="utf-8").splitlines()]
            loaded[path.name] = [line for line in lines if line and not line.startswith("#")]
        return loaded

    @staticmethod
    def _randoms(options):
        """Pick a random element. Accepts a pipe-separated string (`"a|b|c"`)
        or any sequence (e.g. `data.countries`). When given a string, each
        entry is stripped of surrounding whitespace and empty entries are
        dropped, so `"a | b | "` is equivalent to `"a|b"`."""
        if isinstance(options, str):
            options = [o.strip() for o in options.split("|") if o.strip()]
        return random.choice(options)

    def compile(self, source: str) -> jinja2.Template:
        """Compile a template source string once; reuse across renders."""
        return self.env.from_string(source)

    def render(self, template: jinja2.Template) -> Dict[str, Any]:
        """Render a compiled template and parse the result as JSON.

        Helpers like `unix_time_stamp` emit a wrapper dict shaped
        `{"__logicaltype_<name>__": <value>}` so the Avro schema inferrer can
        annotate the field with the right `logicalType`. We unwrap those
        markers here and record the field-to-logical-type mapping on
        `self.logical_types`.
        """
        rendered = template.render()
        try:
            data = json.loads(rendered)
        except json.JSONDecodeError as e:
            logger.error("Error parsing rendered template: %s", e)
            logger.error("Rendered content: %s", rendered)
            raise
        return self._unwrap_logical_markers(data)

    def _unwrap_logical_markers(self, obj: Any, parent_key: str = "") -> Any:
        if isinstance(obj, dict):
            if len(obj) == 1:
                only_key = next(iter(obj))
                if (
                    only_key.startswith(self._LOGICALTYPE_PREFIX)
                    and only_key.endswith(self._LOGICALTYPE_SUFFIX)
                    and len(only_key) > len(self._LOGICALTYPE_PREFIX) + len(self._LOGICALTYPE_SUFFIX)
                ):
                    logical = only_key[len(self._LOGICALTYPE_PREFIX):-len(self._LOGICALTYPE_SUFFIX)]
                    if parent_key:
                        self.logical_types[parent_key] = logical
                    return obj[only_key]
            return {k: self._unwrap_logical_markers(v, parent_key=k) for k, v in obj.items()}
        if isinstance(obj, list):
            return [self._unwrap_logical_markers(item, parent_key=parent_key) for item in obj]
        return obj

    def _counter(self, name: str, start: int, step: int) -> int:
        current = self.counters.get(name, start)
        self.counters[name] = current + step
        return current

    @staticmethod
    def _floating(min_val: float, max_val: float, decimals: int = 2) -> float:
        return round(random.uniform(min_val, max_val), decimals)

    def _random_ip(self, cidr: str) -> str:
        """Generate a random host IP from CIDR (excludes network/broadcast)."""
        if "/" not in cidr:
            return cidr

        network = ipaddress.ip_network(cidr, strict=False)
        # /31 and /32 have no usable host range — pick from all addresses
        if network.num_addresses <= 2:
            host_int = random.randint(
                int(network.network_address), int(network.broadcast_address)
            )
        else:
            host_int = random.randint(
                int(network.network_address) + 1, int(network.broadcast_address) - 1
            )
        return str(ipaddress.ip_address(host_int))

    _ALPHANUM = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

    def _random_string(self, min_len: int, max_len: int) -> str:
        """Generate a random alphanumeric string."""
        length = random.randint(min_len, max_len)
        return "".join(random.choice(self._ALPHANUM) for _ in range(length))

    def _random_string_vocab(self, min_len: int, max_len: int, vocab: str) -> str:
        """Generate a random string drawn from the given vocabulary."""
        length = random.randint(min_len, max_len)
        return "".join(random.choice(vocab) for _ in range(length))

    def _generate_from_regex(
        self,
        pattern: str,
    ) -> str:
        """Generate a random string matching a regex pattern using exrex.

        Templates commonly write `\\d` (which arrives here as the 2-character
        string `\\d`) to survive being re-read inside a JSON-shaped template.
        exrex expects standard regex syntax, so we normalise `\\\\` to `\\` first
        — that means `\\d`, `\\w`, `[A-Z]`, `{n,m}`, `+`, `*`, `?`, `|`, and
        groups all work as in `re`.
        """
        normalised = pattern.replace("\\\\", "\\")
        return exrex.getone(normalised)


def _snake_to_pascal(name: str) -> str:
    """Convert a snake_case identifier to PascalCase (e.g. src_ip -> SrcIp)."""
    return "".join(part.capitalize() for part in name.split("_") if part) or "Nested"


_LOGICAL_TYPE_PRIMITIVE: dict[str, str] = {
    "timestamp-millis": "long",
    "iso-8601-timestamp": "string",
}


def infer_avro_schema(
    data: Dict[str, Any],
    name: str,
    namespace: str = "io.confluent.siem",
    logical_types: dict[str, str] | None = None,
) -> str:
    """Infer Avro schema from a data dictionary.

    `logical_types` maps field name -> Avro logical type (e.g.
    `{"occurred_at_ms": "timestamp-millis"}`) and is populated by the
    `TemplateRenderer` when it unwraps helper markers like
    `{"__logicaltype_timestamp-millis__": ...}`.
    """

    logical_types = logical_types or {}
    used_names: set[str] = set()

    def _unique(base: str) -> str:
        candidate = base
        counter = 1
        while candidate in used_names:
            candidate = f"{base}{counter}"
            counter += 1
        used_names.add(candidate)
        return candidate

    def infer_type(value: Any, field_name: str = "") -> Any:
        """Infer Avro type from Python value."""
        if isinstance(value, bool):
            return "boolean"
        if isinstance(value, int):
            if field_name in logical_types:
                logical = logical_types[field_name]
                return {
                    "type": _LOGICAL_TYPE_PRIMITIVE.get(logical, "long"),
                    "logicalType": logical,
                }
            # Always emit `long` for integers. Picking `int` vs `long` based
            # on a sampled value is non-deterministic — random samples can land
            # on either side of 2^31, and once a topic is registered with
            # `long`, the registry's BACKWARD compatibility forbids narrowing
            # back to `int`. `int` -> `long` is a safe widening.
            return "long"
        if isinstance(value, float):
            return "double"
        if isinstance(value, str):
            if field_name in logical_types:
                logical = logical_types[field_name]
                return {
                    "type": _LOGICAL_TYPE_PRIMITIVE.get(logical, "string"),
                    "logicalType": logical,
                }
            return "string"
        if isinstance(value, dict):
            base_name = _snake_to_pascal(field_name) if field_name else "Nested"
            record_name = _unique(base_name)
            fields = [
                {"name": k, "type": infer_type(value=v, field_name=k)}
                for k, v in value.items()
            ]
            return {"type": "record", "name": record_name, "fields": fields}
        if isinstance(value, list):
            # Suffix the element's logical name so an array<dict> and a sibling
            # dict with the same field name don't collide on record names.
            element_field = f"{field_name}_item" if field_name else "item"
            if value:
                return {"type": "array", "items": infer_type(value=value[0], field_name=element_field)}
            return {"type": "array", "items": "string"}
        return "string"

    fields: list[Any] = []
    for key, value in data.items():
        fields.append({"name": key, "type": infer_type(value=value, field_name=key)})

    schema: dict[str, Any] = {
        "type": "record",
        "name": _snake_to_pascal(name) + "Record",
        "namespace": namespace,
        "fields": fields,
    }

    return json.dumps(obj=schema)


def sample_for_schema(
    renderer: "TemplateRenderer",
    template: jinja2.Template,
    num_samples: int = 5,
) -> Dict[str, Any]:
    """Render a few samples and merge them so empty/short arrays don't pin
    the schema to `array<string>` when later records would contain real items.

    Returns a representative record. Warns on fields that are always empty
    lists since their element type can't be inferred.
    """
    samples = [renderer.render(template=template) for _ in range(num_samples)]
    merged = dict(samples[0])

    # If a top-level field is an empty list in the first sample but populated
    # later, prefer the populated version.
    for sample in samples[1:]:
        for key, value in sample.items():
            if isinstance(value, list) and value and (
                key not in merged or (isinstance(merged[key], list) and not merged[key])
            ):
                merged[key] = value

    for key, value in merged.items():
        if isinstance(value, list) and not value:
            logger.warning(
                "Field '%s' is always an empty list in samples; "
                "defaulting its element type to string.",
                key,
            )

    return merged


def load_config(config_file: str) -> Dict[str, str]:
    """Load key=value pairs from a properties file (comments and blanks ignored)."""
    config: dict[str, str] = {}
    with open(config_file, "r", encoding="utf-8") as f:
        for raw_line in f:
            line = raw_line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, value = line.split("=", maxsplit=1)
                config[key.strip()] = value.strip()
    return config


def create_topic_if_not_exists(
    kafka_config: Dict[str, str],
    topic: str,
    partitions: int = 6,
    replication: int = 1,
) -> None:
    """Create the topic if it doesn't already exist."""
    admin_config = dict(kafka_config)
    admin_config.setdefault("bootstrap.servers", "localhost:9092")
    admin_config.setdefault("security.protocol", "PLAINTEXT")

    admin_client = AdminClient(admin_config)

    metadata = admin_client.list_topics(timeout=10)
    if topic in metadata.topics:
        logger.info("Topic '%s' already exists", topic)
        return

    new_topic = NewTopic(topic, num_partitions=partitions, replication_factor=replication)
    futures: Dict[str, Future] = admin_client.create_topics([new_topic])

    for topic_name, future in futures.items():
        try:
            future.result()
            logger.info("Topic '%s' created successfully", topic_name)
        except Exception as e:
            logger.error("Failed to create topic '%s': %s", topic_name, e)


def create_producer(kafka_config: Dict[str, str]) -> Producer:
    """Create a Kafka producer with defaults and an error callback for visibility."""
    producer_config: dict[str, Any] = dict(kafka_config)
    producer_config.setdefault("bootstrap.servers", "localhost:9092")
    producer_config.setdefault("security.protocol", "PLAINTEXT")
    # Surface broker-side issues (DNS failure, auth, broker down).
    producer_config["error_cb"] = lambda err: logger.error("Kafka producer error: %s", err)
    return Producer(producer_config)


def delivery_report(err, msg) -> None:
    """Delivery callback — logs only on failure to avoid flooding stdout."""
    if err is not None:
        logger.error("Message delivery failed: %s", err)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    # Third-party HTTP clients (used by the Schema Registry SDK) are chatty at
    # INFO — keep them at WARNING so our own messages aren't drowned out.
    for noisy in ("httpx", "httpcore", "urllib3"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    parser = argparse.ArgumentParser(description="SIEM Data Producer for Kafka with Avro")
    parser.add_argument("template", help="Template name (without .j2 extension)")
    parser.add_argument("-t", "--topic", help="Kafka topic (not required in dry-run mode)")
    parser.add_argument(
        "-f",
        "--frequency",
        type=float,
        default=1.0,
        help="Frequency in seconds between records (default: 1.0)",
    )
    parser.add_argument(
        "-n",
        "--num-records",
        type=int,
        default=0,
        help="Total number of records to produce (0 = continuous)",
    )
    parser.add_argument(
        "-b",
        "--batch-size",
        type=int,
        default=1,
        help="Number of records per batch (default: 1)",
    )
    parser.add_argument(
        "--kafka-config",
        default="./kafka/config.properties",
        help="Kafka configuration file",
    )
    parser.add_argument(
        "--registry-config",
        default="./kafka/registry.properties",
        help="Schema Registry configuration file",
    )
    parser.add_argument(
        "--templates-dir",
        default="./templates",
        help="Templates directory",
    )
    parser.add_argument(
        "-ns",
        "--namespace",
        default="io.confluent.siem",
        help="Avro schema namespace (default: io.confluent.siem)",
    )
    parser.add_argument(
        "-p",
        "--partitions",
        type=int,
        default=6,
        help="Number of partitions when creating the topic (default: 6). "
             "Ignored if the topic already exists.",
    )
    parser.add_argument(
        "-s",
        "--schema-id-location",
        choices=["headers", "body"],
        default="headers",
        help=(
            "Where to place the Avro schema ID. 'headers' (default, modern) "
            "stores it in the Kafka message headers; 'body' (legacy) prefixes "
            "it to the serialized value with the 5-byte magic-byte framing."
        ),
    )
    parser.add_argument(
        "-k",
        "--key",
        default=None,
        help=(
            "Top-level field whose value is used as the Kafka message key "
            "(must be a scalar: string, int, float, or bool). The field "
            "remains in the value payload. Default: no key (null)."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Generate and display data without producing to Kafka",
    )

    args = parser.parse_args()

    if not args.dry_run and not args.topic:
        parser.error("the following arguments are required: -t/--topic (not required with --dry-run)")

    template_path = Path(args.templates_dir) / f"{args.template}.j2"
    if not template_path.exists():
        logger.error("Template not found: %s", template_path)
        sys.exit(1)

    with open(template_path, "r", encoding="utf-8") as f:
        template_content = f.read()

    renderer = TemplateRenderer(data_dir=Path(args.templates_dir) / "data")
    template = renderer.compile(template_content)

    # Dry run mode - just generate and display data
    if args.dry_run:
        num_samples = args.num_records if args.num_records > 0 else 10
        print(
            f"Dry run mode - generating {num_samples} sample records from "
            f"template '{args.template}':\n"
        )
        for i in range(num_samples):
            data = renderer.render(template=template)
            print(f"Record {i + 1}:")
            print(json.dumps(data, indent=2))
            print()
        sys.exit(0)

    kafka_config = load_config(args.kafka_config)
    registry_config = load_config(args.registry_config)

    # Generate several samples to infer schema — avoids pinning empty-list
    # fields to array<string> when later records would carry real items.
    sample_data = sample_for_schema(renderer=renderer, template=template)
    avro_schema_str = infer_avro_schema(
        data=sample_data,
        name=args.template,
        namespace=args.namespace,
        logical_types=renderer.logical_types,
    )

    if args.key is not None:
        if args.key not in sample_data:
            parser.error(
                f"--key '{args.key}' not found in rendered template. "
                f"Available top-level fields: {sorted(sample_data.keys())}"
            )
        sample_value = sample_data[args.key]
        if not isinstance(sample_value, (str, int, float, bool)):
            parser.error(
                f"--key '{args.key}' must reference a scalar field "
                f"(string, int, float, bool); got {type(sample_value).__name__}"
            )

    logger.info(
        "Inferred Avro Schema:\n%s",
        json.dumps(json.loads(avro_schema_str), indent=2),
    )

    schema_registry_conf = {
        "url": registry_config.get("schemaRegistryURL", "http://localhost:8081")
    }
    if registry_config.get("basic.auth.user.info"):
        schema_registry_conf["basic.auth.user.info"] = registry_config["basic.auth.user.info"]

    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    schema_id_serializer = (
        header_schema_id_serializer
        if args.schema_id_location == "headers"
        else prefix_schema_id_serializer
    )
    avro_serializer = AvroSerializer(
        schema_registry_client,
        avro_schema_str,
        lambda obj, ctx: obj,  # obj is already a dict
        conf={"schema.id.serializer": schema_id_serializer},
    )
    logger.info("Schema ID location: %s", args.schema_id_location)

    logger.info("Checking/creating topic '%s'...", args.topic)
    create_topic_if_not_exists(kafka_config, topic=args.topic, partitions=args.partitions)

    producer = create_producer(kafka_config)

    logger.info("Producing to topic '%s' with frequency %ss", args.topic, args.frequency)
    if args.num_records > 0:
        logger.info("Total records: %d", args.num_records)
    else:
        logger.info("Mode: Continuous (press Ctrl+C to stop)")

    try:
        count = 0
        next_deadline = time.monotonic()
        while True:
            # Check if we've reached the limit
            if args.num_records > 0 and count >= args.num_records:
                break

            # Produce batch
            for _ in range(args.batch_size):
                if args.num_records > 0 and count >= args.num_records:
                    break

                # Generate and serialize. `header_schema_id_serializer`
                # populates the passed headers list with the schema-id entry,
                # which we then forward to producer.produce(). For body
                # (prefix) mode the list stays empty and is harmless.
                data = renderer.render(template=template)
                headers: list = []
                try:
                    serialized_value = avro_serializer(
                        data,
                        SerializationContext(args.topic, MessageField.VALUE, headers),
                    )
                except Exception as e:
                    logger.error("Error serializing message: %s", e)
                    continue

                # Per-record key validation. Startup only validates the first
                # sample; a template with conditional fields could omit the key
                # field on later renders, which would otherwise silently
                # produce the string "None" as the partition key.
                message_key = None
                if args.key:
                    key_value = data.get(args.key)
                    if not isinstance(key_value, (str, int, float, bool)):
                        logger.error(
                            "Key field '%s' is missing or non-scalar in this "
                            "record (got %s); skipping",
                            args.key,
                            type(key_value).__name__,
                        )
                        continue
                    message_key = str(key_value).encode("utf-8")

                # Produce, retrying on BufferError so a transient full queue
                # doesn't silently drop records. Drop after MAX_RETRIES so a
                # permanently broken producer can't wedge the loop.
                MAX_RETRIES = 5
                for attempt in range(MAX_RETRIES + 1):
                    try:
                        producer.produce(
                            topic=args.topic,
                            key=message_key,
                            value=serialized_value,
                            headers=headers if headers else None,
                            callback=delivery_report,
                        )
                        producer.poll(0)
                        count += 1
                        break
                    except BufferError:
                        if attempt == MAX_RETRIES:
                            logger.warning(
                                "Buffer full after %d retries — dropping record",
                                MAX_RETRIES,
                            )
                            break
                        producer.poll(0.5)
                    except Exception as e:
                        logger.error("Error producing message: %s", e)
                        break

            # Pace the next batch against a deadline so production time doesn't
            # cause the effective frequency to drift below the configured one.
            if args.num_records == 0 or count < args.num_records:
                next_deadline += args.frequency
                sleep_for = next_deadline - time.monotonic()
                if sleep_for > 0:
                    time.sleep(sleep_for)
                else:
                    # We're running behind — skip the sleep and reset the
                    # deadline so we don't accumulate an ever-growing debt.
                    next_deadline = time.monotonic()

    except KeyboardInterrupt:
        logger.info("Stopping producer...")

    finally:
        # Final flush, bounded so we don't hang forever if the broker is gone.
        remaining = producer.flush(timeout=30)
        if remaining:
            logger.warning(
                "%d message(s) still in queue after 30s flush timeout",
                remaining,
            )
        logger.info("Total records produced: %d", count)


if __name__ == "__main__":
    main()
