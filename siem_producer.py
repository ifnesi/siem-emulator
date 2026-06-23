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

DEFAULT_RETENTION_MS = 86400000  # retention.ms when creating a topic (1 day)


class TemplateRenderer:
    """Renders Jinja2 templates with random-data helpers registered as globals."""

    _LOGICALTYPE_PREFIX = "__logicaltype_"
    _LOGICALTYPE_SUFFIX = "__"

    def __init__(self, data_dir: Path | None = None) -> None:
        self.counters: dict[str, int] = dict()
        self.logical_types: dict[str, str] = dict()
        # State pools: pre-generated pools of values indexed by key
        self.state_pools: dict[str, dict[str, Any]] = dict()
        self.env = jinja2.Environment(
            autoescape=False,
            undefined=jinja2.StrictUndefined,
            keep_trailing_newline=False,
        )
        self.env.globals.update(
            {
                "now": lambda: (
                    f'{{"__logicaltype_iso-8601-timestamp__": '
                    f'"{datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")}"}}'
                ),
                "unix_time_stamp": lambda max_ago: (
                    f'{{"__logicaltype_timestamp-millis__": '
                    f"{int(time.time() * 1000) - random.randint(0, int(max_ago) * 1000)}}}"
                ),
                # Plain UTC strftime — for raw-text templates (e.g. NGINX, syslog
                # lines) that need an arbitrary formatted timestamp rather than the
                # JSON-wrapped logical-type markers emitted by `now()`.
                "strftime": lambda fmt: datetime.now(timezone.utc).strftime(fmt),
                "ip": self._random_ip,
                "guid": lambda: str(uuid.uuid4()),
                "randoms": self._randoms,
                "integer": random.randint,
                "random_string": self._random_string,
                "random_string_vocabulary": self._random_string_vocab,
                "counter": self._counter,
                "floating": self._floating,
                "gaussian": self._gaussian,
                "regex": self._generate_from_regex,
                "data": self._load_data(data_dir) if data_dir else dict(),
                "init_pool": self._init_pool,
                "pool": self._get_from_pool,
                "update_pool": self._update_pool,
                "min": min,
                "max": max,
            }
        )

    @staticmethod
    def _load_data(data_dir: Path) -> Dict[str, list[str]]:
        """Load each file under data_dir as a list of stripped, non-empty,
        non-comment lines. Lines starting with `#` (after stripping) are
        ignored, so files can be self-documented. A file named `known_ports`
        becomes `data.known_ports` in templates."""
        if not data_dir.is_dir():
            return dict()
        loaded: dict[str, list[str]] = dict()
        for path in sorted(data_dir.iterdir()):
            if not path.is_file() or path.name.startswith("."):
                continue
            lines = [
                line.strip() for line in path.read_text(encoding="utf-8").splitlines()
            ]
            loaded[path.name] = [
                line for line in lines if line and not line.startswith("#")
            ]
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

    def render_raw(self, template: jinja2.Template) -> str:
        """Render a compiled template and return the raw string, with no JSON
        parsing or logical-type unwrapping. Used by `--no-schema` mode for
        non-structured payloads like NGINX access logs or syslog lines."""
        return template.render()

    def render_raw_with_key(self, template: jinja2.Template, key_field: str) -> tuple[str, Any]:
        """Render a compiled template and extract a specific variable value.
        
        Uses Jinja2's module compilation to access template variables after rendering.
        
        Returns:
            tuple: (rendered_output, key_value) where key_value is None if not found
        """
        # Compile template as a module to get access to variables
        module = template.make_module()
        
        # The rendered output is in the module
        raw_output = str(module)
        
        # Try to get the key field from the module's namespace
        key_value = getattr(module, key_field, None)
        
        return raw_output, key_value

    def render(
        self,
        template: jinja2.Template,
    ) -> Dict[str, Any]:
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

    def _unwrap_logical_markers(
        self,
        obj: Any,
        parent_key: str = "",
    ) -> Any:
        if isinstance(obj, dict):
            if len(obj) == 1:
                only_key = next(iter(obj))
                if (
                    only_key.startswith(self._LOGICALTYPE_PREFIX)
                    and only_key.endswith(self._LOGICALTYPE_SUFFIX)
                    and len(only_key)
                    > len(self._LOGICALTYPE_PREFIX) + len(self._LOGICALTYPE_SUFFIX)
                ):
                    logical = only_key[
                        len(self._LOGICALTYPE_PREFIX) : -len(self._LOGICALTYPE_SUFFIX)
                    ]
                    if parent_key:
                        self.logical_types[parent_key] = logical
                    return obj[only_key]
            return {
                k: self._unwrap_logical_markers(v, parent_key=k) for k, v in obj.items()
            }
        if isinstance(obj, list):
            return [
                self._unwrap_logical_markers(item, parent_key=parent_key)
                for item in obj
            ]
        return obj

    def _counter(
        self,
        name: str,
        start: int,
        step: int,
    ) -> int:
        current = self.counters.get(name, start)
        self.counters[name] = current + step
        return current

    @staticmethod
    def _floating(
        min_val: float,
        max_val: float,
        decimals: int = 2,
    ) -> float:
        return round(random.uniform(min_val, max_val), decimals)

    @staticmethod
    def _gaussian(
        mean: float,
        std_dev: float,
        decimals: int = 2,
    ) -> float:
        """Generate a random number from a Gaussian (normal) distribution.

        Args:
            mean: The mean (average) of the distribution
            std_dev: The standard deviation of the distribution
            decimals: Number of decimal places to round to (default: 2)

        Returns:
            A random float from the Gaussian distribution
        """
        return round(
            random.gauss(
                mean,
                std_dev,
            ),
            decimals,
        )

    def _random_ip(
        self,
        cidr: str,
    ) -> str:
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

    def _random_string(
        self,
        min_len: int,
        max_len: int,
    ) -> str:
        """Generate a random alphanumeric string."""
        length = random.randint(min_len, max_len)
        return "".join(random.choice(self._ALPHANUM) for _ in range(length))

    def _random_string_vocab(
        self,
        min_len: int,
        max_len: int,
        vocab: str,
    ) -> str:
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

    def _init_pool(self, pool_name: str, pool_data: dict) -> str:
        """Initialize a state pool with pre-generated data.

        Only initializes the pool if it doesn't already exist, ensuring
        consistent data across multiple template renders.

        Args:
            pool_name: Name of the pool (e.g., 'devices')
            pool_data: Dictionary mapping keys to their attributes

        Returns:
            Empty string (for use in templates without output)

        Example in template:
            {%- set _ = init_pool('devices', {
                'IOT-DEV-001': {'location': 'Building 1', 'ip': '10.20.1.5'},
                'IOT-DEV-002': {'location': 'Building 2', 'ip': '10.20.1.6'}
            }) %}
        """
        # Only initialize if the pool doesn't exist yet
        if pool_name not in self.state_pools:
            self.state_pools[pool_name] = pool_data
            logger.debug(
                "Initialized pool '%s' with %d entries", pool_name, len(pool_data)
            )
        return ""

    def _get_from_pool(
        self, pool_name: str, key: str, field: str, default: Any = None
    ) -> Any:
        """Retrieve a value from a state pool.

        Args:
            pool_name: Name of the pool (e.g., 'devices')
            key: The key to look up (e.g., device_id)
            field: The field to retrieve (e.g., 'location', 'ip_address')
            default: Default value if key or field not found

        Returns:
            The stored value or default

        Example in template:
            "location": {{ pool('devices', device_id, 'location', 'Unknown') | tojson }}
        """
        if pool_name not in self.state_pools:
            return default

        pool = self.state_pools[pool_name]
        if key not in pool:
            return default

        return pool[key].get(field, default)

    def _update_pool(self, pool_name: str, key: str, field: str, value: Any) -> str:
        """Update a value in a state pool.

        This enables "random walk" patterns where each reading becomes the new
        baseline for the next reading, creating realistic time-series drift.

        Args:
            pool_name: Name of the pool (e.g., 'devices')
            key: The key to update (e.g., device_id)
            field: The field to update (e.g., 'baseline_temperature')
            value: The new value to store

        Returns:
            Empty string (for use in templates without output)

        Example in template:
            {%- set new_temp = pool('devices', device_id, 'baseline_temperature', 22.0) + gaussian(0.0, 0.5, 2) -%}
            {%- set _ = update_pool('devices', device_id, 'baseline_temperature', new_temp) -%}
            "temperature_celsius": {{ new_temp | round(2) }}
        """
        if pool_name not in self.state_pools:
            logger.warning("Attempted to update non-existent pool '%s'", pool_name)
            return ""

        pool = self.state_pools[pool_name]
        if key not in pool:
            logger.warning(
                "Attempted to update non-existent key '%s' in pool '%s'", key, pool_name
            )
            return ""

        pool[key][field] = value
        return ""


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

    logical_types = logical_types or dict()
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
        if value is None:
            # A field rendered as JSON `null` (e.g. via a conditional template
            # branch). Make it a nullable string union; the field builder adds
            # a `null` default so the field is also safe to omit.
            return ["null", "string"]
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
            fields = [_build_field(k, v) for k, v in value.items()]
            return {"type": "record", "name": record_name, "fields": fields}
        if isinstance(value, list):
            # Suffix the element's logical name so an array<dict> and a sibling
            # dict with the same field name don't collide on record names.
            element_field = f"{field_name}_item" if field_name else "item"
            if value:
                return {
                    "type": "array",
                    "items": infer_type(value=value[0], field_name=element_field),
                }
            return {"type": "array", "items": "string"}
        return "string"

    def _build_field(name: str, value: Any) -> dict[str, Any]:
        """Build a record field, adding a `null` default for nullable unions
        so the field can also be safely omitted by later records."""
        field_type = infer_type(value=value, field_name=name)
        field: dict[str, Any] = {"name": name, "type": field_type}
        if isinstance(field_type, list) and field_type and field_type[0] == "null":
            field["default"] = None
        return field

    fields: list[Any] = [_build_field(key, value) for key, value in data.items()]

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

    Sampling reuses the live renderer, so stateful helpers (`counter`, state
    pools) would otherwise be advanced by `num_samples` before the first real
    record is produced. We snapshot and restore the counters so, e.g., a
    `counter("seq", 1, 1)` still starts at 1 in production.
    """
    counters_snapshot = dict(renderer.counters)
    samples = [renderer.render(template=template) for _ in range(num_samples)]
    renderer.counters = counters_snapshot
    merged = dict(samples[0])

    # If a top-level field is an empty list in the first sample but populated
    # later, prefer the populated version.
    for sample in samples[1:]:
        for key, value in sample.items():
            if (
                isinstance(value, list)
                and value
                and (
                    key not in merged
                    or (isinstance(merged[key], list) and not merged[key])
                )
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
    config: dict[str, str] = dict()
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
    retention_ms: int = DEFAULT_RETENTION_MS,
) -> None:
    """Create the topic if it doesn't already exist."""
    admin_config = dict(kafka_config)
    admin_config.setdefault("bootstrap.servers", "localhost:9092")
    admin_config.setdefault("security.protocol", "PLAINTEXT")

    admin_client = AdminClient(admin_config)

    # Fetching metadata is the first real contact with the broker — surface an
    # unreachable/misconfigured broker as a clear message instead of a raw
    # KafkaException traceback.
    try:
        metadata = admin_client.list_topics(timeout=10)
    except Exception as e:
        logger.error(
            "Could not reach Kafka at '%s': %s",
            admin_config.get("bootstrap.servers"),
            e,
        )
        sys.exit(1)
    if topic in metadata.topics:
        logger.info("Topic '%s' already exists", topic)
        return

    new_topic = NewTopic(
        topic,
        num_partitions=partitions,
        replication_factor=replication,
        config={"retention.ms": str(retention_ms)},
    )
    futures: Dict[str, Future] = admin_client.create_topics([new_topic])

    for topic_name, future in futures.items():
        try:
            future.result()
            logger.info("Topic '%s' created successfully", topic_name)
        except Exception as e:
            # Don't fall through to producing into a topic that doesn't exist —
            # every send would silently fail in the delivery callback.
            logger.error("Failed to create topic '%s': %s", topic_name, e)
            sys.exit(1)


def create_producer(
    kafka_config: Dict[str, str],
    acks: str = "all",
    message_timeout_ms: int = 60000,
    linger_ms: int = 5,
) -> Producer:
    """Create a Kafka producer with defaults and an error callback for visibility."""
    producer_config: dict[str, Any] = dict(kafka_config)
    producer_config.setdefault("bootstrap.servers", "localhost:9092")
    producer_config.setdefault("security.protocol", "PLAINTEXT")
    producer_config.setdefault("acks", acks)
    producer_config.setdefault("message.timeout.ms", message_timeout_ms)
    producer_config.setdefault("linger.ms", linger_ms)
    # Surface broker-side issues (DNS failure, auth, broker down).
    producer_config["error_cb"] = lambda err: logger.error(
        "Kafka producer error: %s", err
    )

    return Producer(producer_config)


_delivery_failures: dict[str, int] = dict()


def delivery_report(err, msg) -> None:
    """Delivery callback — logs only on failure to avoid flooding stdout."""
    if err is not None:
        key = str(err)
        _delivery_failures[key] = _delivery_failures.get(key, 0) + 1
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

    parser = argparse.ArgumentParser(
        description="SIEM Data Producer for Kafka with Avro"
    )
    parser.add_argument("template", help="Template name (without .j2 extension)")
    parser.add_argument(
        "-t", "--topic", help="Kafka topic (not required in dry-run mode)"
    )
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
        "--schema",
        help="Use an existing AVRO schema other than inferring it based on the template",
    )
    parser.add_argument(
        "-ns",
        "--namespace",
        default="io.confluent.siem",
        help="Avro schema namespace, ignored when a schema is set (default: io.confluent.siem)",
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
        "--retention-ms",
        type=int,
        default=DEFAULT_RETENTION_MS,
        help="retention.ms set when creating the topic (default: 1 day = "
        "86400000). Ignored if the topic already exists.",
    )
    parser.add_argument(
        "-s",
        "--schema-id-location",
        choices=["headers", "body"],
        default="body",
        help=(
            "Where to place the Avro schema ID. 'headers' (modern) "
            "stores it in the Kafka message headers; 'body' (legacy) prefixes "
            "it to the serialized value with the 5-byte magic-byte framing."
        ),
    )
    parser.add_argument(
        "-k",
        "--key",
        default=None,
        help=(
            "Field whose value is used as the Kafka message key. "
            "For JSON (default): top-level field (must be scalar: string, int, float, or bool). "
            "For raw text (--no-schema): field name to extract from key=value format. "
            "The field remains in the value payload. Default: no key (null)."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Generate and display data without producing to Kafka",
    )
    parser.add_argument(
        "--inferred-schema",
        action="store_true",
        help="Display the inferred AVRO Schema",
    )
    parser.add_argument(
        "--acks",
        default="all",
        help="Producer acks setting (default: all)",
    )
    parser.add_argument(
        "--message-timeout-ms",
        type=int,
        default=120000,
        help="Message delivery timeout in ms (default: 60000)",
    )
    parser.add_argument(
        "--linger-ms",
        type=int,
        default=100,
        help="Linger time in ms before sending a batch (default: 5)",
    )
    parser.add_argument(
        "--no-schema",
        action="store_true",
        help=(
            "Treat the rendered template as raw UTF-8 text (e.g. NGINX access "
            "logs, syslog lines) instead of JSON. Skips Avro serialization and "
            "Schema Registry entirely. Mutually exclusive with --schema, "
            "--inferred-schema, and -k/--key."
        ),
    )

    args = parser.parse_args()

    if args.batch_size < 1:
        parser.error("-b/--batch-size must be >= 1")
    if args.num_records < 0:
        parser.error("-n/--num-records must be >= 0 (0 = continuous)")
    if args.frequency < 0:
        parser.error("-f/--frequency must be >= 0")

    if args.no_schema:
        conflicts = list()
        if args.schema:
            conflicts.append("--schema")
        if args.inferred_schema:
            conflicts.append("--inferred-schema")
        if conflicts:
            parser.error(f"--no-schema cannot be combined with: {', '.join(conflicts)}")

    if not (args.dry_run or args.inferred_schema) and not args.topic:
        parser.error(
            "the following arguments are required: -t/--topic (not required with --dry-run or --inferred-schema)"
        )

    template_path = Path(args.templates_dir) / f"{args.template}.j2"
    if not template_path.exists():
        logger.error("Template not found: %s", template_path)
        sys.exit(1)

    with open(template_path, "r", encoding="utf-8") as f:
        template_content = f.read()

    renderer = TemplateRenderer(data_dir=Path(args.templates_dir) / "data")
    template = renderer.compile(template_content)

    sample_data: Dict[str, Any] = dict()
    avro_schema_str = ""

    if not args.no_schema:
        # Generate several samples to infer schema — avoids pinning empty-list
        # fields to array<string> when later records would carry real items.
        sample_data = sample_for_schema(renderer=renderer, template=template)

        if args.schema:
            schema_path = Path(args.schema)
            if not schema_path.exists():
                logger.error("Schema file not found: %s", schema_path)
                sys.exit(1)
            with open(schema_path, "r", encoding="utf-8") as f:
                raw_schema = f.read()
            try:
                avro_schema_str = json.dumps(json.loads(raw_schema))
            except json.JSONDecodeError as e:
                logger.error("Invalid JSON in schema file %s: %s", schema_path, e)
                sys.exit(1)
        else:
            avro_schema_str = infer_avro_schema(
                data=sample_data,
                name=args.template,
                namespace=args.namespace,
                logical_types=renderer.logical_types,
            )

        if args.inferred_schema:
            source = (
                f"file {args.schema}" if args.schema else f"template {args.template}"
            )
            logger.info("Schema from %s:", source)
            print(json.dumps(json.loads(avro_schema_str), indent=2))
            print()

    # Dry run mode - just generate and display data
    if args.dry_run:
        num_samples = args.num_records if args.num_records > 0 else 10
        mode_label = "raw text" if args.no_schema else "sample"
        logger.info(
            f"Dry run mode - generating {num_samples} {mode_label} records from "
            f"template '{args.template}':"
        )
        for i in range(num_samples):
            logger.info(f"Record {i + 1}:")
            if args.no_schema:
                print(renderer.render_raw(template=template))
            else:
                print(json.dumps(renderer.render(template=template), indent=2))
            print()

    if args.dry_run or args.inferred_schema:
        sys.exit(0)

    kafka_config = load_config(args.kafka_config)

    avro_serializer = None
    if not args.no_schema:
        registry_config = load_config(args.registry_config)

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

        if not args.schema:
            logger.info(
                "Inferred Avro Schema:\n%s",
                json.dumps(json.loads(avro_schema_str), indent=2),
            )

        schema_registry_conf = {
            "url": registry_config.get("schemaRegistryURL", "http://localhost:8081")
        }
        if registry_config.get("basic.auth.user.info"):
            schema_registry_conf["basic.auth.user.info"] = registry_config[
                "basic.auth.user.info"
            ]

        ca = registry_config.get("ssl.ca.location")
        if ca:
            schema_registry_conf["ssl.ca.location"] = ca

        schema_registry_client = SchemaRegistryClient(schema_registry_conf)
        schema_id_serializer = (
            header_schema_id_serializer
            if args.schema_id_location == "headers"
            else prefix_schema_id_serializer
        )
        serializer_conf: dict[str, Any] = {
            "schema.id.serializer": schema_id_serializer
        }
        # Honor auto.register.schemas from registry.properties (it's an
        # AvroSerializer option, not a SchemaRegistryClient one). Accepts the
        # usual truthy spellings; defaults to the SDK behaviour (True) when
        # unset.
        if "auto.register.schemas" in registry_config:
            serializer_conf["auto.register.schemas"] = registry_config[
                "auto.register.schemas"
            ].strip().lower() in ("true", "1", "yes")
        avro_serializer = AvroSerializer(
            schema_registry_client,
            avro_schema_str,
            lambda obj, ctx: obj,  # obj is already a dict
            conf=serializer_conf,
        )
        logger.info("Schema ID location: %s", args.schema_id_location)
    else:
        logger.info("Schema mode: --no-schema (raw UTF-8, no Schema Registry)")

    logger.info("Checking/creating topic '%s'...", args.topic)
    create_topic_if_not_exists(
        kafka_config,
        topic=args.topic,
        partitions=args.partitions,
        retention_ms=args.retention_ms,
    )

    producer = create_producer(
        kafka_config,
        acks=args.acks,
        message_timeout_ms=args.message_timeout_ms,
        linger_ms=args.linger_ms,
    )

    logger.info(
        "Producing to topic '%s' with frequency %ss", args.topic, args.frequency
    )
    if args.num_records > 0:
        logger.info("Total records: %d", args.num_records)
    else:
        logger.info("Mode: Continuous (press Ctrl+C to stop)")

    # If this many records fail back-to-back (render, serialize, or produce),
    # give up rather than spin forever — in `-n` mode persistent failures would
    # otherwise never let `count` reach the target and the loop would never exit.
    MAX_CONSECUTIVE_FAILURES = 100

    try:
        count = 0
        consecutive_failures = 0
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
                headers: list = list()
                message_key = None

                if args.no_schema:
                    try:
                        if args.key:
                            # Render with key extraction
                            raw_text, key_value = renderer.render_raw_with_key(
                                template=template,
                                key_field=args.key
                            )
                            serialized_value = raw_text.encode("utf-8")
                            
                            # Use extracted key if found and is scalar
                            if key_value is not None:
                                if isinstance(key_value, (str, int, float, bool)):
                                    message_key = str(key_value).encode("utf-8")
                                else:
                                    logger.warning(
                                        "Key field '%s' is not a scalar type (got %s); using null key",
                                        args.key,
                                        type(key_value).__name__
                                    )
                            else:
                                logger.warning(
                                    "Key field '%s' not found in template; using null key",
                                    args.key
                                )
                        else:
                            # No key needed, use simple render
                            serialized_value = renderer.render_raw(template=template).encode("utf-8")
                    except Exception as e:
                        logger.error("Error rendering template: %s", e)
                        consecutive_failures += 1
                        continue
                else:
                    # Rendering can raise (e.g. a random field combination that
                    # produces invalid JSON, or a helper that throws). Catch it
                    # so a single bad render is skipped rather than crashing a
                    # long-running producer — same contract as the raw path.
                    try:
                        data = renderer.render(template=template)
                    except Exception as e:
                        logger.error("Error rendering template: %s", e)
                        consecutive_failures += 1
                        continue
                    try:
                        serialized_value = avro_serializer(
                            data,
                            SerializationContext(
                                args.topic, MessageField.VALUE, headers
                            ),
                        )
                    except Exception as e:
                        logger.error("Error serializing message: %s", e)
                        consecutive_failures += 1
                        continue

                    # Per-record key validation. Startup only validates the first
                    # sample; a template with conditional fields could omit the key
                    # field on later renders, which would otherwise silently
                    # produce the string "None" as the partition key.
                    if args.key:
                        key_value = data.get(args.key)
                        if not isinstance(key_value, (str, int, float, bool)):
                            logger.error(
                                "Key field '%s' is missing or non-scalar in this "
                                "record (got %s); skipping",
                                args.key,
                                type(key_value).__name__,
                            )
                            consecutive_failures += 1
                            continue
                        message_key = str(key_value).encode("utf-8")

                # Produce, retrying on BufferError so a transient full queue
                # doesn't silently drop records. Drop after MAX_RETRIES so a
                # permanently broken producer can't wedge the loop.
                MAX_RETRIES = 5
                produced = False
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
                        produced = True
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

                consecutive_failures = 0 if produced else consecutive_failures + 1

            if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                logger.error(
                    "Aborting after %d consecutive failures — check broker / "
                    "Schema Registry connectivity and template output",
                    consecutive_failures,
                )
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
        total_failed = sum(_delivery_failures.values())
        logger.info("Total records produced: %d  |  delivery failures: %d", count, total_failed)
        if _delivery_failures:
            for reason, n in sorted(_delivery_failures.items(), key=lambda x: -x[1]):
                logger.warning("  %dx %s", n, reason)


if __name__ == "__main__":
    main()
