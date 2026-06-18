"""Shared helpers and tunables for the SIEM demo streaming apps.

These were duplicated verbatim across `dns_streaming_app.py`,
`fortigate_streaming_app.py`, `paloalto_streaming_app.py`,
`windows_streaming_app.py` and (partly) `dns_quix_app.py`. They live here so
there's a single definition of the Kafka/Schema-Registry plumbing and the
topic-creation tunables.

Apps run as scripts from the `demo/` directory, so `import utils` / `from utils
import ...` resolves to this package (the script's own directory is on
`sys.path`).
"""

import os
import logging

from confluent_kafka.admin import NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient

# ── Shared tunables ───────────────────────────────────────────────────────────
NUM_PARTITIONS = 1
REPLICATION_FACTOR = 1
DEFAULT_RETENTION_MS = 86400000  # retention.ms for topics we create (1 day)
AUTO_OFFSET_RESET = "earliest"
POLL_TIMEOUT = 1.0
ADMIN_OP_TIMEOUT = 30.0
# Schemas live in demo/schemas; this file is demo/utils/__init__.py, so go up
# one level from the package directory.
DEFAULT_SCHEMA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "schemas")

# Module logger. The shared log format carries no logger name, so a single
# logger here renders identically to each app's own named logger.
logger = logging.getLogger("siem-demo")


def setup_logging(name):
    """Configure the root logger with the shared format and return a named logger."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    return logging.getLogger(name)


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
    client_conf = {
        "url": sr_conf["schemaRegistryURL"],
    }
    if sr_conf.get("basic.auth.user.info"):
        client_conf["basic.auth.user.info"] = sr_conf["basic.auth.user.info"]
    ca = sr_conf.get("ssl.ca.location") or kafka_conf.get("ssl.ca.location")
    if ca:
        client_conf["ssl.ca.location"] = ca
    return SchemaRegistryClient(client_conf)


def ensure_topics(
    admin,
    topics,
    retention_ms=DEFAULT_RETENTION_MS,
):
    """Create any missing topics with NUM_PARTITIONS partitions and retention_ms."""
    existing = set(admin.list_topics(timeout=ADMIN_OP_TIMEOUT).topics.keys())
    to_create = [
        NewTopic(
            t,
            num_partitions=NUM_PARTITIONS,
            replication_factor=REPLICATION_FACTOR,
            config={
                "retention.ms": str(retention_ms),
            },
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
