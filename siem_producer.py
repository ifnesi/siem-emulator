#!/usr/bin/env python3
"""
SIEM Data Producer for Kafka with Avro Serialization
Produces data from templates to Kafka topics with automatic Avro schema inference
"""

import re
import sys
import json
import time
import random
import argparse

from typing import Dict, Any
from pathlib import Path
from datetime import datetime, timezone
from argparse import ArgumentParser, Namespace

from confluent_kafka.schema_registry._sync.avro import AvroSerializer
from confluent_kafka.schema_registry._sync.schema_registry_client import (
    SchemaRegistryClient,
)
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient
from confluent_kafka.cimpl import NewTopic
from concurrent.futures._base import Future
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.admin._metadata import ClusterMetadata
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer


class TemplateRenderer:
    """Renders templates with random data"""

    # Common ports and protocols
    KNOWN_PORTS: list[int] = [
        20,
        21,
        22,
        23,
        25,
        53,
        80,
        110,
        143,
        443,
        445,
        3306,
        3389,
        5432,
        8080,
        8443,
    ]
    KNOWN_PROTOCOLS: list[str] = [
        "HTTP",
        "HTTPS",
        "FTP",
        "SSH",
        "SMTP",
        "DNS",
        "TELNET",
        "IMAP",
        "POP3",
        "SMB",
        "MySQL",
        "PostgreSQL",
        "RDP",
    ]

    def __init__(self) -> None:
        self.ip_ranges = {}

    def render(
        self,
        template: str,
    ) -> Dict[str, Any]:
        """Render a template string to a dictionary"""
        # Replace template functions with actual values
        rendered: str = template

        # {{now}} - current timestamp
        rendered: str = re.sub(
            r"\{\{now\}\}",
            lambda m: datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
            rendered,
        )

        # {{unix_time_stamp seconds}} - Unix timestamp (seconds ago)
        rendered: str = re.sub(
            r"\{\{unix_time_stamp (\d+)\}\}",
            lambda m: str(int(time.time()) - random.randint(a=0, b=int(m.group(1)))),
            rendered,
        )

        # {{ip_known_port}} - Random known port
        rendered: str = re.sub(
            r"\{\{ip_known_port\}\}",
            lambda m: str(random.choice(seq=self.KNOWN_PORTS)),
            rendered,
        )

        # {{ip_known_protocol}} - Random known protocol
        rendered: str = re.sub(
            r"\{\{ip_known_protocol\}\}",
            lambda m: random.choice(seq=self.KNOWN_PROTOCOLS),
            rendered,
        )

        # {{random_v_from_list "list_name"}} - Random value from a list (simplified to random IP)
        rendered: str = re.sub(
            r'\{\{random_v_from_list "([^"]+)"\}\}',
            lambda m: self._random_ip(cidr="10.0.0.0/8"),
            rendered,
        )

        # {{ip "CIDR"}} - random IP from CIDR
        rendered: str = re.sub(
            r'\{\{ip "([^"]+)"\}\}',
            lambda m: self._random_ip(cidr=m.group(1)),
            rendered,
        )

        # {{randoms "opt1|opt2|opt3"}} - random choice
        rendered: str = re.sub(
            r'\{\{randoms "([^"]+)"\}\}',
            lambda m: random.choice(m.group(1).split(sep="|")),
            rendered,
        )

        # {{integer min max}} - random integer
        rendered: str = re.sub(
            r"\{\{integer (\d+) (\d+)\}\}",
            lambda m: str(random.randint(a=int(m.group(1)), b=int(m.group(2)))),
            rendered,
        )

        # {{random_string min max}} - random string
        rendered: str = re.sub(
            r"\{\{random_string (\d+) (\d+)\}\}",
            lambda m: self._random_string(
                min_len=int(m.group(1)), max_len=int(m.group(2))
            ),
            rendered,
        )

        # {{random_string_vocabulary min max "chars"}} - random string from vocabulary
        rendered: str = re.sub(
            r'\{\{random_string_vocabulary (\d+) (\d+) "([^"]+)"\}\}',
            lambda m: self._random_string_vocab(
                min_len=int(m.group(1)),
                max_len=int(m.group(2)),
                vocab=m.group(3),
            ),
            rendered,
        )

        # {{counter "name" start step}} - counter (simplified to random for now)
        rendered: str = re.sub(
            r'\{\{counter "([^"]+)" (\d+) (\d+)\s*\}\}',
            lambda m: str(
                random.randint(
                    a=int(m.group(2)), b=int(m.group(2)) + int(m.group(3)) * 100
                )
            ),
            rendered,
        )

        # {{floating min max}} or {{floating min max decimals}} - random floating point number
        def floating_replacer(m):
            min_val = float(m.group(1))
            max_val = float(m.group(2))
            decimals = int(m.group(3)) if m.group(3) else 2  # Default to 2 decimal places
            return str(round(random.uniform(min_val, max_val), decimals))
        
        rendered: str = re.sub(
            r'\{\{floating (\d+(?:\.\d+)?) (\d+(?:\.\d+)?)(?:\s+(\d+))?\}\}',
            floating_replacer,
            rendered,
        )

        # {{regex "pattern"}} - generate random string matching regex pattern
        def regex_replacer(m):
            pattern = m.group(1)
            result = self._generate_from_regex(pattern=pattern)
            return result
        
        rendered: str = re.sub(
            r'\{\{regex "([^"]+)"\}\}',
            regex_replacer,
            rendered,
        )

        # Parse as JSON
        try:
            return json.loads(s=rendered)
        except json.JSONDecodeError as e:
            print(f"Error parsing rendered template: {e}")
            print(f"Rendered content: {rendered}")
            raise

    def _random_ip(
        self,
        cidr: str,
    ) -> str:
        """Generate random IP from CIDR notation"""
        if "/" not in cidr:
            return cidr

        network, prefix = cidr.split(sep="/")
        prefix: int = int(prefix)

        # Parse network address
        octets: list[int] = [int(x) for x in network.split(sep=".")]

        # Calculate how many bits are available for host addresses
        host_bits: int = 32 - prefix

        # Generate random host part
        random_host: int = random.randint(a=0, b=(2**host_bits) - 1)

        # Apply to network address
        ip_int: int = (
            (octets[0] << 24) + (octets[1] << 16) + (octets[2] << 8) + octets[3]
        )
        ip_int: int = (ip_int & (0xFFFFFFFF << host_bits)) | random_host

        # Convert back to dotted notation
        return f"{(ip_int >> 24) & 0xFF}.{(ip_int >> 16) & 0xFF}.{(ip_int >> 8) & 0xFF}.{ip_int & 0xFF}"

    def _random_string(
        self,
        min_len: int,
        max_len: int,
    ) -> str:
        """Generate random alphanumeric string"""
        length: int = random.randint(a=min_len, b=max_len)
        chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
        return "".join(random.choice(seq=chars) for _ in range(length))

    def _random_string_vocab(
        self,
        min_len: int,
        max_len: int,
        vocab: str,
    ) -> str:
        """Generate random string from vocabulary"""
        length: int = random.randint(a=min_len, b=max_len)
        return "".join(random.choice(seq=vocab) for _ in range(length))

    def _generate_from_regex(
        self,
        pattern: str,
    ) -> str:
        """Generate random string matching a regex pattern
        
        Supports common regex patterns:
        - \\d: digit (0-9)
        - \\w: word character (a-z, A-Z, 0-9, _)
        - [a-z], [A-Z], [0-9]: character classes
        - {n}: exact repetition
        - {n,m}: repetition range
        - .: any character (generates alphanumeric)
        """
        result = []
        i = 0
        
        while i < len(pattern):
            char_to_add = None
            char_generator = None  # Function to generate a new character
            chars_consumed = 0
            
            # Handle escape sequences (including double-escaped from template)
            if pattern[i:i+2] == '\\\\' and i + 2 < len(pattern):
                # Double backslash from template file (e.g., \\d in template becomes \\\\d in string)
                next_char = pattern[i + 2]
                if next_char == 'd':
                    # Digit
                    char_generator = lambda: str(random.randint(0, 9))
                    chars_consumed = 3
                elif next_char == 'w':
                    # Word character
                    char_generator = lambda: random.choice('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_')
                    chars_consumed = 3
                elif next_char == 's':
                    # Whitespace
                    char_to_add = ' '
                    chars_consumed = 3
                elif next_char == '(':
                    # Literal opening paren
                    char_to_add = '('
                    chars_consumed = 3
                elif next_char == ')':
                    # Literal closing paren
                    char_to_add = ')'
                    chars_consumed = 3
                else:
                    # Literal escaped character
                    char_to_add = next_char
                    chars_consumed = 3
            elif pattern[i] == '\\' and i + 1 < len(pattern):
                # Single backslash (for direct usage)
                next_char = pattern[i + 1]
                if next_char == 'd':
                    # Digit
                    char_generator = lambda: str(random.randint(0, 9))
                    chars_consumed = 2
                elif next_char == 'w':
                    # Word character
                    char_generator = lambda: random.choice('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_')
                    chars_consumed = 2
                elif next_char == 's':
                    # Whitespace
                    char_to_add = ' '
                    chars_consumed = 2
                else:
                    # Literal escaped character
                    char_to_add = next_char
                    chars_consumed = 2
            
            # Handle character classes [...]
            elif pattern[i] == '[':
                end = pattern.find(']', i)
                if end == -1:
                    char_to_add = pattern[i]
                    chars_consumed = 1
                else:
                    char_class = pattern[i+1:end]
                    # Handle ranges like a-z, A-Z, 0-9
                    chars = []
                    j = 0
                    while j < len(char_class):
                        if j + 2 < len(char_class) and char_class[j + 1] == '-':
                            # Range
                            start_char = ord(char_class[j])
                            end_char = ord(char_class[j + 2])
                            chars.extend(chr(c) for c in range(start_char, end_char + 1))
                            j += 3
                        else:
                            chars.append(char_class[j])
                            j += 1
                    
                    if chars:
                        # Create a generator that picks from this character class
                        char_generator = lambda chars_list=chars: random.choice(chars_list)
                    chars_consumed = end - i + 1
            
            # Handle dot (any character)
            elif pattern[i] == '.':
                char_generator = lambda: random.choice('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789')
                chars_consumed = 1
            
            # Skip repetition markers - they're handled below
            elif pattern[i] == '{':
                i += 1
                continue
            
            # Literal character
            else:
                char_to_add = pattern[i]
                chars_consumed = 1
            
            # Now check if next character is a repetition marker
            if char_to_add is not None or char_generator is not None:
                next_pos = i + chars_consumed
                if next_pos < len(pattern) and pattern[next_pos] == '{':
                    # Find the closing brace
                    end = pattern.find('}', next_pos)
                    if end != -1:
                        repetition = pattern[next_pos+1:end]
                        if ',' in repetition:
                            # Range {n,m}
                            parts = repetition.split(',')
                            min_rep = int(parts[0]) if parts[0] else 0
                            max_rep = int(parts[1]) if parts[1] else min_rep + 10
                            count = random.randint(min_rep, max_rep)
                        else:
                            # Exact {n}
                            count = int(repetition)
                        
                        # Generate multiple characters
                        if char_generator:
                            # Generate a new random character for each repetition
                            result.append(''.join(char_generator() for _ in range(count)))
                        else:
                            # Repeat the literal character
                            result.append(char_to_add * count)
                        i = end + 1
                        continue
                
                # No repetition, just add the character
                if char_generator:
                    result.append(char_generator())
                else:
                    result.append(char_to_add)
                i += chars_consumed
            else:
                i += 1
        
        return ''.join(result)


def infer_avro_schema(
    data: Dict[str, Any],
    name: str,
) -> str:
    """Infer Avro schema from a data dictionary"""

    used_names: set[Any] = set()

    def infer_type(
        value: Any,
        field_name: str = "",
    ) -> Dict[str, Any]:
        """Infer Avro type from Python value"""
        if isinstance(value, bool):
            return "boolean"
        elif isinstance(value, int):
            return "long" if abs(value) > 2147483647 else "int"
        elif isinstance(value, float):
            return "double"
        elif isinstance(value, str):
            return "string"
        elif isinstance(value, dict):
            # Use field name to create unique nested record names
            base_name: str = field_name.capitalize() if field_name else "Nested"
            nested_name: str = base_name
            counter = 1
            while nested_name in used_names:
                nested_name: str = f"{base_name}{counter}"
                counter += 1
            used_names.add(nested_name)

            fields: list[Any] = []
            for k, v in value.items():
                field_type: Dict[str, Any] = infer_type(value=v, field_name=k)
                fields.append({"name": k, "type": field_type})
            return {"type": "record", "name": nested_name, "fields": fields}
        elif isinstance(value, list):
            if value:
                return {"type": "array", "items": infer_type(value[0], field_name)}
            return {"type": "array", "items": "string"}
        else:
            return "string"

    fields: list[Any] = []
    for key, value in data.items():
        field_type: Dict[str, Any] = infer_type(value, field_name=key)
        fields.append({"name": key, "type": field_type})

    schema: dict[str, Any] = {
        "type": "record",
        "name": name.replace("_", "").capitalize() + "Record",
        "namespace": "com.example.siem",
        "fields": fields,
    }

    return json.dumps(obj=schema)


def load_config(config_file: str) -> Dict[str, str]:
    """Load configuration from properties file"""
    config: dict[Any, Any] = {}
    with open(file=config_file, mode="r", encoding="utf-8") as f:
        for line in f:
            line: str = line.strip()
            if line and not line.startswith("#"):
                if "=" in line:
                    key, value = line.split(sep="=", maxsplit=1)
                    config[key.strip()] = value.strip()
    return config


def create_topic_if_not_exists(
    kafka_config: Dict[str, str],
    topic: str,
    partitions: int = 1,
    replication: int = 1,
) -> None:
    """Create topic if it doesn't exist"""
    # Use all properties from kafka_config
    admin_config: dict[str, str] = dict(kafka_config)

    # Set defaults if not provided
    if "bootstrap.servers" not in admin_config:
        admin_config["bootstrap.servers"] = "localhost:9092"
    if "security.protocol" not in admin_config:
        admin_config["security.protocol"] = "PLAINTEXT"

    admin_client: AdminClient = AdminClient(conf=admin_config)

    # Check if topic exists
    metadata: ClusterMetadata = admin_client.list_topics(timeout=10)
    if topic in metadata.topics:
        print(f"Topic '{topic}' already exists")
        return

    # Create topic
    new_topic: NewTopic = NewTopic(
        topic, num_partitions=partitions, replication_factor=replication
    )
    fs: Dict[str, Future[Any]] = admin_client.create_topics(new_topics=[new_topic])

    # Wait for operation to complete
    for topic_name, f in fs.items():
        try:
            f.result()  # The result itself is None
            print(f"Topic '{topic_name}' created successfully")
        except Exception as e:
            print(f"Failed to create topic '{topic_name}': {e}")


def create_producer(kafka_config: Dict[str, str]) -> Producer:
    """Create Kafka producer"""
    # Use all properties from kafka_config
    producer_config: dict[str, str] = dict(kafka_config)

    # Set defaults if not provided
    if "bootstrap.servers" not in producer_config:
        producer_config["bootstrap.servers"] = "localhost:9092"
    if "security.protocol" not in producer_config:
        producer_config["security.protocol"] = "PLAINTEXT"

    return Producer(producer_config)


def delivery_report(err, msg) -> None:
    """Delivery callback"""
    if err is not None:
        print(f"Message delivery failed: {err}", file=sys.stderr)
    else:
        print(
            f"Message delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}"
        )


def main() -> None:
    parser: ArgumentParser = argparse.ArgumentParser(
        description="SIEM Data Producer for Kafka with Avro"
    )
    parser.add_argument("template", help="Template name (without .tpl extension)")
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
        "--dry-run",
        action="store_true",
        help="Generate and display data without producing to Kafka",
    )

    args: Namespace = parser.parse_args()

    # Validate topic is provided when not in dry-run mode
    if not args.dry_run and not args.topic:
        parser.error("the following arguments are required: -t/--topic (not required with --dry-run)")

    # Load template
    template_path: Path = Path(args.templates_dir) / f"{args.template}.tpl"
    if not template_path.exists():
        print(f"Error: Template not found: {template_path}", file=sys.stderr)
        sys.exit(1)

    with open(file=template_path, mode="r", encoding="utf-8") as f:
        template_content: str = f.read()

    # Initialize renderer
    renderer: TemplateRenderer = TemplateRenderer()

    # Dry run mode - just generate and display data
    if args.dry_run:
        print(f"Dry run mode - generating {args.num_records if args.num_records > 0 else 10} sample records from template '{args.template}':\n")
        num_samples = args.num_records if args.num_records > 0 else 10
        for i in range(num_samples):
            data: Dict[str, Any] = renderer.render(template=template_content)
            print(f"Record {i + 1}:")
            print(json.dumps(obj=data, indent=2))
            print()
        sys.exit(0)

    # Load configurations
    kafka_config: Dict[str, str] = load_config(config_file=args.kafka_config)
    registry_config: Dict[str, str] = load_config(config_file=args.registry_config)

    # Load template
    template_path: Path = Path(args.templates_dir) / f"{args.template}.tpl"
    if not template_path.exists():
        print(f"Error: Template not found: {template_path}", file=sys.stderr)
        sys.exit(1)

    with open(file=template_path, mode="r") as f:
        template_content: str = f.read()

    # Initialize renderer
    renderer: TemplateRenderer = TemplateRenderer()

    # Generate sample data to infer schema
    sample_data: Dict[str, Any] = renderer.render(template=template_content)
    avro_schema_str: str = infer_avro_schema(data=sample_data, name=args.template)

    print(
        f"Inferred Avro Schema:\n{json.dumps(obj=json.loads(avro_schema_str), indent=2)}\n"
    )

    # Create Schema Registry client
    schema_registry_conf: dict[str, str] = {
        "url": registry_config.get("schemaRegistryURL", "http://localhost:8081")
    }

    # Add optional authentication (basic.auth.user.info format: username:password)
    if "basic.auth.user.info" in registry_config and registry_config["basic.auth.user.info"]:
        schema_registry_conf["basic.auth.user.info"] = registry_config["basic.auth.user.info"]

    schema_registry_client: SchemaRegistryClient = SchemaRegistryClient(
        conf=schema_registry_conf
    )

    # Create Avro serializer
    avro_serializer: AvroSerializer = AvroSerializer(
        schema_registry_client,
        avro_schema_str,
        lambda obj, ctx: obj,  # obj is already a dict
    )

    # Create topic if it doesn't exist
    print(f"Checking/creating topic '{args.topic}'...")
    create_topic_if_not_exists(kafka_config, topic=args.topic)
    print()

    # Create producer
    producer: Producer = create_producer(kafka_config)

    print(f"Producing to topic '{args.topic}' with frequency {args.frequency}s")
    if args.num_records > 0:
        print(f"Total records: {args.num_records}")
    else:
        print("Mode: Continuous (press Ctrl+C to stop)")
    print()

    try:
        count = 0
        while True:
            # Check if we've reached the limit
            if args.num_records > 0 and count >= args.num_records:
                break

            # Produce batch
            for _ in range(args.batch_size):
                if args.num_records > 0 and count >= args.num_records:
                    break

                # Generate data
                data: Dict[str, Any] = renderer.render(template=template_content)

                # Serialize with Avro
                try:
                    serialized_value: bytes | None = avro_serializer(
                        obj=data,
                        ctx=SerializationContext(
                            topic=args.topic,
                            field=MessageField.VALUE,
                        ),
                    )

                    # Produce to Kafka
                    producer.produce(
                        topic=args.topic,
                        value=serialized_value,
                        callback=delivery_report,
                    )

                    count += 1

                except Exception as e:
                    print(f"Error producing message: {e}", file=sys.stderr)

            # Flush
            producer.flush()

            # Wait for next batch
            if args.num_records == 0 or count < args.num_records:
                time.sleep(args.frequency)

    except KeyboardInterrupt:
        print("\nStopping producer...")

    finally:
        # Final flush
        producer.flush()
        print(f"\nTotal records produced: {count}")


if __name__ == "__main__":
    main()
