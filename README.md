# SIEM Data Emulator with Python and Confluent Kafka

Generate realistic SIEM (Security Information and Event Management) data and produce it to Confluent Kafka using a custom Python producer. Supports both **Avro** serialization (with automatic schema inference and Schema Registry integration) for structured JSON templates, and **raw UTF-8 text** (via `--no-schema`) for non-structured payloads like NGINX access logs or plain syslog lines.

## Features

- ✅ Custom templates
- ✅ Automatic Avro schema inference from templates
- ✅ Schema Registry integration
- ✅ Configurable frequency and batch size (deadline-paced so production time doesn't drift the rate)
- ✅ Continuous or fixed-count production modes
- ✅ 5 comprehensive SIEM templates included
- ✅ Realistic data for demos and testing
- ✅ Perfect for Splunk, Kibana, and other SIEM platforms

## Quick Start

### 1. Setup Virtual Environment and Install Dependencies

```bash
# Create virtual environment (if not exists)
python3 -m venv .venv

# Activate virtual environment
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Start Confluent Platform

```bash
docker compose up -d
```

Wait 30-60 seconds for services to start. Access Control Center at http://localhost:9021

### 3. Run Producers

Topics are created automatically if they don't exist (6 partitions by default, replication factor 1). Use `-p/--partitions N` to override.

```bash
# Activate virtual environment first
source .venv/bin/activate

# Continuous mode (press Ctrl+C to stop)
python siem_producer.py dns_log -t dns_log

# Produce 100 records
python siem_producer.py dns_log -t dns_log -n 100

# High frequency (100ms between records)
python siem_producer.py dns_log -t dns_log -f 0.1

# Batch mode (10 records per batch, every 2 seconds)
python siem_producer.py dns_log -t dns_log -f 2 -b 10 -n 1000
```

### 4. View Data

Open http://localhost:9021 → Topics → Select topic → Messages

## Available Templates

All templates are in the `templates/` directory and produce realistic, demo-ready data:

### 1. **dns_log** - DNS Query Logs (14 fields)
DNS traffic analysis with realistic queries and responses.

**Key Fields:**
- `src_ip`, `dst_ip` - 17+ internal IPs, 13+ DNS servers (Google, Cloudflare, internal)
- `query` - 40+ realistic domains (Google, AWS, GitHub, Microsoft, Slack, etc.)
- `qtype` - Query types (A, AAAA, TXT, MX, CNAME, PTR, SRV, NS)
- `rcode` - Response codes (NOERROR, NXDOMAIN, SERVFAIL, REFUSED)
- `protocol` - UDP, TCP, DoH, DoT
- `dns_flags` - RD, RA, AD, CD, AA
- `latency_ms` - 1-150ms response time

**Use Cases:** DNS traffic analysis, query patterns, DNS server performance

### 2. **siem_log** - Firewall/Security Events (19 fields)
Comprehensive security events from firewalls, IDS/IPS, WAF, and proxies.

**Key Fields:**
- `hostname` - 13+ firewall devices (Palo Alto, Fortigate, Checkpoint, ASA, etc.)
- `device_type` - firewall, IDS, IPS, WAF, proxy
- `severity` - info, warning, critical, alert
- `action` - allow, deny, block, drop, reject
- `threat_category` - malware, phishing, botnet, exploit, dos, data-exfiltration
- `source`/`destination` - Nested records with IP, port, zone, country
- `bytes_sent`, `bytes_received` - Traffic volume
- `session_duration` - Connection duration
- `application` - 25+ applications identified
- `user` - User attribution

**Use Cases:** Security monitoring, threat detection, firewall analysis, compliance

### 3. **net_device** - Network Flow Data (31 fields)
Detailed network flow records from switches and routers.

**Key Fields:**
- `device_name` - core-switch, edge-router, dist-switch, etc.
- `interface_in`/`interface_out` - GigabitEthernet, TenGigabitEthernet, Port-channel
- `vlan_id`, `vlan_name` - PROD-WEB, PROD-APP, DEV-NETWORK, DMZ, etc.
- `src_ip`, `dst_ip`, `src_port`, `dst_port` - Connection details
- `protocol_name` - TCP, UDP, ICMP, GRE, ESP, SCTP
- `tcp_flags` - SYN, ACK, FIN, RST, PSH
- `bytes_in`, `bytes_out`, `packets_in`, `packets_out` - Traffic metrics
- `flow_start_time`, `flow_end_time`, `flow_duration` - Flow timing
- `application` - HTTP, HTTPS, SSH, MySQL, Kafka, etc.
- `direction` - ingress, egress, internal, external

**Use Cases:** Network performance monitoring, capacity planning, traffic analysis

### 4. **syslog_log** - System Logs (18 fields)
Standard syslog messages from servers and infrastructure.

**Key Fields:**
- `hostname` - 16+ servers (web, app, db, k8s, jenkins, gitlab, etc.)
- `facility` - kern, user, mail, daemon, auth, cron, local0-7
- `severity` - emerg, alert, crit, err, warning, notice, info, debug
- `process_name` - sshd, nginx, docker, kubelet, mysqld, postgres, etc.
- `message` - 30+ realistic log messages (login, deployment, errors, etc.)
- `event_type` - authentication, security, deployment, monitoring
- `user` - root, admin, service accounts

**Use Cases:** System monitoring, troubleshooting, audit logging, compliance

### 5. **iot_device** - IoT Sensor Telemetry (13 fields) 🆕
Realistic IoT device telemetry with stateful sensor readings that drift gradually over time using state pools.

**Key Fields:**
- `device_id` - IOT-DEV-001, IOT-DEV-002, etc.
- `location` - Building and floor (e.g., "Building 1 - Floor A")
- `firmware_version` - 1.0.0, 1.0.1, 1.1.0, 2.0.0, etc.
- `network_type` - WiFi, LoRaWAN, Zigbee, Bluetooth, Ethernet, Cellular
- `ip_address`, `mac_address` - Network identifiers
- `manufacturer`, `model` - Device hardware info
- `temperature_celsius` - Gradual drift from device-specific baseline
- `humidity_percent` - Gradual drift from device-specific baseline
- `pressure_psi` - Gradual drift from device-specific baseline
- `battery_level` - Slowly decreasing over time
- `signal_strength` - Fluctuates around device-specific baseline

**Stateful Behavior:**
- Each device maintains its own baseline values for all sensors
- Readings drift gradually using small Gaussian deltas (realistic time-series)
- Static attributes (location, firmware, network type) remain constant per device
- Battery level decreases slowly over time
- Perfect example of state pool usage for realistic IoT simulation

**Use Cases:**
- IoT device monitoring and alerting
- Time-series analysis and anomaly detection
- Sensor data visualization
- Testing IoT platforms and dashboards
- Demonstrating stateful workload patterns

**Example:**
```bash
# Continuous IoT telemetry with realistic drift
python siem_producer.py iot_device -t iot_telemetry -f 5

# High-frequency sensor data (every 100ms)
python siem_producer.py iot_device -t iot_telemetry -f 0.1 -n 10000
```

See `templates/iot_device.j2` for the complete implementation using `init_pool()`, `pool()`, and `update_pool()`.

### 6. **pcap_data** - Network Packet Capture (37 fields)
PCAP-style network traffic data perfect for connection visualization and traffic analysis.

**Key Fields:**
- `src_ip`, `dst_ip` - 13+ internal, 13+ external IPs
- `src_hostname`, `dst_hostname` - Friendly names (web-server-01, api-gateway, etc.)
- `src_port`, `dst_port` - Ephemeral and well-known ports
- `bytes_sent`, `packets_sent` - Traffic volume metrics
- `protocol` - TCP, UDP, ICMP, GRE
- `tcp_flags` - SYN, ACK, FIN, RST, PSH
- `connection_state` - ESTABLISHED, SYN_SENT, FIN_WAIT, etc.
- `application` - HTTP, HTTPS, SSH, MySQL, Redis, Kafka, etc.
- `gateway` - Network gateway information
- `direction` - inbound, outbound, internal, external
- `latency_ms`, `retransmissions`, `packet_loss`, `jitter_ms` - Performance metrics
- `geo_src_country`, `geo_dst_country` - Geographic information

**Use Cases:**
- **Network Diagrams** - Visualize who connects to whom
- **Traffic Analysis** - Total bytes by source/destination
- **Performance Monitoring** - Latency, packet loss, jitter
- **Application Mapping** - Identify application usage patterns

**Perfect for Splunk/Kibana Visualizations:**
```
# Connection graph
src_hostname → dst_hostname (by bytes_sent)

# Top talkers
sum(bytes_sent) by src_ip, dst_ip

# Application distribution
count by application

# Geographic flow
traffic by geo_src_country → geo_dst_country
```

### 7. **nginx_access_log** - NGINX Access Logs (raw text, `--no-schema`)
NGINX combined log format with request time, produced as raw UTF-8 lines (no Avro, no Schema Registry). Use with `--no-schema`.

**Example output:**
```
47.29.190.187 - - [18/May/2026:21:38:03 +0000] "GET /contact HTTP/2.0" 200 34049 "-" "curl/7.88.1" "0.734"
```

**Fields per line:** remote IP, identity, user, `[local time]`, `"method path HTTP/version"`, status, body bytes, `"referer"`, `"user-agent"`, `"request time"`.

```bash
python siem_producer.py nginx_access_log -t nginx_access_log --no-schema
```

## Usage

```bash
# Make sure virtual environment is activated
source .venv/bin/activate

python siem_producer.py TEMPLATE [OPTIONS]

Required Arguments:
  TEMPLATE              Template name (without .j2 extension)
  -t, --topic TOPIC     Kafka topic name (not required with --dry-run)

Optional Arguments:
  -f, --frequency SEC          Seconds between records (default: 1.0)
  -n, --num-records N          Total records to produce (0 = continuous, default: 0)
  -b, --batch-size N           Records per batch (default: 1)
  -k, --key FIELD              Top-level field whose value is used as the Kafka
                               message key (must be a scalar). Default: no key (null).
  -p, --partitions N           Partitions when creating the topic (default: 6).
                               Ignored if the topic already exists.
  -s, --schema-id-location LOC Where to put the Avro schema ID — `headers` (default,
                               modern) or `body` (legacy 5-byte magic-byte framing).
  -ns, --namespace NS          Avro schema namespace (default: io.confluent.siem).
                               Ignored when --schema is set.
  --schema FILE                Use an existing Avro schema (JSON file) instead of
                               inferring one from the template. The file's contents
                               are registered as-is with Schema Registry.
  --inferred-schema            Print the schema that would be registered and exit
                               (no Kafka connection, no records produced).
  --no-schema                  Treat the rendered template as raw UTF-8 text
                               (e.g. NGINX access logs, syslog lines). Skips Avro
                               serialization and Schema Registry entirely.
                               Mutually exclusive with --schema, --inferred-schema, -k/--key.
  --dry-run                    Generate and display data without producing to Kafka
  --kafka-config FILE          Kafka config file (default: ./kafka/config.properties)
  --registry-config FILE       Schema Registry config (default: ./kafka/registry.properties)
  --templates-dir DIR          Templates directory (default: ./templates)

Examples:
  # Dry run - preview generated data without Kafka
  python siem_producer.py pcap_data --dry-run -n 5

  # Continuous production
  python siem_producer.py dns_log -t dns_log

  # Produce 1000 records
  python siem_producer.py siem_log -t siem_log -n 1000

  # High frequency (10 records/second)
  python siem_producer.py net_device -t net_device -f 0.1

  # Batch mode
  python siem_producer.py syslog_log -t syslog_log -f 5 -b 100

  # Keyed messages (partition by event_id)
  python siem_producer.py auth_event -t auth_events -k event_id

  # Custom partition count and legacy schema-ID body framing
  python siem_producer.py dns_log -t dns_log -p 12 -s body

  # Preview the inferred schema and exit (no Kafka connection needed)
  python siem_producer.py dns_log --inferred-schema

  # Register and produce against a hand-written / pre-existing schema file
  python siem_producer.py dns_log -t dns_log --schema schemas/dns_log.avsc

  # Raw text payload (no Avro, no Schema Registry) — e.g. NGINX access logs
  python siem_producer.py nginx_access_log -t nginx_access_log --no-schema
```

### Message Keys

By default messages are produced with a null key. Use `-k`/`--key FIELD` to set the Kafka message key from a top-level field in the rendered record:

```bash
python siem_producer.py auth_event -t auth_events -k event_id
python siem_producer.py dns_log    -t dns_log     -k src_ip
```

- The field must exist in the rendered record and be a **scalar** (string, int, float, or bool). Nested objects and arrays are rejected at startup.
- The key value is serialized as a UTF-8 string (so Control Center / `kafka-console-consumer` display it cleanly).
- The field **remains in the value payload** — keying is purely additive; the Avro schema is unchanged.
- Useful for partitioning by user, host, IP, or correlation ID so all events for the same entity land on the same partition (and therefore preserve order).

### Topic Partitions

When the producer creates a topic, it uses `-p`/`--partitions` (default `6`). This only applies at **creation time** — if the topic already exists, the flag is ignored and Kafka keeps the existing partition count. To change partitions on an existing topic you must delete it or use `kafka-topics --alter --partitions N` (which can only **increase** the count, and will break keyed-message ordering guarantees for existing keys).

### Schema ID Location

The Avro [schema ID](https://www.confluent.io/blog/schema-id-kafka-headers-data-governance/) tells consumers which registered schema was used to serialize a message. The producer supports two placements via `-s`/`--schema-id-location`:

| Mode      | What it does                                                                              | When to use                                                                                                              |
| --------- | ----------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------ |
| `headers` | (default) Schema ID lives in a Kafka message header named `__value_schema_id`.            | Modern default. Cleaner value payload (no framing bytes), better for data-governance tooling and non-Avro-aware consumers. Requires Confluent Platform 7.4+ / clients that understand header framing. |
| `body`    | Schema ID is prefixed inside the value bytes as `0x00 <4-byte big-endian id>` (5 bytes).  | Legacy. Required for older consumers or libraries that haven't adopted header framing yet.                               |

```bash
# Default — schema ID in headers
python siem_producer.py dns_log -t dns_log

# Legacy body framing
python siem_producer.py dns_log -t dns_log -s body
```

Don't mix modes within the same topic — a topic written with one mode and consumed with a client expecting the other will fail to deserialize.

### Dry Run Mode

Use `--dry-run` to preview generated data without connecting to Kafka or Schema Registry:

```bash
# Preview 10 records (default)
python siem_producer.py pcap_data --dry-run

# Preview specific number of records
python siem_producer.py siem_log --dry-run -n 3

# Test all templates
python siem_producer.py dns_log --dry-run -n 2
python siem_producer.py siem_log --dry-run -n 2
python siem_producer.py net_device --dry-run -n 2
python siem_producer.py syslog_log --dry-run -n 2
python siem_producer.py pcap_data --dry-run -n 2
```

Perfect for:
- Testing templates before production
- Verifying data format
- Debugging template syntax
- Generating sample data for documentation

### Using an Existing Schema

By default the producer infers the Avro schema from a few rendered samples (see [How It Works](#how-it-works)). Use `--schema PATH` to skip inference and register a hand-written / version-controlled schema instead:

```bash
python siem_producer.py dns_log -t dns_log --schema schemas/dns_log.avsc
```

- The file must be a valid Avro schema in JSON form; it is registered with Schema Registry **as-is** (no normalisation beyond `json.loads` / `json.dumps`).
- The rendered records must conform to the schema — fields the schema requires must be produced by the template, and types must match.
- `--namespace` / `-ns` is ignored in this mode (the namespace already lives in the schema file).
- Useful when you need a stable schema across environments, want to control naming/defaults/docs that inference doesn't produce, or are evolving a schema by hand to manage BACKWARD compatibility.

### Inspecting the Inferred Schema

Use `--inferred-schema` to print the schema the producer would register and exit — no Kafka or Schema Registry connection is made, and `-t/--topic` is not required:

```bash
python siem_producer.py dns_log --inferred-schema
```

Handy for:
- Bootstrapping a `--schema` file from a template (pipe the output to `schemas/<name>.avsc` and edit from there).
- Verifying which fields picked up logical types (`timestamp-millis`, `iso-8601-timestamp`) from `now()` / `unix_time_stamp()`.
- Reviewing a schema change before it hits Schema Registry.

Combining `--inferred-schema` with `--schema` will print the file you supplied rather than an inferred one — useful as a quick syntax check that the file is valid JSON before you try to produce against it.

### Raw-Text Payloads (`--no-schema`)

Use `--no-schema` for templates whose output is not a JSON record — e.g. NGINX access logs, plain syslog lines, CSV rows. The rendered template is encoded as UTF-8 and produced directly to Kafka with no Avro serialization, no Schema Registry call, and no schema-id framing.

```bash
# Preview a raw NGINX access log
python siem_producer.py nginx_access_log --no-schema --dry-run -n 3

# Produce to Kafka
python siem_producer.py nginx_access_log -t nginx_access_log --no-schema
```

- The template renders to a single string per record; nothing is parsed as JSON, so it can contain any text.
- The `strftime(fmt)` helper is available for arbitrary timestamp formats (e.g. `strftime('%d/%b/%Y:%H:%M:%S +0000')` for NGINX).
- `--no-schema` is mutually exclusive with `--schema`, `--inferred-schema`, and `-k/--key` (no fields to extract a key from).
- `--registry-config` is not read in this mode.

## How It Works

1. **Template Rendering**: The Python script reads your template and renders it with random data
2. **Schema Inference**: Several sample records are generated and merged to infer the Avro schema — this prevents fields that happen to be empty arrays in the first sample from being permanently typed as `array<string>`
3. **Schema Registration**: Schema is registered with Schema Registry
4. **Avro Serialization**: Data is serialized using the inferred Avro schema
5. **Kafka Production**: Serialized data is produced to the specified topic

## Template Syntax

Templates are [Jinja2](https://jinja.palletsprojects.com/) files (`.j2`) that render to JSON. Two conventions:

- **String fields** use the built-in `tojson` filter — it provides the JSON quotes and escapes special characters: `{{ helper(...) | tojson }}`
- **Numeric fields** render the raw value: `{{ helper(...) }}`

**Exception — `now()` and `unix_time_stamp()`** emit a JSON-shaped marker on their own so the Avro schema inferrer can attach a `logicalType` to the field. Render them bare (no `| tojson`):

```jinja
{
  "ts":             {{ now() }},
  "occurred_at_ms": {{ unix_time_stamp(60) }},
  "src_ip":         {{ ip("10.10.0.0/16") | tojson }},
  "query":          {{ randoms("opt1|opt2|opt3") | tojson }},
  "latency_ms":     {{ integer(1, 40) }}
}
```

### Supported Functions

- `now()` - Current UTC timestamp as an ISO-8601 string. The field's Avro schema gets `{"type": "string", "logicalType": "iso-8601-timestamp"}`. Render bare (no `| tojson`).
- `unix_time_stamp(N)` - Unix timestamp in **milliseconds**, randomly chosen between now and N seconds ago. The field's Avro schema gets `{"type": "long", "logicalType": "timestamp-millis"}`. Render bare (no `| tojson`).
- `ip("CIDR")` - Random IP from CIDR range (string)
- `guid()` - Random UUID4 as a lowercase hyphenated string, e.g. `"550e8400-e29b-41d4-a716-446655440000"` (string). Use in place of a hand-rolled `regex("[0-9a-f]{8}-...")` for event/trace/correlation IDs.
- `randoms(source)` - Random choice from `source`, which is either a pipe-separated string (`"a|b|c"`) or any sequence — typically one of the lists loaded from `templates/data/` and exposed as `data.<filename>` (e.g. `randoms(data.countries)`). Repeat values to bias the distribution: `"info|info|info|warning"`. Cast to a number with `| int` when emitting into a numeric field (e.g. `randoms(data.known_ports) | int`).
- `data.<filename>` - List of stripped, non-empty, non-comment lines loaded from `templates/data/<filename>` at startup. See **External Data Sources** below.
- `integer(min, max)` - Random integer in range (accepts negative bounds)
- `floating(min, max, decimals=2)` - Random floating-point number, accepts negatives
- `gaussian(mean, std_dev, decimals=2)` - Random number from a Gaussian (normal) distribution. Perfect for realistic sensor readings and natural variation.
- `random_string(min, max)` - Random alphanumeric string of length in `[min, max]`
- `random_string_vocabulary(min, max, "chars")` - Random string of length in `[min, max]` drawn from a character set
- `counter("name", start, step)` - Monotonic counter per name (start, start+step, start+2*step, ...)
- `regex("pattern")` - Random string matching regex pattern
- `min(a, b, ...)` - Return the smallest value from arguments. Useful for clamping upper bounds.
- `max(a, b, ...)` - Return the largest value from arguments. Useful for clamping lower bounds.
- `init_pool("pool_name", dict)` - Initialize a state pool with pre-generated data (see **Stateful Workloads** below)
- `pool("pool_name", key, field)` - Retrieve a value from a state pool (see **Stateful Workloads** below)
- `update_pool("pool_name", key, field, value)` - Update a value in a state pool (see **Stateful Workloads** below)

Since templates are full Jinja2, `{% if %}` / `{% for %}` / nested expressions are all available if you need conditional or repeating fields.

### Floating-Point Numbers

`floating(min, max, decimals=2)` generates random floating-point numbers with configurable decimal places. Perfect for metrics, measurements, and percentages.

**Examples:**
```jinja
{
  "temperature":     {{ floating(15, 35) }},                  // 23.45 (default 2 decimals)
  "cpu_usage":       {{ floating(0, 100) }},                  // 78.92 (default 2 decimals)
  "delta":           {{ floating(-1.5, 1.5, decimals=2) }},   // -0.42 (negative bounds allowed)
  "disk_io":         {{ floating(0.5, 10.5, decimals=1) }},   // 7.8 (1 decimal)
  "response_time":   {{ floating(0.1, 5.9, decimals=3) }},    // 2.347 (3 decimals)
  "precision_value": {{ floating(0, 1, decimals=4) }},        // 0.1234 (4 decimals)
  "percentage":      {{ floating(0, 100, decimals=0) }}       // 78.0 (still a float; use integer() for an int)
}
```

### Gaussian Distribution

`gaussian(mean, std_dev, decimals=2)` generates random numbers from a Gaussian (normal) distribution. Perfect for realistic sensor readings, natural variation, and modeling real-world measurements.

**Examples:**
```jinja
{
  "temperature":     {{ gaussian(22.0, 2.0, 2) }},        // Mean 22°C, std dev 2°C
  "humidity":        {{ gaussian(45.0, 10.0, 1) }},       // Mean 45%, std dev 10%
  "response_time":   {{ gaussian(100.0, 15.0, 2) }},      // Mean 100ms, std dev 15ms
  "cpu_usage":       {{ gaussian(50.0, 20.0, 1) }},       // Mean 50%, std dev 20%
  "pressure":        {{ gaussian(14.7, 0.2, 3) }},        // Mean 14.7 psi, std dev 0.2
  "signal_strength": {{ gaussian(-65.0, 5.0, 1) }}        // Mean -65 dBm, std dev 5 (negative values work)
}
```

**Use Cases:**
- Sensor readings (temperature, humidity, pressure)
- Network metrics (latency, jitter)
- System metrics (CPU, memory, disk I/O)
- Any measurement that follows a bell curve distribution

### Stateful Workloads (State Pools)

For realistic time-series data where each reading should drift from the previous one (rather than jumping randomly), use **state pools**. This enables "random walk" patterns where sensor baselines evolve gradually over time, creating realistic IoT device behavior, stock prices, or any metric that changes incrementally.

**Three functions work together:**

1. **`init_pool("pool_name", dict)`** - Initialize a pool with pre-generated entities and their baseline values
2. **`pool("pool_name", key, field)`** - Retrieve a value from the pool
3. **`update_pool("pool_name", key, field, value)`** - Update a value in the pool (for the next render)

**Pattern:**
```jinja
{# 1. Initialize pool once with baseline values for each entity #}
{%- set device_configs = {} -%}
{%- for i in range(0, 10) -%}
    {%- set device_id = "Device-%03d"|format(i) -%}
    {%- set _ = device_configs.update({
        device_id: {
            'location': randoms("Building A|Building B|Building C"),
            'baseline_temperature': gaussian(22.0, 5.0, 2),
            'baseline_humidity': gaussian(45.0, 15.0, 1)
        }
    }) -%}
{%- endfor -%}
{%- set _ = init_pool('devices', device_configs) -%}

{# 2. Pick a random device #}
{%- set device_id = randoms(device_configs.keys()|list) -%}

{# 3. Get current baseline and add small drift #}
{%- set new_temp = pool('devices', device_id, 'baseline_temperature') + gaussian(0.0, 0.5, 2) -%}
{%- set new_humidity = pool('devices', device_id, 'baseline_humidity') + gaussian(0.0, 1.0, 1) -%}

{# 4. Clamp values to realistic bounds using min/max #}
{%- set new_temp = max(-10, min(50, new_temp)) -%}
{%- set new_humidity = max(0, min(100, new_humidity)) -%}

{# 5. Update pool with new values (becomes baseline for next render) #}
{%- set _ = update_pool('devices', device_id, 'baseline_temperature', new_temp) -%}
{%- set _ = update_pool('devices', device_id, 'baseline_humidity', new_humidity) -%}

{# 6. Emit the record #}
{
    "timestamp": {{ now() }},
    "device_id": {{ device_id | tojson }},
    "location": {{ pool('devices', device_id, 'location') | tojson }},
    "temperature_celsius": {{ new_temp | round(2) }},
    "humidity_percent": {{ new_humidity | round(1) }}
}
```

**How it works:**
- **First render:** Each device gets a random baseline (e.g., Device-001 starts at 23.4°C, Device-002 at 19.8°C)
- **Subsequent renders:** Each reading drifts slightly from the previous one using `gaussian(0.0, small_std_dev)`
- **Result:** Device-001 might read 23.6°C → 23.4°C → 23.9°C → 23.7°C (realistic drift), not 23.4°C → 18.2°C → 27.9°C (unrealistic jumps)

**Complete Example:** See `templates/iot_device.j2` for a full implementation with:
- 10 IoT devices with unique IDs, locations, firmware versions, IP addresses, MAC addresses
- 5 sensor baselines per device (temperature, humidity, pressure, battery, signal strength)
- Gradual drift using small Gaussian deltas
- Static attributes (location, firmware) retrieved from pool without updates

**Use Cases:**
- **IoT sensors** - Temperature, humidity, pressure readings that drift gradually
- **Stock prices** - Price changes as small deltas from previous close
- **Network metrics** - Latency/jitter that varies around a baseline
- **System metrics** - CPU/memory usage that trends up/down over time
- **User behavior** - Session duration, page views that evolve per user

**Key Benefits:**
- **Realistic time-series:** Data looks like real sensor readings, not random noise
- **Per-entity state:** Each device/user/host maintains its own baseline
- **Correlation:** Multiple metrics per entity can drift together (e.g., high temp → low battery)
- **Reproducible:** Same device always starts from the same baseline (until updated)

**Important Notes:**
- Pools are initialized once at startup and persist across all renders
- Don't pass default values to `pool()` — it overrides the carefully initialized baselines
- Use small standard deviations in `gaussian()` for drift (0.1-1.0) vs. large ones for initial baselines (5.0-15.0)
- Static attributes (location, firmware) are retrieved but never updated

### Regex Pattern Support

The `{{regex "pattern"}}` function generates random strings matching regex patterns. Perfect for creating realistic formatted data like SSNs, phone numbers, license plates, etc.

**Supported regex features:** anything Python's `re` module supports — the generator delegates to the [`exrex`](https://pypi.org/project/exrex/) library. Common examples:
- `\d`, `\w`, `\s` - digit / word / whitespace
- `[a-z]`, `[A-Z]`, `[0-9]`, `[^abc]` - character classes (incl. negation)
- `{n}`, `{n,m}`, `+`, `*`, `?` - repetition
- `a|b|c` - alternation
- `(...)` - groups
- `.` - any character
- `\(`, `\)`, `\.` - literal escapes

**Examples:**
```jinja
{
  "ssn":           {{ regex("\\d{3}-\\d{2}-\\d{4}") | tojson }},                                   // "123-45-6789"
  "phone":         {{ regex("\\(\\d{3}\\) \\d{3}-\\d{4}") | tojson }},                             // "(555) 123-4567"
  "zip_code":      {{ regex("\\d{5}") | tojson }},                                                  // "90210"
  "license_plate": {{ regex("[A-Z]{3}-\\d{4}") | tojson }},                                         // "ABC-1234"
  "hex_color":     {{ regex("#[0-9A-F]{6}") | tojson }},                                            // "#FF5733"
  "username":      {{ regex("[a-z]{5,10}") | tojson }},                                             // "johndoe" (5-10 chars)
  "product_code":  {{ regex("[A-Z]{2}\\d{3}[A-Z]") | tojson }},                                    // "AB123C"
  "mac_address":   {{ regex("[0-9A-F]{2}:[0-9A-F]{2}:[0-9A-F]{2}:[0-9A-F]{2}:[0-9A-F]{2}:[0-9A-F]{2}") | tojson }}  // "A1:B2:C3:D4:E5:F6"
}
```

**Note:** Backslashes inside Jinja string literals follow Python rules, so write `"\\d"` for a literal `\d`. The generator also accepts the older quadruple-backslash form (`"\\\\d"`) for backward compatibility.

### External Data Sources

Long pipe-separated lists clutter templates and force a code change every time you tweak the catalog of countries, hostnames, ports, etc. Instead, keep the list in its own plain-text file under `templates/data/`:

```
templates/data/
├── countries          # one value per line
├── devices
├── dns_servers
├── endpoints
├── interfaces
├── known_ports
├── users_agents
└── users
```

At startup the producer reads every file in that directory and exposes it on the Jinja2 `data` global, keyed by filename. A file named `countries` becomes `data.countries` — a Python list of strings.

**File format**
- One value per line.
- Surrounding whitespace is trimmed and blank lines are ignored.
- Lines whose first non-whitespace character is `#` are treated as comments and skipped — handy for grouping or annotating entries. There's no escape for a literal leading `#`; if you genuinely need a value that starts with `#` (e.g. a hex color like `#FF5733`), generate it from a template helper such as `regex("#[0-9A-F]{6}")` instead of putting it in a data file.
- Filename (no extension required) becomes the attribute name; stick to identifier-safe names so `data.foo` works (use `data["foo-bar"]` if you really need a dash).
- Repeat lines to bias the distribution — `US` appearing 11× and `JP` 3× makes `US` ~3.7× more likely.

Example with comments:

```
# Common Linux daemons
sshd
nginx
postgres

# Container runtime
docker
containerd
```

**Using a data source in a template**

```jinja
{
  "country":  {{ randoms(data.countries) | tojson }},
  "host":     {{ randoms(data.endpoints) | tojson }},
  "device":   {{ randoms(data.devices) | tojson }},
  "protocol": {{ randoms(data.known_protocols) | tojson }},
  "port":     {{ randoms(data.known_ports) | int }}
}
```

Everything in `data.*` is a list of **strings** — cast to a number with `| int` (or `| float`) when emitting into a numeric field, just like inline `randoms("80|443") | int`.

**Extending a data source inline**

`data.*` values are ordinary Python lists, so the natural Jinja2 way to extend one is list concatenation with `+`. No new helper needed:

```jinja
{# Add a couple of extras for this template only #}
{{ randoms(data.users + ["root", "admin"]) | tojson }}

{# Bias the inline additions by repeating them (operator * on a list) #}
{{ randoms(data.users + ["root"] * 20) | tojson }}

{# Prefer pipe-shorthand for the extras? Use Python's str.split #}
{{ randoms(data.users + "root|admin".split("|")) | tojson }}

{# Combine two data files into one pool #}
{{ randoms(data.users + data.service_accounts) | tojson }}
```

If the same combined pool is reused across several fields in one template, build it once with `{% set %}`:

```jinja
{% set user_pool = data.users + ["root", "admin"] %}
{
  "actor": {{ randoms(user_pool) | tojson }},
  "owner": {{ randoms(user_pool) | tojson }}
}
```

The same trick works for filtering, slicing, or sorting (`data.users | reject("startswith", "svc-") | list`, `data.countries[:5]`, etc.) — anything Jinja2 can do to a list works against `data.*` for free.

**Adding your own data source**

1. Drop a new file into `templates/data/` (e.g. `templates/data/usernames`).
2. Put one value per line; repeat values to weight the distribution.
3. Reference it from any template as `data.usernames`.
4. Restart the producer — files are loaded once at startup.

Because `data.*` values are ordinary Python lists, every Jinja2 list construct works on them too — e.g. iterate with `{% for u in data.usernames %}…{% endfor %}` or pick at random with the built-in filter: `{{ data.countries | random | tojson }}`.

**When to use a data file vs. inline `randoms("a|b|c")`**
- **Data file** — long lists, lists shared across templates, anything a non-developer should be able to edit, or anything you want under version control as data rather than code.
- **Inline** — short, template-specific options where the distribution is part of the template's meaning (e.g. `randoms("info|info|info|warning|error")`).

## Creating Custom Templates

1. Create a new `.j2` file in `templates/`.
2. Write the template using the Jinja2 conventions described above (`| tojson` for strings, bare `{{ }}` for numbers).
3. Run the producer with the file's basename — e.g. `templates/my_log.j2` → `python siem_producer.py my_log`.

### Worked example

A richer template that exercises every helper. Save as `templates/auth_event.j2`:

```jinja
{
  "timestamp":  {{ now() }},
  "event_id":   {{ guid() | tojson }},
  "sequence":   {{ counter("auth", 1, 1) }},
  "occurred_at_ms": {{ unix_time_stamp(60) }},
  "user":       {{ randoms("alice|bob|carol|dave|root") | tojson }},
  "action":     {{ randoms("login|login|login|logout|password_change|failed_login") | tojson }},
  "source": {
    "ip":      {{ ip("10.0.0.0/16") | tojson }},
    "port":    {{ integer(1024, 65535) }},
    "country": {{ randoms("US|US|GB|DE|FR|JP|BR") | tojson }}
  },
  "target_port": {{ randoms("22|443|3389|5432") | int }},
  "latency_ms":  {{ floating(0.5, 250.0, decimals=2) }},
  "session_id":  {{ random_string_vocabulary(16, 24, "0123456789ABCDEF") | tojson }}
}
```

Preview it, then produce to Kafka:

```bash
source .venv/bin/activate
python siem_producer.py auth_event --dry-run -n 3
python siem_producer.py auth_event -t auth_events -n 100
```

The Avro schema is inferred from a few rendered samples and registered automatically.

### Common patterns

- **String values** — `{{ helper(...) | tojson }}`. `tojson` adds the surrounding quotes and escapes anything that needs escaping (backslashes, control chars, embedded quotes). Don't add your own `"..."` around the expression.
- **Numeric values** — `{{ helper(...) }}`. No quotes, no filter; the bare value parses as a JSON number.
- **`randoms()` producing a number** — `{{ randoms("80|443|22") | int }}`. `randoms` always returns a string; `| int` (or `| float`) casts so it renders as a JSON number.
- **Nested objects / arrays** — write the JSON structure literally; only the expressions inside `{{ ... }}` are dynamic.
- **Correlated fields** — full Jinja2 is available, so use `{% set %}` and `{% if %}` to derive one field from another:

  ```jinja
  {% set action = randoms("allow|allow|deny") %}
  {
    "action":   {{ action | tojson }},
    "severity": {{ ("info" if action == "allow" else "warning") | tojson }}
  }
  ```

### How fields map to Avro types

Schema inference walks the rendered Python dict and maps each value:

| Python value                          | Avro type                |
| ------------------------------------- | ------------------------ |
| `str`                                 | `string`                 |
| `int` (any range)                     | `long`                   |
| `float`                               | `double`                 |
| `bool`                                | `boolean`                |
| `dict`                                | nested `record`          |
| `list`                                | `array`                  |

Integers are always emitted as `long` rather than `int`. Picking based on a sampled value is non-deterministic (random samples can land on either side of 2^31), and once a topic is registered with `long`, Schema Registry's BACKWARD compatibility forbids narrowing back to `int`. Widening `int` → `long` is safe (Avro promotes `int` writers to `long` readers).

**Logical types.** A few helpers also annotate the schema with an Avro `logicalType`:

| Helper                  | Avro field type                                                  |
| ----------------------- | ---------------------------------------------------------------- |
| `now()`                 | `{"type": "string", "logicalType": "iso-8601-timestamp"}`        |
| `unix_time_stamp(N)`    | `{"type": "long", "logicalType": "timestamp-millis"}`            |

These helpers emit a JSON-shaped marker (`{"__logicaltype_<name>__": <value>}`) that the renderer unwraps after parsing — the marker tells the schema inferrer which `logicalType` to attach, and the data going to Kafka is the bare value (string or long). Because they're already JSON, render them bare (`{{ now() }}`, `{{ unix_time_stamp(60) }}`) without `| tojson`.

`iso-8601-timestamp` is a custom (non-standard) logical type — Avro readers that don't recognize it fall back to treating the field as a plain `string`, which is the desired behavior. `timestamp-millis` is part of the [Avro spec](https://avro.apache.org/docs/current/specification/#logical-types).

## Configuration

The producer automatically reads **ALL non-commented properties** from the configuration files. No hardcoded values are used.

### kafka/config.properties

All properties from [librdkafka configuration](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md) are automatically applied to the Kafka producer and admin client.

```properties
# Required
bootstrap.servers=localhost:9092
security.protocol=PLAINTEXT

# Optional - SASL authentication
#security.protocol=SASL_SSL
#sasl.mechanisms=PLAIN
#sasl.username=your-username
#sasl.password=your-password

# Optional - compression
#compression.type=gzip
#compression.level=9

# Optional - monitoring
#statistics.interval.ms=1000

# Any other librdkafka property will be automatically applied
```

### kafka/registry.properties

Schema Registry connection settings. All non-commented properties are automatically used.

```properties
# Required
schemaRegistryURL=http://localhost:8081
auto.register.schemas=true

# Optional - Basic authentication (format: username:password or API_KEY:API_SECRET)
#basic.auth.user.info=
```

**Important**:
- The script reads ALL non-commented properties from these files
- No need to modify the Python code to add new configuration options
- Simply uncomment or add any property you need in the config files

## Verify Setup

```bash
# List topics
docker compose exec broker kafka-topics --bootstrap-server localhost:9092 --list

# Check registered schemas
curl http://localhost:8081/subjects

# View schema details
curl http://localhost:8081/subjects/dns_log-value/versions/latest | jq .
```

## Troubleshooting

**Module not found**
```bash
pip install -r requirements.txt
```

**Connection refused**
```bash
docker compose ps  # Check services are running
docker compose logs broker  # Check broker logs
```

**Template not found**
- Ensure template file exists in `templates/` directory
- Use template name without `.j2` extension

## Stopping

```bash
# Stop producer: Ctrl+C

# Stop Confluent Platform
docker compose down

# Remove all data
docker compose down -v
```

## Project Structure

```
.
├── siem_producer.py           # Python producer script
├── requirements.txt           # Python dependencies
├── docker-compose.yml         # Confluent Platform services
├── kafka/
│   ├── config.properties      # Kafka connection config
│   └── registry.properties    # Schema Registry config
├── schemas/                   # Hand-written Avro schemas (used via --schema)
│   ├── auth_event.avsc
│   ├── dns_log.avsc
│   ├── net_device.avsc
│   ├── pcap_data.avsc
│   ├── siem_log.avsc
│   └── syslog_log.avsc
└── templates/                 # SIEM data templates
    ├── auth_event.j2
    ├── dns_log.j2
    ├── iot_device.j2          # IoT telemetry with stateful sensors
    ├── siem_log.j2
    ├── net_device.j2
    ├── syslog_log.j2
    ├── pcap_data.j2
    ├── nginx_access_log.j2    # raw text (use with --no-schema)
    └── data/                  # Plain-text lists, one value per line
        ├── countries          #   → data.countries
        ├── devices            #   → data.devices
        ├── dns_servers        #   → data.dns_servers
        ├── endpoints          #   → data.endpoints
        ├── interfaces         #   → data.interfaces
        ├── known_ports        #   → data.known_ports
        ├── known_protocols    #   → data.known_protocols
        ├── user_agents        #   → data.user_agents
        └── users              #   → data.users
```

## Resources

- [Confluent Kafka Python](https://docs.confluent.io/kafka-clients/python/current/overview.html)
- [Apache Avro](https://avro.apache.org/docs/)
- [Confluent Platform](https://docs.confluent.io/)