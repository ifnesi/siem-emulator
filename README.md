# SIEM Data Emulator with Python and Confluent Kafka

Generate realistic SIEM (Security Information and Event Management) data and produce it to Confluent Kafka with Avro serialization using a custom Python producer.

## Features

- ✅ Custom templates
- ✅ Automatic Avro schema inference from templates
- ✅ Schema Registry integration
- ✅ Configurable frequency and batch size
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

Topics are created automatically if they don't exist (1 partition, replication factor 1).

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

### 5. **pcap_data** - Network Packet Capture (37 fields) 🆕
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

## Usage

```bash
# Make sure virtual environment is activated
source .venv/bin/activate

python siem_producer.py TEMPLATE [OPTIONS]

Required Arguments:
  TEMPLATE              Template name (without .tpl extension)
  -t, --topic TOPIC     Kafka topic name (not required with --dry-run)

Optional Arguments:
  -f, --frequency SEC   Seconds between records (default: 1.0)
  -n, --num-records N   Total records to produce (0 = continuous, default: 0)
  -b, --batch-size N    Records per batch (default: 1)
  --dry-run             Generate and display data without producing to Kafka
  --kafka-config FILE   Kafka config file (default: ./kafka/config.properties)
  --registry-config FILE Schema Registry config (default: ./kafka/registry.properties)
  --templates-dir DIR   Templates directory (default: ./templates)

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
```

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

## How It Works

1. **Template Rendering**: The Python script reads your template and renders it with random data
2. **Schema Inference**: Avro schema is automatically inferred from the generated JSON structure
3. **Schema Registration**: Schema is registered with Schema Registry
4. **Avro Serialization**: Data is serialized using the inferred Avro schema
5. **Kafka Production**: Serialized data is produced to the specified topic

## Template Syntax

```json
{
  "ts": "{{now}}",
  "src_ip": "{{ip \"10.10.0.0/16\"}}",
  "query": "{{randoms \"opt1|opt2|opt3\"}}",
  "latency_ms": {{integer 1 40}}
}
```

### Supported Functions

- `{{now}}` - Current UTC timestamp
- `{{unix_time_stamp N}}` - Unix timestamp N seconds ago
- `{{ip "CIDR"}}` - Random IP from CIDR range
- `{{ip_known_port}}` - Random well-known port (20, 21, 22, 23, 25, 53, 80, 110, 143, 443, 445, 3306, 3389, 5432, 8080, 8443)
- `{{ip_known_protocol}}` - Random protocol (HTTP, HTTPS, FTP, SSH, SMTP, DNS, TELNET, IMAP, POP3, SMB, MySQL, PostgreSQL, RDP)
- `{{randoms "a|b|c"}}` - Random choice from pipe-separated options
- `{{integer min max}}` - Random integer in range
- `{{floating min max [decimals]}}` - Random floating-point number (default 2 decimal places) 🆕
- `{{random_string min max}}` - Random alphanumeric string
- `{{random_string_vocabulary min max "chars"}}` - Random string from character set
- `{{random_v_from_list "list"}}` - Random value from list (simplified to IP generation)
- `{{counter "name" start step}}` - Counter value (simplified to random for now)
- `{{regex "pattern"}}` - Random string matching regex pattern 🎉

### Floating-Point Numbers

The `{{floating min max [decimals]}}` function generates random floating-point numbers with configurable decimal places (default: 2). Perfect for metrics, measurements, and percentages.

**Examples:**
```json
{
  "temperature": {{floating 15 35}},           // 23.45 (default 2 decimals)
  "cpu_usage": {{floating 0 100}},             // 78.92 (default 2 decimals)
  "disk_io": {{floating 0.5 10.5 1}},          // 7.8 (1 decimal)
  "response_time": {{floating 0.1 5.9 3}},     // 2.347 (3 decimals)
  "precision_value": {{floating 0 1 4}},       // 0.1234 (4 decimals)
  "percentage": {{floating 0 100 0}}           // 78 (0 decimals - whole number)
}
```

### Regex Pattern Support

The `{{regex "pattern"}}` function generates random strings matching regex patterns. Perfect for creating realistic formatted data like SSNs, phone numbers, license plates, etc.

**Supported regex features:**
- `\d` - Random digit (0-9)
- `\w` - Random word character (a-z, A-Z, 0-9, _)
- `\s` - Whitespace
- `[a-z]`, `[A-Z]`, `[0-9]` - Character classes with ranges
- `{n}` - Exact repetition (e.g., `\d{3}` = 3 digits)
- `{n,m}` - Variable repetition (e.g., `[a-z]{5,10}` = 5-10 lowercase letters)
- `.` - Any alphanumeric character
- `\(`, `\)` - Literal parentheses (and other escaped characters)

**Examples:**
```json
{
  "ssn": "{{regex \"\\d{3}-\\d{2}-\\d{4}\"}}",           // "123-45-6789"
  "phone": "{{regex \"\\(\\d{3}\\) \\d{3}-\\d{4}\"}}",  // "(555) 123-4567"
  "zip_code": "{{regex \"\\d{5}\"}}",                    // "90210"
  "license_plate": "{{regex \"[A-Z]{3}-\\d{4}\"}}",     // "ABC-1234"
  "hex_color": "{{regex \"#[0-9A-F]{6}\"}}",            // "#FF5733"
  "username": "{{regex \"[a-z]{5,10}\"}}",              // "johndoe" (5-10 chars)
  "product_code": "{{regex \"[A-Z]{2}\\d{3}[A-Z]\"}}",  // "AB123C"
  "mac_address": "{{regex \"[0-9A-F]{2}:[0-9A-F]{2}:[0-9A-F]{2}:[0-9A-F]{2}:[0-9A-F]{2}:[0-9A-F]{2}\"}}"  // "A1:B2:C3:D4:E5:F6"
}
```

**Note:** In template files, use double backslashes (`\\d`) for regex escape sequences. The system handles both single and double-escaped patterns automatically.

## Creating Custom Templates

1. Create a new `.tpl` file in `templates/` directory
2. Use the template syntax above
3. Run the producer with your template name

Example (`templates/my_log.tpl`):
```json
{
  "timestamp": "{{now}}",
  "severity": "{{randoms \"low|medium|high|critical\"}}",
  "source_ip": "{{ip \"192.168.0.0/16\"}}",
  "message": "{{random_string 10 50}}"
}
```

Run it:
```bash
source .venv/bin/activate
python siem_producer.py my_log -t my_topic
```

The Avro schema will be automatically inferred and registered!

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
- Use template name without `.tpl` extension

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
└── templates/                 # SIEM data templates
    ├── dns_log.tpl
    ├── siem_log.tpl
    ├── net_device.tpl
    └── syslog_log.tpl
```

## Resources

- [Confluent Kafka Python](https://docs.confluent.io/kafka-clients/python/current/overview.html)
- [Apache Avro](https://avro.apache.org/docs/)
- [Confluent Platform](https://docs.confluent.io/)