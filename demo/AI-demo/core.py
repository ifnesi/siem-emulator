"""
Shared configuration and helpers used by both the CLI agent (agent.py) and
the web UI (webui/app.py). Keeps the system prompt, allowed tools, and
MCP -> Bedrock tool conversion in one place.
"""

import os

# Tools this demo lets the agent call. The MCP server (read_only: true on its
# connection) already disables every mutating tool server-side; this is a
# second, explicit allow-list on the client side so the agent only ever
# offers Bedrock the handful of tools this demo is about.
ALLOWED_TOOLS = {"list-topics", "consume-messages", "list-schemas"}

SYSTEM_PROMPT = """\
You are an AI assistant for a SIEM (Security Information and Event Management) demo.
You have access to a read-only Confluent Platform Kafka cluster running locally via
docker-compose. Topics contain DNS logs, FortiGate firewall logs, Palo Alto logs, and
Windows Event Logs, produced by siem_producer.py and (for DNS/FortiGate/Palo Alto)
further parsed/enriched by Flink/streaming apps into per-event-type sub-topics such as
siem_poc_dns_logs-aggregate, siem_poc_fortigate_logs-traffic-forward,
siem_poc_paloalto_logs-threat-virus, etc. All payloads are Avro, decoded via Schema
Registry.

The tools' `cluster_id` and `environment_id` parameters are for Confluent Cloud only
and are optional. This is a self-managed Confluent Platform cluster with a single
connection configured — always omit `cluster_id` and `environment_id` when calling
tools; never ask the user for them.

When listing topics, focus on the siem_poc_* topics. When consuming messages,
fetch a small sample (10-20 messages) unless the user asks for more. Use
list-schemas when the user asks about schema/subject structure, field names,
or Avro types registered for a topic.
Be concise and highlight security-relevant patterns in the data.
"""

AWS_REGION = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
BEDROCK_MODEL = os.environ.get("BEDROCK_MODEL", "us.anthropic.claude-haiku-4-5-20251001-v1:0")

# MCP server (mcp-confluent, HTTP transport) — see mcp-server/mcp-config.yaml.
# Defaults match the docker-compose.ai-demo.yml service name/port.
MCP_SERVER_URL = os.environ.get("MCP_SERVER_URL", "http://mcp-confluent:8080/mcp")

REQUIRED_ENV_VARS = (
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
)


def missing_env_vars() -> list[str]:
    return [v for v in REQUIRED_ENV_VARS if not os.environ.get(v)]


def mcp_tool_to_bedrock(tool) -> dict:
    # mcp>=2.0 renamed Tool.inputSchema (camelCase, matching the wire
    # protocol) to Tool.input_schema (snake_case, matching Python
    # convention) on the parsed model.
    schema = tool.input_schema or {"type": "object", "properties": {}}
    return {
        "toolSpec": {
            "name": tool.name,
            "description": tool.description or "",
            "inputSchema": {"json": schema},
        }
    }
