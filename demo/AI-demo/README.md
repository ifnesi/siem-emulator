# SIEM AI Agent

Lets you query this project's local Confluent Platform cluster in natural
language, via the [Confluent MCP Server](https://github.com/confluentinc/mcp-confluent)
(`mcp-confluent`). Three ways to use it:

- **Option A — Claude Code** connected directly to the MCP server over HTTP
  (Claude itself is the agent — no Bedrock, no custom chat app)
- **Option B — Web UI** — a small Flask + React chat app using AWS Bedrock
  (Claude Haiku 4.5) as the LLM
- **Option C — Terminal CLI** — same Bedrock-backed agent as Option B, as a
  chat REPL

Everything (MCP server, Web UI, CLI agent) runs in Docker, alongside the rest
of this repo's `docker-compose.yml` stack.

```
Option A:  Claude Code ──────────────┐
Option B:  ai-webui (Flask) ─────────┼──▶ mcp-confluent (HTTP, :8080) ──▶ broker + schema-registry
Option C:  ai-cli (REPL)    ─────────┘         (local docker-compose SIEM cluster)

Options B/C also call:
  ai-webui / ai-cli ──▶ AWS Bedrock (Claude Haiku 4.5)
```

All three are **read-only** against the cluster (the MCP connection sets
`read_only: true`) and expose the same tools:
- `list-topics` — list topics
- `consume-messages` — fetch and Avro-decode messages from a topic via Schema Registry
- `list-schemas` — list registered schemas (full Avro definitions)

This is a local demo — the MCP server's HTTP transport runs with auth
disabled (`server.auth.disabled: true` in `mcp-server/mcp-config.yaml`) since
it's only reachable via the docker-compose network or `localhost:8080` on
your machine. Don't reuse this config as-is for anything internet-facing.

---

## Why a second Kafka listener?

`mcp-confluent`'s `direct` connection type requires a `kafka.auth` block —
there is no "no-auth" option in its config schema, and it defaults to
`sasl_ssl`. This repo's `broker` service is otherwise PLAINTEXT-only (no
SASL, no TLS), which is why `docker-compose.ai-demo.yml` adds a second,
**internal-only** `SASL_PLAINTEXT` listener (`broker:9095`) purely for
`mcp-confluent` to authenticate against — the same pattern used by
`mcp-confluent`'s own `docker-compose.cp-test.yml` for local testing. The
existing listeners used by `siem_producer.py` and the streaming apps
(`9092`/`29092`) are untouched.

Schema Registry needs no such change — it's already unauthenticated, and
`schema_registry.auth` is optional in `mcp-confluent`'s config.

---

## Prerequisites

- Docker + Docker Compose
- An AWS IAM access key with Bedrock permission for the model in
  `BEDROCK_MODEL` (only needed for Options B/C — Option A uses Claude Code
  directly, no AWS involved). See **AWS / IAM Setup for Bedrock** below.

---

## AWS / IAM Setup for Bedrock (Options B/C only)

`ai-webui`/`ai-cli` call AWS Bedrock's `converse_stream` API directly via
`boto3` (see `core.py`, `agent.py`, `webui/app.py`) — there's no Confluent or
MCP involvement in this part. You need an IAM identity whose access key you
put in `demo/AI-demo/.env`, with:

1. **Model access enabled** for the Anthropic model in your account/region
   (a one-time, per-account/region console step — separate from IAM
   permissions).
2. **An IAM policy** granting `bedrock:InvokeModelWithResponseStream` (and
   `InvokeModel`, used for non-streaming calls) on that model.

### 1. Enable model access

Bedrock requires you to explicitly request access to a model family before
any IAM identity — however permissioned — can invoke it:

1. AWS Console → **Amazon Bedrock** → **Model access** (left sidebar).
2. Click **Modify model access** (or **Enable specific models**).
3. Check **Anthropic → Claude Haiku 4.5** (and Claude generally, since some
   accounts gate the whole vendor).
4. Submit. For Anthropic models this is usually granted instantly (no
   business-justification form), but check the console — some
   accounts/regions still require one.
5. Do this **in the same region** you'll set as `AWS_REGION` — model access
   is per-region, not account-wide.

Verify from the CLI once enabled:

```bash
aws bedrock list-foundation-models --region us-east-1 \
  --query "modelSummaries[?contains(modelId,'claude-haiku')].modelId"
```

### 2. IAM policy

`BEDROCK_MODEL` defaults to `us.anthropic.claude-haiku-4-5-20251001-v1:0` — a
**cross-region inference profile** (the `us.` prefix), not a bare model ID.
Cross-region inference profiles route your request to whichever underlying
model in that geo has capacity, so the policy needs to authorize **both**
the inference-profile ARN itself **and** the underlying foundation-model ARN
it can route to — authorizing only one or the other fails at call time with
an `AccessDeniedException`.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "SiemAiDemoBedrockInvoke",
      "Effect": "Allow",
      "Action": [
        "bedrock:InvokeModel",
        "bedrock:InvokeModelWithResponseStream"
      ],
      "Resource": [
        "arn:aws:bedrock:*::foundation-model/anthropic.claude-haiku-4-5-20251001-v1:0",
        "arn:aws:bedrock:*:*:inference-profile/us.anthropic.claude-haiku-4-5-20251001-v1:0"
      ]
    }
  ]
}
```

Notes on the two `Resource` entries:
- **Foundation-model ARNs** (`foundation-model/...`) have no account ID
  segment — they're global per-region resources — hence the `*::` (empty
  account). The `*` region wildcard covers whichever region(s) the `us.`
  profile happens to route to (`us-east-1`, `us-east-2`, `us-west-2` at the
  time of writing).
- **Inference-profile ARNs** (`inference-profile/...`) *do* include your
  account ID — the `*` there is a region wildcard, not an account wildcard;
  IAM still enforces the real account ID from the credentials making the
  call.

If you override `BEDROCK_MODEL` (different model family, different region
prefix, or a bare model ID instead of a cross-region profile), update both
ARNs to match — a mismatched policy is the most common cause of a Bedrock
`AccessDeniedException` here.

Attach this policy to an IAM user (or role, if running the containers
somewhere that can assume one) dedicated to this demo — least-privilege,
scoped to just this one model, rather than reusing a broader existing key.

### 3. Create the access key

AWS Console → **IAM** → **Users** → *(your demo user)* → **Security
credentials** → **Create access key** → **Application running outside AWS**.
Copy the Access Key ID and Secret Access Key into `demo/AI-demo/.env` (see
Setup below) — this is the only place they need to live; nothing here talks
to AWS from anywhere else.

### 4. Verify end-to-end before wiring up the demo

```bash
aws bedrock-runtime invoke-model \
  --region us-east-1 \
  --model-id us.anthropic.claude-haiku-4-5-20251001-v1:0 \
  --body '{"anthropic_version":"bedrock-2023-05-31","max_tokens":16,"messages":[{"role":"user","content":"say hi"}]}' \
  --cli-binary-format raw-in-base64-out \
  /dev/stdout
```

A JSON response with a `content` block back means both model access and the
IAM policy are correctly set up. An `AccessDeniedException` naming
`foundation-model/...` means step 1 (model access) isn't enabled yet; one
naming `inference-profile/...` or `foundation-model/...` in the *policy
error message* means step 2 (IAM policy) is missing that ARN.

If you deploy in a region other than `us-east-1`, list what's actually
available there and set `AWS_REGION`/`BEDROCK_MODEL` (and the corresponding
ARNs in the policy) to match — cross-region inference profile IDs are
region-family-specific (`us.`, `eu.`, `apac.`, ...):

```bash
aws bedrock list-inference-profiles --region <your-region> \
  --query "inferenceProfileSummaries[?contains(inferenceProfileId, 'haiku')].inferenceProfileId"
```

---

## Setup

Copy `demo/AI-demo/.env.example` to `demo/AI-demo/.env` and fill in your AWS
credentials:

```bash
cp demo/AI-demo/.env.example demo/AI-demo/.env
# then edit demo/AI-demo/.env and fill in AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY
```

`demo/AI-demo/.env` is gitignored and loaded into the `ai-webui`/`ai-cli`
containers via `env_file:` in `docker-compose.ai-demo.yml` — **not** the
repo-root `.env`. Compose's `${VAR}` substitution in a compose file only
reads the shell environment plus a top-level `.env` in the *project
directory* (the repo root, since `docker-compose.yml` is the first `-f`
file) — never a `.env` sitting next to a second `-f` file — which is why
these two services deliberately use `env_file:` instead.

Bring up the main stack plus the AI demo services (from the repo root):

```bash
docker compose -f docker-compose.yml -f demo/AI-demo/docker-compose.ai-demo.yml up -d
```

(Add any other services from the main stack you want, e.g. `control-center`.)

`siem-datagen` (see `datagen/` at the repo root) runs the producers +
streaming apps for you — it waits for `broker`/`schema-registry` to report
healthy, then starts `siem_producer.py` and the `demo/*_streaming_app.py`
apps in the order from the root README, 2s apart, so there's already data
for the agent to query.

---

## Option A — Claude Code (direct MCP connection)

This repo's root `.mcp.json` already declares the `siem-confluent` server,
pointed at `http://localhost:8080/mcp` (the `mcp-confluent` container's
published port — no auth needed, no tunnel needed, since everything's local):

```json
{
  "mcpServers": {
    "siem-confluent": {
      "type": "http",
      "url": "http://localhost:8080/mcp"
    }
  }
}
```

Just run `claude` from the repo root. On first use, Claude Code prompts you
to approve the project MCP server (since it's declared in `.mcp.json`, not
added via `claude mcp add`). Approve it, then ask things like *"what topics
are on the cluster?"* or *"show me the schema for the DNS logs topic"*.

> To skip the approval prompt, add `"siem-confluent"` to
> `enabledMcpjsonServers` in your `~/.claude/settings.json`.

---

## Option B — Web UI

Already started above via `docker compose ... up -d` — open
**http://localhost:5050**, enter any username (no password), and start
chatting.

---

## Option C — Terminal CLI

The `ai-cli` service isn't started by `up` (it's an interactive REPL, not a
long-running service) — run it directly:

```bash
docker compose -f docker-compose.yml -f demo/AI-demo/docker-compose.ai-demo.yml run --rm ai-cli
```

### Example session

```
 SIEM AI Agent  (type 'exit' or Ctrl-C to quit)

 Model     : us.anthropic.claude-haiku-4-5-20251001-v1:0
 MCP server: http://mcp-confluent:8080/mcp
 Tools     : consume-messages, list-schemas, list-topics

You: what topics are available?

Agent:
  → tool: list-topics  {}

  Here are the SIEM topics:
  - siem_poc_dns_logs-aggregate
  - siem_poc_fortigate_logs-event-system
  - siem_poc_fortigate_logs-traffic-forward
  ...

You: show me the last 5 DNS log messages

Agent:
  → tool: consume-messages  {"topic": "siem_poc_dns_logs-aggregate", "maxMessages": 5}

  Here are 5 recent DNS log entries: ...
```

---

## Stopping / tearing down

```bash
docker compose -f docker-compose.yml -f demo/AI-demo/docker-compose.ai-demo.yml down
```

---

## Files

```
demo/AI-demo/
├── core.py                    # Shared config: system prompt, allowed tools, Bedrock model
├── mcp_client.py              # Background-thread MCP (streamable-HTTP) client, shared by CLI + WebUI
├── agent.py                   # Option C — terminal chat REPL
├── webui/
│   ├── app.py                 # Option B — Flask backend (SSE streaming)
│   └── templates/index.html   # Single-file React frontend, no build step
├── mcp-server/
│   ├── Dockerfile             # Installs @confluentinc/mcp-confluent from npm
│   └── mcp-config.yaml        # Connection config (Kafka + Schema Registry)
├── Dockerfile                 # Shared Python image for agent.py / webui/app.py
├── requirements.txt
├── .env.example
├── README.md                  # This file
└── docker-compose.ai-demo.yml # mcp-confluent, ai-webui, ai-cli services
                                # + broker listener override for MCP auth
                                # (used as a second -f from the repo root — see Setup)

.mcp.json                      # (repo root) Claude Code Option A config
```
