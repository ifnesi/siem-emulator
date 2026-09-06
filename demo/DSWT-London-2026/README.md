# DSWT London 2026 — Claude Code × Confluent Cloud

A live demo showing **Claude Code** reading real Kafka streams from
**Confluent Cloud** through the **managed MCP server**, discovering the
relationships between them, and **suggesting the Flink SQL** to build new data
products on top — raw → bronze → silver, an at‑risk detector, and an executive
summary.

> **The pitch:** data streams are a better foundation to build from — and this is
> how a data scientist or architect explores and productizes a stream without
> leaving their terminal.

---

## Architecture

```mermaid
flowchart LR
  subgraph Local["Your laptop"]
    DG["siem-emulator datagen\n(Docker) — 7 Jinja2 templates"]
    CC_CLI["Claude Code\n(.mcp.json)"]
  end
  subgraph Cloud["Confluent Cloud (Terraform-provisioned)"]
    T["7 raw topics\ncustomers · products · orders · order_items\nshipments · delivery_status · payments"]
    MCP["Managed MCP server\n(read-only)"]
    FLINK["Flink compute pool"]
    SILVER["medal_silver_order_fulfillment\n+ report_alerts_at_risk\n(Terraform CTAS)"]
  end

  DG -- "produce (Avro)" --> T
  CC_CLI -- "read: list/describe/consume/schemas" --> MCP
  MCP --- T
  CC_CLI -. "suggests Flink SQL" .-> FLINK
  FLINK --> SILVER
  MCP --- SILVER
```

- **Terraform** provisions everything in one apply: the environment, cluster,
  Flink pool, the 7 raw topics, their **Avro schemas**, **RTCE** (Real‑Time
  Context Engine) on the raw topics, the **Flink data products** (CTAS in
  `terraform/sql/`), and the service accounts, RBAC and API keys.
- **Datagen (Docker)** pumps the seven related e‑commerce streams into Confluent
  Cloud, pinning the same `schemas/dswt_*.avsc` Terraform registered (`--schema`).
- **Claude Code** connects to the **managed MCP server** (read‑only) to explore
  and **suggests** the Flink SQL — the same SQL Terraform provisions.

The managed MCP server is read‑only by design (`list_kafka_topics`,
`describe_kafka_topic`, `consume_kafka_messages`, `list_schema_subjects`,
`read_schema_subject`). **RTCE is what makes topics visible to the context‑engine
MCP endpoint**, which is why Terraform enables it on the raw topics (each needs a
registered schema first). The derived Flink tables are *not* RTCE‑enabled.

---

## The data (what Claude will discover)

Seven streams with real referential integrity, so the joins reconcile exactly:

| Topic | Kind | Key | Links via |
|---|---|---|---|
| `dswt_customers` | dimension (compacted, 1000) | `customer_id` | — |
| `dswt_products` | dimension (compacted, 100) | `product_id` | — |
| `dswt_orders` | fact | `order_id` | `customer_id` → customers |
| `dswt_order_items` | fact (fan‑out, 2–5 rows/order) | `order_id` | `order_id` → orders, `product_id` → products |
| `dswt_shipments` | fact (the promise) | `order_id` | `order_id` → orders |
| `dswt_delivery_status` | fact (stateless status events, lifecycle/order) | `order_id` | `order_id` → orders/shipments |
| `dswt_payments` | fact | `order_id` | `order_id` → orders |

Three deliberately discoverable relationships make the "wow" land — all exact:

- **`order_items.unit_price == products.unit_price`** for the joined `product_id`.
- **`Σ order_items.line_total == orders.order_total`** per `order_id` (each order
  fans out into a random 2–5 line items that sum to its total).
- **`payments.amount == orders.order_total`** for the same `order_id`.

They come from seeded, deterministic helpers in `siem_producer.py`
(`seeded_integer` / `seeded_floating` / `seeded_choice`): every line item is a
pure function of `(order_id, line)`, so independent producer processes derive
identical values from a shared key and the totals reconcile across topics — see
the shared item model documented in `templates/dswt_orders.j2`. ~1 in 10 payments
are `DECLINED`.

**Delivery is modelled the event‑driven way.** `dswt_shipments` carries the
promise (`promised_days`, `shipped_ts`); `dswt_delivery_status` is a *stateless*
event stream that only reports statuses as they happen. It never carries
`actual_days` or `is_late` — **lateness is computed in Flink** (silver) from the
`DELIVERED` event vs `shipped_ts + promised_days`; ~1 in 6 delivered orders are
late. **Delivery is gated on the payment outcome:** `CAPTURED` orders run
`CREATED → IN_TRANSIT → OUT_FOR_DELIVERY → DELIVERED`; `DECLINED` orders mostly
`CANCELLED` (`… → PAYMENT_FLAGGED → ON_HOLD → CANCELLED`), but **~1 in 8 leak
through to `DELIVERED`** — the shipped-despite-declined revenue leak the demo
finds live.

**One coherent per‑order clock.** Every event of an order — placed → paid
(minutes later) → shipped (+2 h) → delivered (over the promised/actual days) — is
a deterministic function of `order_id`, so the timestamps line up *across the
independent producers* and elapsed times are real (Claude can say "declined 2 min
after the order, delivered 5 days later"). See the shared timeline in
`templates/dswt_orders.j2`.

**Referential integrity comes from a bounded, closed keyspace** — not a
coordinator. There are exactly 1000 customers, 100 products (both bulk‑loaded
before any order flows), and `NUM_ORDERS` orders; every child references an id
inside those bounds, and each producer runs `-n` and stops at its limit, so
nothing is ever orphaned. Fan‑out streams get an exact `-n`: `order_items` = the
deterministic sum of `item_count`, and `delivery_status` = `4 × NUM_ORDERS`.

### Real-time flow and volume

The flow is sized by two knobs: `ORDERS_PER_SEC` (default **5**) and `RUN_HOURS`
(default **24**), giving `NUM_ORDERS = ORDERS_PER_SEC × RUN_HOURS × 3600` =
**432,000**. The five fact streams flow at **rate‑matched** speeds so they
progress through the order keyspace together, then stop at ~24h:

| Topic | Messages | Rate | Formula |
|---|---:|---:|---|
| `dswt_customers` | 1,000 | bulk | fixed (loaded first) |
| `dswt_products` | 100 | bulk | fixed (loaded first) |
| `dswt_orders` | 432,000 | 5/s | `NUM_ORDERS` |
| `dswt_order_items` | 1,512,947 | 17/s | Σ `item_count` (2–5), avg 3.502/order |
| `dswt_shipments` | 432,000 | 5/s | `NUM_ORDERS` |
| `dswt_delivery_status` | 1,728,000 | 20/s | `4 × NUM_ORDERS` |
| `dswt_payments` | 432,000 | 5/s | `NUM_ORDERS` |
| **Total** | **4,538,047** | ~47/s | |

The `order_items` count is exact and reproducible — the entrypoint computes
`Σ item_count` with the same seeded helper the template uses. Override
`ORDERS_PER_SEC`, `RUN_HOURS`, or `NUM_ORDERS` to change throughput/duration
(e.g. `NUM_ORDERS=5000 docker compose up` for a quick test run).

```mermaid
erDiagram
  CUSTOMERS ||--o{ ORDERS : places
  ORDERS ||--o{ ORDER_ITEMS : contains
  PRODUCTS ||--o{ ORDER_ITEMS : "referenced by"
  ORDERS ||--|| PAYMENTS : "paid by"
  ORDERS ||--|| SHIPMENTS : "shipped as"
  SHIPMENTS ||--o{ DELIVERY_STATUS : "tracked by"
```

### The Flink data products (what Terraform builds on top)

The seven raw streams above are refined into a medallion of Flink tables — each a
`CREATE TABLE … AS SELECT` in `terraform/sql/`, applied by `terraform apply`. Raw
→ **bronze** (clean/dedupe) → **silver** (enriched join + computed lateness and
the revenue-leak flag) → **report** (the at-risk detector and the exec summary):

```mermaid
flowchart LR
  subgraph Raw["Raw topics (datagen → Kafka)"]
    RO["dswt_orders"]
    RP["dswt_payments"]
    RS["dswt_shipments"]
    RD["dswt_delivery_status"]
    RC["dswt_customers"]
  end
  subgraph Bronze["Bronze — cleaned / deduped"]
    BO["medal_bronze_orders"]
    BP["medal_bronze_payments"]
    BS["medal_bronze_shipments"]
    BD["medal_bronze_delivery_current\n(current status per order)"]
  end
  subgraph Silver["Silver — enriched join"]
    SF["medal_silver_order_fulfillment\n+ is_late (computed)\n+ delivered_despite_decline"]
  end
  subgraph Report["Report — data products"]
    RA["report_alerts_at_risk"]
    RE["report_exec_summary_hourly\n(hourly TUMBLE by channel)"]
  end

  RO --> BO --> SF
  RP --> BP --> SF
  RS --> BS --> SF
  RD --> BD --> SF
  RC --> SF
  SF --> RA
  RO -. "reads raw rowtime\n(no bronze dep)" .-> RE
```

`report_exec_summary_hourly` reads the raw `dswt_orders` directly (Kafka
`$rowtime` as event time), so it has no bronze dependency; everything else follows
the `depends_on` chain bronze → silver → alerts.

---

## Prerequisites

- Terraform ≥ 1.5, Docker, and a Confluent Cloud account.
- A **Cloud API key** (OrganizationAdmin/EnvironmentAdmin) for Terraform.
- Claude Code, run from the **repo root** (where `.mcp.json` lives).

---

## Act 0 — Setup (off stage, ~10 min + a few min for data)

### 1. Provision Confluent Cloud

```bash
cd demo/DSWT-London-2026/terraform
cp terraform.tfvars.example terraform.tfvars      # adjust cloud/region if needed
export CONFLUENT_CLOUD_API_KEY="..."              # provider auth (env, not on disk)
export CONFLUENT_CLOUD_API_SECRET="..."
terraform init
terraform plan
terraform apply -auto-approve
```

Terraform writes everything the demo needs (all git‑ignored):

- `kafka/cc-kafka.properties` + `kafka/cc-sr.properties` at the repo root, and
- **`demo/DSWT-London-2026/.env`** — the datagen CC connection plus the Claude MCP
  **endpoint** (`DSWT_CC_MCP_URL`). The MCP **token** (`DSWT_CC_MCP_AUTH`) is *not*
  written by Terraform — you set it yourself from a Global API key (see step 3).

`terraform output flink_compute_pool_id` gives the pool id for the Flink SQL step.

### 2. Start the datagen (Docker)

```bash
cd .. # Go back to demo/DSWT-London-2026
docker compose up --build -d     # reads the Terraform-generated .env
```

It **bulk-loads the dimensions first** (1000 customers + 100 products), then
**streams the facts in real time** — ~5 orders/sec (with their order_items,
shipments, delivery-status lifecycle and payments at rate-matched speeds) for
~24h, then stops. Every producer walks the same closed `order_id` keyspace
`[0, NUM_ORDERS-1]` and stops at its limit, so no child is ever orphaned (its
order/customer/product is in the set). Tune with `ORDERS_PER_SEC` / `RUN_HOURS`
(see the volume table above). Leave it running for the event — the topics keep
flowing so the demo shows live data.

### 3. Point Claude Code at the managed MCP server

The `cc-managed-mcp` server is declared in the **repo‑root `.mcp.json`** (Claude
Code reads project MCP config only there — not from `.claude/`). It's generic and
resolves from two env vars:

```jsonc
"cc-managed-mcp": {
  "type": "http",
  "url": "${DSWT_CC_MCP_URL}",                        // regional, cluster-scoped endpoint
  "headers": { "Authorization": "Basic ${DSWT_CC_MCP_AUTH}" }
}
```

- **`DSWT_CC_MCP_URL`** — Terraform builds and writes this into `.env`. It's the
  **regional, cluster-scoped** endpoint:
  `https://mcp.<region>.<cloud>.confluent.cloud/mcp/v1/context-engine/organizations/<org>/environments/<env>/kafka-clusters/<lkc>`
- **`DSWT_CC_MCP_AUTH`** — **you set this yourself** (Terraform does not generate
  it). It's `base64(<key>:<secret>)` of a **Global API key** in Confluent Cloud.
  Create one in the console (Cloud API keys → Add key → **Global**) with owner =
  the `mcp-reader` service account Terraform made
  (`terraform output mcp_reader_service_account`) so it inherits the read‑only
  RBAC. You export it in the launch step below.

Claude Code expands `${VAR}` in both `url` and `headers`. Source `.env` (URL) and
export the auth, then launch from the repo root:

```bash
set -a; source .env; set +a       # sets DSWT_CC_MCP_URL
export DSWT_CC_MCP_AUTH="$(printf '%s:%s' <GLOBAL_KEY> <GLOBAL_SECRET> | base64)"
claude
```

Project MCP servers need approval on first run — approve `cc-managed-mcp` when
prompted (or set `"enableAllProjectMcpServers": true` in `.claude/settings.json`
to skip the prompt for rehearsals). Verify:
> *"Using cc-managed-mcp, list the topics on the cluster."*

You should see the seven `dswt_*` topics.

---

## The demo (Acts 1–3)

Exact prompts to type into Claude Code. (Tell it to use `cc-managed-mcp`.)

### Act 1 — Discovery
> *"You have read‑only access to a Confluent Cloud cluster via the cc-managed-mcp
> server. List the topics, look at a few sample messages and the schemas, and
> tell me what business this data is from and what each stream represents."*

Claude enumerates the topics, consumes samples, reads schemas, and infers an
e‑commerce order‑management domain — from the raw streams alone.

### Act 2 — Relationships
> *"Find the relationships between these streams. What are the join keys? Draw the
> data model as a mermaid ER diagram, and check whether any fields reconcile
> across streams."*

Claude identifies `order_id` and `customer_id`/`product_id`, draws the ERD, and
(the money moment) notices **`payments.amount` equals `orders.order_total`** and
**line prices match the product catalog**.

### Act 3 — Live discovery (a data scientist feeling out the streams)
Three quick questions. Each is a **cross‑stream lookup** the MCP does with plain
reads — filter one stream, resolve against another, no aggregation. The last one
hands the work to Flink.

**1. The leak**
> *"Which recent orders had their payment declined but we delivered them
> anyway?"*

Claude filters payments for `DECLINED` and checks each against delivery status —
surfacing the ~1‑in‑8 declined orders that slipped through to `DELIVERED` (the
rest are `CANCELLED`). A live "we're fulfilling orders whose card failed" moment.

**2. Who got hurt**
> *"Which of our gold or platinum customers had a late delivery or a declined
> payment recently?"*

Correlates orders → customer (loyalty tier) → delivery / payment. Names the VIPs
having a bad experience — the shortlist to make things right with.

**3. Now find it at scale → Flink**
> *"Sampling only sees the newest slice — write me the Flink SQL that finds every
> declined‑but‑delivered order, continuously."*

The teaching beat: the MCP is for **discovery**, Flink is for **the real
numbers**. Claude hands you a `SELECT` that joins payments to delivery and filters
— exactly the shape of the `report_*` tables `terraform/sql/` already deploys.

> **Why the split?** The managed MCP is read‑only — no `COUNT`/`SUM`/`GROUP BY`/
> `JOIN`, ~200 rows/call. That's the point: live reads for *exploring and tracing*
> streams; **aggregation belongs upstream in Flink**. (Bonus closer if you have
> time: *"Pick one order and tell me its whole story across the streams."*)

---

## Fallbacks (live‑with‑a‑net)

- **Pre‑tested SQL** in `terraform/sql/` — already applied by `terraform apply`;
  paste a file in the console if a live suggestion needs a fix.
- **Infra already applied** — never run `terraform apply` on stage.
- **Screen recording** of a full clean run, in case the venue network fails.
- **Safe mode:** the repo's local OSS‑MCP demo (`demo/AI-demo/`) runs entirely
  offline against a local cluster if Confluent Cloud is unreachable.

## Rehearsal checklist

- [ ] `terraform apply` clean; outputs captured.
- [ ] `docker compose up` — all seven topics have data in the CC console.
- [ ] `DSWT_CC_MCP_AUTH` exported; Claude lists the seven topics via `cc-managed-mcp`.
- [ ] `payments.amount == orders.order_total` verified live (Act 2).
- [ ] `terraform apply` created the 7 CTAS data products; `medal_silver_order_fulfillment`
      is populating in the Flink console (they read `$rowtime`/data as it flows).
- [ ] Full Acts 1–3 run on the clock; recording captured.

## Teardown

```bash
cd demo/DSWT-London-2026 && docker compose down
cd terraform && terraform destroy -auto-approve
```

(If `destroy` says "no changes" but resources still exist, run `terraform init`
first — a config edit can require re-init before the state is readable.)

## Re-provisioning (destroy → re-apply) and reconnecting Claude

`terraform destroy` then `apply` creates a **new environment + cluster**, so the
`<env>`/`<lkc>` in `DSWT_CC_MCP_URL` change (org/region/cloud stay the same).
Terraform re-writes `.env` with the new URL, but:

- **Claude does not auto-reconnect.** It expands `${DSWT_CC_MCP_URL}` at *launch*,
  so a running session keeps the old endpoint even after `.env` changes. **Exit
  Claude, re-source the new `.env`, and start `claude` again.**
- **No re-approval needed** — `.mcp.json` text is unchanged (it's `${VAR}`), so the
  project-server approval persists across re-applies.
- **Global API key:** if you created it for the Terraform-managed `mcp-reader` SA,
  `destroy` deletes that SA and invalidates the key — you'd make a new one. **To
  avoid this every cycle, create the Global key for a *persistent* principal** (your
  user, or a long-lived SA outside this Terraform); then only the URL changes.
- **Datagen restart only** (`docker compose down && up`) does **not** change the
  cluster/URL — Claude stays connected; nothing to do.

## Security notes

- `.env`, `kafka/cc-*.properties`, `terraform.tfvars`, and Terraform state are
  git‑ignored — keep the filled‑in versions off git.
- The MCP reader is a least‑privilege, read‑only service account.
- Rotate/destroy the demo API keys after the event (`terraform destroy` removes
  the service accounts and keys).

## Files

```
demo/DSWT-London-2026/
├── README.md                 # this runbook
├── .env                      # generated by Terraform (git-ignored)
├── docker-compose.yml        # runs the datagen against Confluent Cloud
├── docker/
│   ├── Dockerfile            # datagen image (build context = repo root)
│   └── entrypoint.sh         # bulk-loads dims, runs the 5 fact streams
└── terraform/                # ONE apply provisions everything:
    ├── providers.tf  vars.tf  main.tf  outputs.tf  terraform.tfvars.example
    └── sql/                   # data products as CTAS: medal_bronze_orders/payments/
                               #   shipments/delivery_current, silver_order_
                               #   fulfillment, report_alerts_at_risk, report_exec_summary_hourly
```

Datagen internals live at the repo root: `templates/dswt_*.j2`,
`templates/data/*`, `schemas/dswt_*.avsc` (registered by Terraform + pinned by
the producer), `siem_producer.py`.
