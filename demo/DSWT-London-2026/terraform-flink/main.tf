# =============================================================================
# DSWT London 2026 — Flink data products (CTAS statements).
#
# A SEPARATE root module so these DO NOT run during the infra `terraform apply`.
# The presenter applies them ONE BY ONE via the CLI, in dependency order:
#
#   terraform init
#   terraform apply -target=confluent_flink_statement.bronze_orders
#   terraform apply -target=confluent_flink_statement.bronze_payments
#   terraform apply -target=confluent_flink_statement.bronze_shipments
#   terraform apply -target=confluent_flink_statement.bronze_delivery_current
#   terraform apply -target=confluent_flink_statement.silver
#   terraform apply -target=confluent_flink_statement.alerts
#   terraform apply -target=confluent_flink_statement.exec_summary
#
# Each is one CTAS (CREATE TABLE ... AS SELECT) → one long-running Flink job that
# creates its backing topic. Reads the infra module's state for ids/keys.
# =============================================================================

data "terraform_remote_state" "infra" {
  backend = "local"
  config = {
    path = "${path.module}/../terraform/terraform.tfstate"
  }
}

locals {
  org_id       = data.terraform_remote_state.infra.outputs.organization_id
  env_id       = data.terraform_remote_state.infra.outputs.cc_demo_env
  cluster_id   = data.terraform_remote_state.infra.outputs.cc_kafka_cluster_id
  pool_id      = data.terraform_remote_state.infra.outputs.flink_compute_pool_id
  principal_id = data.terraform_remote_state.infra.outputs.app_manager_id
  flink_rest   = data.terraform_remote_state.infra.outputs.flink_rest_endpoint
  flink_key    = data.terraform_remote_state.infra.outputs.flink_api_key
  flink_secret = data.terraform_remote_state.infra.outputs.flink_api_secret

  # The statement's catalog/database context (env id / cluster id), so the SQL
  # can reference the raw dswt_* tables (and its own bronze_*/silver tables)
  # unqualified.
  props = {
    "sql.current-catalog"  = local.env_id
    "sql.current-database" = local.cluster_id
  }
}

# ── Bronze (4) ───────────────────────────────────────────────────────────────
resource "confluent_flink_statement" "bronze_orders" {
  organization { id = local.org_id }
  environment { id = local.env_id }
  compute_pool { id = local.pool_id }
  principal { id = local.principal_id }
  statement     = file("${path.module}/sql/bronze_orders.sql")
  properties    = local.props
  rest_endpoint = local.flink_rest
  credentials {
    key    = local.flink_key
    secret = local.flink_secret
  }
  lifecycle {
    prevent_destroy = false
  }
}

resource "confluent_flink_statement" "bronze_payments" {
  organization { id = local.org_id }
  environment { id = local.env_id }
  compute_pool { id = local.pool_id }
  principal { id = local.principal_id }
  statement     = file("${path.module}/sql/bronze_payments.sql")
  properties    = local.props
  rest_endpoint = local.flink_rest
  credentials {
    key    = local.flink_key
    secret = local.flink_secret
  }
  lifecycle {
    prevent_destroy = false
  }
}

resource "confluent_flink_statement" "bronze_shipments" {
  organization { id = local.org_id }
  environment { id = local.env_id }
  compute_pool { id = local.pool_id }
  principal { id = local.principal_id }
  statement     = file("${path.module}/sql/bronze_shipments.sql")
  properties    = local.props
  rest_endpoint = local.flink_rest
  credentials {
    key    = local.flink_key
    secret = local.flink_secret
  }
  lifecycle {
    prevent_destroy = false
  }
}

resource "confluent_flink_statement" "bronze_delivery_current" {
  organization { id = local.org_id }
  environment { id = local.env_id }
  compute_pool { id = local.pool_id }
  principal { id = local.principal_id }
  statement     = file("${path.module}/sql/bronze_delivery_current.sql")
  properties    = local.props
  rest_endpoint = local.flink_rest
  credentials {
    key    = local.flink_key
    secret = local.flink_secret
  }
  lifecycle {
    prevent_destroy = false
  }
}

# ── Silver (needs the 4 bronze_* + raw dswt_customers) ───────────────────────
resource "confluent_flink_statement" "silver" {
  organization { id = local.org_id }
  environment { id = local.env_id }
  compute_pool { id = local.pool_id }
  principal { id = local.principal_id }
  statement     = file("${path.module}/sql/silver_order_fulfillment.sql")
  properties    = local.props
  rest_endpoint = local.flink_rest
  credentials {
    key    = local.flink_key
    secret = local.flink_secret
  }
  depends_on = [
    confluent_flink_statement.bronze_orders,
    confluent_flink_statement.bronze_payments,
    confluent_flink_statement.bronze_shipments,
    confluent_flink_statement.bronze_delivery_current,
  ]
  lifecycle {
    prevent_destroy = false
  }
}

# ── At-risk detector (needs silver) ──────────────────────────────────────────
resource "confluent_flink_statement" "alerts" {
  organization { id = local.org_id }
  environment { id = local.env_id }
  compute_pool { id = local.pool_id }
  principal { id = local.principal_id }
  statement     = file("${path.module}/sql/alerts_at_risk.sql")
  properties    = local.props
  rest_endpoint = local.flink_rest
  credentials {
    key    = local.flink_key
    secret = local.flink_secret
  }
  depends_on = [confluent_flink_statement.silver]
  lifecycle {
    prevent_destroy = false
  }
}

# ── Executive summary (reads raw dswt_orders directly — no bronze dep) ────────
resource "confluent_flink_statement" "exec_summary" {
  organization { id = local.org_id }
  environment { id = local.env_id }
  compute_pool { id = local.pool_id }
  principal { id = local.principal_id }
  statement     = file("${path.module}/sql/exec_summary_hourly.sql")
  properties    = local.props
  rest_endpoint = local.flink_rest
  credentials {
    key    = local.flink_key
    secret = local.flink_secret
  }
  lifecycle {
    prevent_destroy = false
  }
}
