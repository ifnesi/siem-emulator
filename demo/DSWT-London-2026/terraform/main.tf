# =============================================================================
# DSWT London 2026 infrastructure on Confluent Cloud.
#   organization + environment + Kafka cluster + Schema Registry + Flink pool
#   + 7 raw topics + Avro schemas + RTCE (context-engine) + SAs / RBAC / keys.
#
# Deliberately NOT here (this is the live demo part):
#   * confluent_flink_statement — the presenter applies Claude's suggested Flink
#     SQL (bronze/silver/anomalies/exec_summary) on the pool; see ../flink/.
# =============================================================================

data "confluent_organization" "cc_org" {}

# Regional, cluster-scoped managed MCP endpoint that Claude Code connects to.
# Format: https://mcp.<region>.<cloud>.confluent.cloud/mcp/v1/context-engine/
#         organizations/<org>/environments/<env>/kafka-clusters/<lkc>
locals {
  mcp_url = format(
    "https://mcp.%s.%s.confluent.cloud/mcp/v1/context-engine/organizations/%s/environments/%s/kafka-clusters/%s",
    var.cc_cloud_region,
    lower(var.cc_cloud_provider),
    data.confluent_organization.cc_org.id,
    confluent_environment.cc_demo_env.id,
    confluent_kafka_cluster.cc_kafka_cluster.id,
  )
}

# ── Environment + Stream Governance (Schema Registry) ───────────────────────
resource "confluent_environment" "cc_demo_env" {
  display_name = "${var.cc_env_name}-${random_id.id.hex}"
  stream_governance {
    package = var.stream_governance
  }
  lifecycle {
    prevent_destroy = false
  }
}

# ── Kafka cluster ───────────────────────────────────────────────────────────
resource "confluent_kafka_cluster" "cc_kafka_cluster" {
  display_name = var.cc_cluster_name
  availability = var.cc_availability
  cloud        = var.cc_cloud_provider
  region       = var.cc_cloud_region
  # Standard (not Basic) — resource-scoped role bindings (topic=*/subject=*)
  # for the read-only MCP principal require Standard or above.
  standard {}
  environment {
    id = confluent_environment.cc_demo_env.id
  }
  lifecycle {
    prevent_destroy = false
  }
}

data "confluent_schema_registry_cluster" "cc_sr_cluster" {
  environment {
    id = confluent_environment.cc_demo_env.id
  }
  depends_on = [confluent_kafka_cluster.cc_kafka_cluster]
}

# ── Flink compute pool (presenter runs the suggested SQL here) ───────────────
data "confluent_flink_region" "flink_region" {
  cloud  = var.cc_cloud_provider
  region = var.cc_cloud_region
}

resource "confluent_flink_compute_pool" "flink_compute_pool" {
  display_name = "dswt-pool-${random_id.id.hex}"
  cloud        = var.cc_cloud_provider
  region       = var.cc_cloud_region
  max_cfu      = var.flink_cfu
  environment {
    id = confluent_environment.cc_demo_env.id
  }
}

# ── Service accounts ────────────────────────────────────────────────────────
resource "confluent_service_account" "app_manager" {
  display_name = "app-manager-${random_id.id.hex}"
  description  = "Application Manager Service Account (topics + Flink)"
  lifecycle {
    prevent_destroy = false
  }
}

resource "confluent_service_account" "sr" {
  display_name = "sr-${random_id.id.hex}"
  description  = "Schema Registry Service Account"
  lifecycle {
    prevent_destroy = false
  }
}

resource "confluent_service_account" "clients" {
  display_name = "client-${random_id.id.hex}"
  description  = "Kafka Clients Service Account (siem-emulator datagen)"
  lifecycle {
    prevent_destroy = false
  }
}

resource "confluent_service_account" "mcp_reader" {
  display_name = "mcp-reader-${random_id.id.hex}"
  description  = "Read-only principal for the Confluent Cloud managed MCP server"
  lifecycle {
    prevent_destroy = false
  }
}

# ── Role bindings ───────────────────────────────────────────────────────────
resource "confluent_role_binding" "app_manager_environment_admin" {
  principal   = "User:${confluent_service_account.app_manager.id}"
  role_name   = "EnvironmentAdmin"
  crn_pattern = confluent_environment.cc_demo_env.resource_name
}

resource "confluent_role_binding" "sr_environment_admin" {
  principal   = "User:${confluent_service_account.sr.id}"
  role_name   = "EnvironmentAdmin"
  crn_pattern = confluent_environment.cc_demo_env.resource_name
}

resource "confluent_role_binding" "clients_cluster_admin" {
  principal   = "User:${confluent_service_account.clients.id}"
  role_name   = "CloudClusterAdmin"
  crn_pattern = confluent_kafka_cluster.cc_kafka_cluster.rbac_crn
}

# Managed MCP principal: read-only on topics + subjects, and can enumerate the
# environment/clusters for discovery.
resource "confluent_role_binding" "mcp_read_topics" {
  principal   = "User:${confluent_service_account.mcp_reader.id}"
  role_name   = "DeveloperRead"
  crn_pattern = "${confluent_kafka_cluster.cc_kafka_cluster.rbac_crn}/kafka=${confluent_kafka_cluster.cc_kafka_cluster.id}/topic=*"
}

resource "confluent_role_binding" "mcp_read_subjects" {
  principal   = "User:${confluent_service_account.mcp_reader.id}"
  role_name   = "DeveloperRead"
  crn_pattern = "${data.confluent_schema_registry_cluster.cc_sr_cluster.resource_name}/subject=*"
}

# DataDiscovery = valid environment-scoped, read-only role that lets the MCP
# principal enumerate the environment and read schema/topic metadata.
resource "confluent_role_binding" "mcp_data_discovery" {
  principal   = "User:${confluent_service_account.mcp_reader.id}"
  role_name   = "DataDiscovery"
  crn_pattern = confluent_environment.cc_demo_env.resource_name
}

# NOTE: the managed MCP server authenticates with a GLOBAL API key that the
# operator creates and supplies themselves (exported as DSWT_CC_MCP_AUTH), NOT a
# key rendered by Terraform. Create that global key for this mcp_reader service
# account (console/CLI) so it inherits the read-only RBAC above — see the README.

# ── API keys ────────────────────────────────────────────────────────────────
resource "confluent_api_key" "app_manager_kafka_cluster_key" {
  display_name = "app-manager-${var.cc_cluster_name}-key-${random_id.id.hex}"
  description  = "Application Manager Kafka API Key"
  owner {
    id          = confluent_service_account.app_manager.id
    api_version = confluent_service_account.app_manager.api_version
    kind        = confluent_service_account.app_manager.kind
  }
  managed_resource {
    id          = confluent_kafka_cluster.cc_kafka_cluster.id
    api_version = confluent_kafka_cluster.cc_kafka_cluster.api_version
    kind        = confluent_kafka_cluster.cc_kafka_cluster.kind
    environment {
      id = confluent_environment.cc_demo_env.id
    }
  }
  depends_on = [confluent_role_binding.app_manager_environment_admin]
  lifecycle {
    prevent_destroy = false
  }
}

resource "confluent_api_key" "sr_cluster_key" {
  display_name = "sr-${var.cc_cluster_name}-key-${random_id.id.hex}"
  description  = "Schema Registry API Key"
  owner {
    id          = confluent_service_account.sr.id
    api_version = confluent_service_account.sr.api_version
    kind        = confluent_service_account.sr.kind
  }
  managed_resource {
    id          = data.confluent_schema_registry_cluster.cc_sr_cluster.id
    api_version = data.confluent_schema_registry_cluster.cc_sr_cluster.api_version
    kind        = data.confluent_schema_registry_cluster.cc_sr_cluster.kind
    environment {
      id = confluent_environment.cc_demo_env.id
    }
  }
  depends_on = [
    confluent_role_binding.sr_environment_admin,
    data.confluent_schema_registry_cluster.cc_sr_cluster,
  ]
  lifecycle {
    prevent_destroy = false
  }
}

resource "confluent_api_key" "clients_kafka_cluster_key" {
  display_name = "clients-${var.cc_cluster_name}-key-${random_id.id.hex}"
  description  = "Kafka Clients (datagen) API Key"
  owner {
    id          = confluent_service_account.clients.id
    api_version = confluent_service_account.clients.api_version
    kind        = confluent_service_account.clients.kind
  }
  managed_resource {
    id          = confluent_kafka_cluster.cc_kafka_cluster.id
    api_version = confluent_kafka_cluster.cc_kafka_cluster.api_version
    kind        = confluent_kafka_cluster.cc_kafka_cluster.kind
    environment {
      id = confluent_environment.cc_demo_env.id
    }
  }
  depends_on = [confluent_role_binding.clients_cluster_admin]
  lifecycle {
    prevent_destroy = false
  }
}

resource "confluent_api_key" "flink_api_key" {
  display_name = "flink-${var.cc_cluster_name}-key-${random_id.id.hex}"
  description  = "Flink API Key"
  owner {
    id          = confluent_service_account.app_manager.id
    api_version = confluent_service_account.app_manager.api_version
    kind        = confluent_service_account.app_manager.kind
  }
  managed_resource {
    id          = data.confluent_flink_region.flink_region.id
    api_version = data.confluent_flink_region.flink_region.api_version
    kind        = data.confluent_flink_region.flink_region.kind
    environment {
      id = confluent_environment.cc_demo_env.id
    }
  }
  depends_on = [confluent_role_binding.app_manager_environment_admin]
  lifecycle {
    prevent_destroy = false
  }
}

# ── Raw topics (silver/alerts are created live by Flink SQL) ─────────────────
resource "confluent_kafka_topic" "raw" {
  for_each = var.raw_topics

  kafka_cluster {
    id = confluent_kafka_cluster.cc_kafka_cluster.id
  }
  topic_name       = each.key
  partitions_count = each.value.partitions
  rest_endpoint    = confluent_kafka_cluster.cc_kafka_cluster.rest_endpoint
  credentials {
    key    = confluent_api_key.app_manager_kafka_cluster_key.id
    secret = confluent_api_key.app_manager_kafka_cluster_key.secret
  }
  config = {
    "cleanup.policy"      = each.value.cleanup_policy
    "min.insync.replicas" = "2"
    "retention.ms"        = each.value.retention_ms
  }
  lifecycle {
    prevent_destroy = false
  }
}

# ── Avro schemas for each raw topic (registered AFTER the topics) ────────────
# Generated from the templates with `siem_producer.py --inferred-schema`
# (schemas/dswt_*.avsc). The datagen pins the SAME files via --schema so what it
# produces matches exactly. Registering here (not just relying on the producer's
# auto-register) is what lets RTCE be enabled at apply time.
resource "confluent_schema" "raw" {
  for_each = var.raw_topics

  schema_registry_cluster {
    id = data.confluent_schema_registry_cluster.cc_sr_cluster.id
  }
  rest_endpoint = data.confluent_schema_registry_cluster.cc_sr_cluster.rest_endpoint
  subject_name  = "${each.key}-value"
  format        = "AVRO"
  schema        = file("${path.module}/../../../schemas/${each.key}.avsc")
  credentials {
    key    = confluent_api_key.sr_cluster_key.id
    secret = confluent_api_key.sr_cluster_key.secret
  }
  depends_on = [confluent_kafka_topic.raw]
  lifecycle {
    prevent_destroy = false
  }
}

# ── Real-Time Context Engine (RTCE) per topic ────────────────────────────────
# Makes each topic available to the managed MCP context-engine endpoint. Requires
# a registered schema (above) and an RTCE-supported cluster/region.
resource "confluent_rtce_topic" "raw" {
  for_each = var.raw_topics

  cloud       = var.cc_cloud_provider
  region      = var.cc_cloud_region
  topic_name  = each.key
  description = "DSWT London 2026 — ${each.key}"

  environment {
    id = confluent_environment.cc_demo_env.id
  }
  kafka_cluster {
    id = confluent_kafka_cluster.cc_kafka_cluster.id
  }

  depends_on = [confluent_schema.raw]
  lifecycle {
    prevent_destroy = false
  }
}

# ── Render producer connection files from the outputs ───────────────────────
resource "local_file" "cc_kafka_properties" {
  count    = var.write_properties_files ? 1 : 0
  filename = "${path.module}/../../../kafka/cc-kafka.properties"
  content  = <<-EOT
    # Rendered by demo/DSWT-London-2026/terraform — DO NOT COMMIT (live key).
    bootstrap.servers=${replace(confluent_kafka_cluster.cc_kafka_cluster.bootstrap_endpoint, "SASL_SSL://", "")}
    security.protocol=SASL_SSL
    sasl.mechanisms=PLAIN
    sasl.username=${confluent_api_key.clients_kafka_cluster_key.id}
    sasl.password=${confluent_api_key.clients_kafka_cluster_key.secret}
  EOT
}

resource "local_file" "cc_sr_properties" {
  count    = var.write_properties_files ? 1 : 0
  filename = "${path.module}/../../../kafka/cc-sr.properties"
  content  = <<-EOT
    # Rendered by demo/DSWT-London-2026/terraform — DO NOT COMMIT (live key).
    schemaRegistryURL=${data.confluent_schema_registry_cluster.cc_sr_cluster.rest_endpoint}
    basic.auth.user.info=${confluent_api_key.sr_cluster_key.id}:${confluent_api_key.sr_cluster_key.secret}
    auto.register.schemas=true
  EOT
}

# ── Render the demo .env directly (docker compose + Claude MCP endpoint) ─────
resource "local_file" "dswt_env" {
  count           = var.write_env_file ? 1 : 0
  filename        = "${path.module}/../.env"
  file_permission = "0600"
  content         = <<-EOT
    # Rendered by demo/DSWT-London-2026/terraform — DO NOT COMMIT (live keys).
    # Datagen (Confluent Cloud Kafka — clients key):
    CC_BOOTSTRAP=${replace(confluent_kafka_cluster.cc_kafka_cluster.bootstrap_endpoint, "SASL_SSL://", "")}
    CC_KAFKA_KEY=${confluent_api_key.clients_kafka_cluster_key.id}
    CC_KAFKA_SECRET=${confluent_api_key.clients_kafka_cluster_key.secret}
    # Datagen (Schema Registry — sr key):
    CC_SR_URL=${data.confluent_schema_registry_cluster.cc_sr_cluster.rest_endpoint}
    CC_SR_KEY=${confluent_api_key.sr_cluster_key.id}
    CC_SR_SECRET=${confluent_api_key.sr_cluster_key.secret}
    # Claude Code managed-MCP (regional, cluster-scoped) — `source .env` before `claude`.
    # .mcp.json references $${DSWT_CC_MCP_URL} and $${DSWT_CC_MCP_AUTH}.
    DSWT_CC_MCP_URL=${local.mcp_url}
    # DSWT_CC_MCP_AUTH is NOT generated here — create a GLOBAL API key in Confluent
    # Cloud (ideally for the mcp-reader service account) and set it yourself, e.g.:
    #   export DSWT_CC_MCP_AUTH="$(printf '%s:%s' <GLOBAL_KEY> <GLOBAL_SECRET> | base64)"
    # Reference (not read by the container):
    # FLINK_COMPUTE_POOL=${confluent_flink_compute_pool.flink_compute_pool.id}
    # FLINK_API_KEY=${confluent_api_key.flink_api_key.id}
  EOT
}
