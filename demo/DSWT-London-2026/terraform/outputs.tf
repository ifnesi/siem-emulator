# --------------------------------------------------------
# IDs / endpoints
# --------------------------------------------------------
output "cc_demo_env" {
  description = "CC Environment id (also the Flink catalog)."
  value       = confluent_environment.cc_demo_env.id
}

output "cc_kafka_cluster_id" {
  description = "Kafka cluster id (also the Flink database)."
  value       = confluent_kafka_cluster.cc_kafka_cluster.id
}

output "kafka_bootstrap" {
  description = "Bootstrap host:port for the datagen (.env CC_BOOTSTRAP)."
  value       = replace(confluent_kafka_cluster.cc_kafka_cluster.bootstrap_endpoint, "SASL_SSL://", "")
}

output "cc_sr_cluster_endpoint" {
  description = "Schema Registry REST endpoint (.env CC_SR_URL)."
  value       = data.confluent_schema_registry_cluster.cc_sr_cluster.rest_endpoint
}

output "flink_compute_pool_id" {
  description = "Flink compute pool id (lfcp-...) to run the suggested SQL on."
  value       = confluent_flink_compute_pool.flink_compute_pool.id
}

# --------------------------------------------------------
# Datagen credentials (clients + sr) — feed docker/.env
# --------------------------------------------------------
output "clients_kafka_cluster_key" {
  description = ".env CC_KAFKA_KEY"
  value       = confluent_api_key.clients_kafka_cluster_key.id
}

output "clients_kafka_cluster_secret" {
  description = ".env CC_KAFKA_SECRET"
  value       = confluent_api_key.clients_kafka_cluster_key.secret
  sensitive   = true
}

output "sr_cluster_key" {
  description = ".env CC_SR_KEY"
  value       = confluent_api_key.sr_cluster_key.id
}

output "sr_cluster_secret" {
  description = ".env CC_SR_SECRET"
  value       = confluent_api_key.sr_cluster_key.secret
  sensitive   = true
}

# --------------------------------------------------------
# Flink API key (console / confluent CLI)
# --------------------------------------------------------
output "flink_api_key" {
  description = "Flink API key id."
  value       = confluent_api_key.flink_api_key.id
}

output "flink_api_secret" {
  description = "Flink API secret."
  value       = confluent_api_key.flink_api_key.secret
  sensitive   = true
}

# --------------------------------------------------------
# Managed MCP (regional) for Claude Code (.mcp.json cc-managed-mcp)
# --------------------------------------------------------
output "mcp_url" {
  description = "Regional, cluster-scoped MCP endpoint (.env DSWT_CC_MCP_URL)."
  value       = local.mcp_url
}

output "mcp_reader_service_account" {
  description = "Create a GLOBAL API key for this SA, base64 it, and export as DSWT_CC_MCP_AUTH."
  value       = confluent_service_account.mcp_reader.id
}
