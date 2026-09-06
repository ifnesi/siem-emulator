locals {
  description = "Resource created using terraform — DSWT London 2026 demo"
}

# Makes names unique in the account (env, service accounts, keys).
resource "random_id" "id" {
  byte_length = 4
}

# ----------------------------------------
# Confluent Cloud cluster variables
# ----------------------------------------
variable "cc_cloud_provider" {
  type    = string
  default = "AWS"
}

variable "cc_cloud_region" {
  type    = string
  default = "eu-central-1"
}

variable "cc_env_name" {
  type    = string
  default = "env-dswt-london-2026"
}

variable "cc_cluster_name" {
  type    = string
  default = "cc-dswt-ecom"
}

variable "cc_availability" {
  type    = string
  default = "SINGLE_ZONE"
}

variable "stream_governance" {
  type    = string
  default = "ESSENTIALS"
}

variable "flink_cfu" {
  type    = number
  default = 10
}

# Raw topics the datagen produces into. Dimensions are compacted (latest per
# key); facts are delete-cleanup with 7-day retention. Silver/alerts topics are
# NOT here — the presenter creates them live from Claude's suggested Flink SQL.
variable "raw_topics" {
  type = map(object({
    partitions     = number
    cleanup_policy = string
    retention_ms   = string
  }))
  default = {
    dswt_customers       = { partitions = 1, cleanup_policy = "compact", retention_ms = "-1" }
    dswt_products        = { partitions = 1, cleanup_policy = "compact", retention_ms = "-1" }
    dswt_orders          = { partitions = 6, cleanup_policy = "delete", retention_ms = "604800000" }
    dswt_order_items     = { partitions = 6, cleanup_policy = "delete", retention_ms = "604800000" }
    dswt_shipments       = { partitions = 6, cleanup_policy = "delete", retention_ms = "604800000" }
    dswt_delivery_status = { partitions = 6, cleanup_policy = "delete", retention_ms = "604800000" }
    dswt_payments        = { partitions = 6, cleanup_policy = "delete", retention_ms = "604800000" }
  }
}

# Render kafka/cc-kafka.properties + kafka/cc-sr.properties from the outputs so
# the datagen can connect straight away.
variable "write_properties_files" {
  type    = bool
  default = true
}

# Render demo/DSWT-London-2026/.env directly from the outputs (docker compose +
# the DSWT_CC_MCP_AUTH token for Claude) so there is no manual copy step.
variable "write_env_file" {
  type    = bool
  default = true
}
