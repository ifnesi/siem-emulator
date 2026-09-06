terraform {
  required_providers {
    confluent = {
      source  = "confluentinc/confluent"
      version = ">= 2.75.0"
    }
  }
}

provider "confluent" {
  # Same auth as the infra module — export before running:
  #   export CONFLUENT_CLOUD_API_KEY="XXXXX"
  #   export CONFLUENT_CLOUD_API_SECRET="XXXXX"
  # (Flink statements also carry their own Flink API key via `credentials`.)
}
