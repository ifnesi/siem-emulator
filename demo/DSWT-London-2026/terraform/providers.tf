terraform {
  required_providers {
    confluent = {
      source  = "confluentinc/confluent"
      version = "2.30.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
    local = {
      source  = "hashicorp/local"
      version = "~> 2.4"
    }
  }
}

provider "confluent" {
  # Auth via environment variables (export them before `terraform apply`):
  #   export CONFLUENT_CLOUD_API_KEY="XXXXX"
  #   export CONFLUENT_CLOUD_API_SECRET="XXXXX"
}
