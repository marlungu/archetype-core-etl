# Shared Terraform and provider version pins for archetype-core-etl.
#
# This file is the single source of truth for provider versions. Each
# environment under ./environments/<env>/ re-declares the same terraform
# block so `terraform init` picks it up; when pins change here, update the
# environments in lockstep.

terraform {
  required_version = ">= 1.7"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.40"
    }
  }
}
