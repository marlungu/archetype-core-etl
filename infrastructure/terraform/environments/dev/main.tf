# Dev environment root module for archetype-core-etl.
#
# Provider configuration lives here; the S3 remote state backend is declared
# in backend.tf. Wire in feature modules (s3, iam, networking, rds, mwaa)
# from ../../modules as the platform is built out.

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

provider "aws" {
  region = var.region

  default_tags {
    tags = {
      Project     = var.project_name
      Environment = var.environment
      ManagedBy   = "terraform"
    }
  }
}

provider "databricks" {
  host = var.databricks_host
}
