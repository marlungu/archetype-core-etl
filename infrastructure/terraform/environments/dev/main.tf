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
  region  = var.region
  profile = "archetype"

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

module "s3" {
  source       = "../../modules/s3"
  project_name = var.project_name
  environment  = var.environment
}

data "aws_caller_identity" "current" {}

module "iam" {
  source               = "../../modules/iam"
  project_name         = var.project_name
  environment          = var.environment
  region               = var.region
  account_id           = data.aws_caller_identity.current.account_id
  raw_bucket_arn       = module.s3.raw_bucket_arn
  processed_bucket_arn = module.s3.processed_bucket_arn
  audit_bucket_arn     = module.s3.audit_bucket_arn
  dags_bucket_arn      = module.s3.dags_bucket_arn
}

module "networking" {
  source       = "../../modules/networking"
  project_name = var.project_name
  environment  = var.environment
  region       = var.region
}

module "rds" {
  source                     = "../../modules/rds"
  project_name               = var.project_name
  environment                = var.environment
  vpc_id                     = module.networking.vpc_id
  private_subnet_ids         = module.networking.private_subnet_ids
  allowed_security_group_ids = [module.networking.mwaa_security_group_id]
}

module "mwaa" {
  source             = "../../modules/mwaa"
  project_name       = var.project_name
  environment        = var.environment
  region             = var.region
  dags_bucket_arn    = module.s3.dags_bucket_arn
  dags_bucket_name   = module.s3.dags_bucket_name
  execution_role_arn = module.iam.mwaa_execution_role_arn
  private_subnet_ids = module.networking.private_subnet_ids
  security_group_ids = [module.networking.mwaa_security_group_id]
}
