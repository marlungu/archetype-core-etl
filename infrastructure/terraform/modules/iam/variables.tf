variable "project_name" {
  description = "Project identifier used for resource naming."
  type        = string
}

variable "environment" {
  description = "Deployment environment (dev, staging, prod)."
  type        = string
}

variable "region" {
  description = "AWS region."
  type        = string
}

variable "account_id" {
  description = "AWS account ID."
  type        = string
}

variable "raw_bucket_arn" {
  description = "ARN of the raw S3 bucket."
  type        = string
}

variable "processed_bucket_arn" {
  description = "ARN of the processed S3 bucket."
  type        = string
}

variable "audit_bucket_arn" {
  description = "ARN of the audit S3 bucket."
  type        = string
}

variable "dags_bucket_arn" {
  description = "ARN of the DAGs S3 bucket."
  type        = string
}
