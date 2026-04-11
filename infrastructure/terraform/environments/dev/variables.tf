variable "environment" {
  description = "Deployment environment identifier (dev, staging, prod)."
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "environment must be one of: dev, staging, prod."
  }
}

variable "region" {
  description = "AWS region for all regional resources in this environment."
  type        = string
}

variable "project_name" {
  description = "Project identifier used for resource naming and default tags."
  type        = string
}

variable "databricks_host" {
  description = "Databricks workspace URL (e.g. https://dbc-xxxxxxxx-xxxx.cloud.databricks.com)."
  type        = string
}
