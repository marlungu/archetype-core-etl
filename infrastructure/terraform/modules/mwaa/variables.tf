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

variable "dags_bucket_arn" {
  description = "ARN of the S3 bucket for DAGs."
  type        = string
}

variable "dags_bucket_name" {
  description = "Name of the S3 bucket for DAGs."
  type        = string
}

variable "execution_role_arn" {
  description = "ARN of the MWAA execution role."
  type        = string
}

variable "private_subnet_ids" {
  description = "List of private subnet IDs (exactly 2 required)."
  type        = list(string)
}

variable "security_group_ids" {
  description = "List of security group IDs for MWAA."
  type        = list(string)
}

variable "environment_class" {
  description = "MWAA environment class."
  type        = string
  default     = "mw1.small"
}

variable "airflow_version" {
  description = "Airflow version for MWAA."
  type        = string
  default     = "3.0.2"
}

variable "webserver_access_mode" {
  description = "MWAA webserver access: PUBLIC_ONLY for demo, PRIVATE_ONLY for production."
  type        = string
  default     = "PUBLIC_ONLY"
}
