variable "project_name" {
  description = "Project identifier used for resource naming."
  type        = string
}

variable "environment" {
  description = "Deployment environment (dev, staging, prod)."
  type        = string
}

variable "vpc_id" {
  description = "VPC ID for the database."
  type        = string
}

variable "private_subnet_ids" {
  description = "List of private subnet IDs for the DB subnet group."
  type        = list(string)
}

variable "allowed_security_group_ids" {
  description = "Security group IDs allowed to access the database."
  type        = list(string)
}

variable "db_name" {
  description = "Name of the database to create."
  type        = string
  default     = "archetype_audit"
}

variable "db_username" {
  description = "Master username for the database."
  type        = string
  default     = "archetype"
}

variable "instance_class" {
  description = "RDS instance class."
  type        = string
  default     = "db.t3.micro"
}
