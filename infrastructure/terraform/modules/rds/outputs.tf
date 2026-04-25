output "db_endpoint" {
  description = "RDS endpoint (host:port)."
  value       = aws_db_instance.main.endpoint
}

output "db_address" {
  description = "RDS hostname only."
  value       = aws_db_instance.main.address
}

output "db_name" {
  description = "Database name."
  value       = aws_db_instance.main.db_name
}

output "db_username" {
  description = "Master username."
  value       = aws_db_instance.main.username
}

output "db_secret_arn" {
  description = "Secrets Manager ARN for the master password."
  value       = aws_db_instance.main.master_user_secret[0].secret_arn
}
