output "mwaa_environment_name" {
  description = "MWAA environment name."
  value       = aws_mwaa_environment.main.name
}

output "mwaa_webserver_url" {
  description = "MWAA Airflow webserver URL."
  value       = aws_mwaa_environment.main.webserver_url
}

output "mwaa_arn" {
  description = "MWAA environment ARN."
  value       = aws_mwaa_environment.main.arn
}
