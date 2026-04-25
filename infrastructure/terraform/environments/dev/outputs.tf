output "raw_bucket"       { value = module.s3.raw_bucket_name }
output "processed_bucket" { value = module.s3.processed_bucket_name }
output "audit_bucket"     { value = module.s3.audit_bucket_name }
output "dags_bucket"      { value = module.s3.dags_bucket_name }

output "mwaa_execution_role_arn" {
  value = module.iam.mwaa_execution_role_arn
}

output "vpc_id" { value = module.networking.vpc_id }

output "db_endpoint"   { value = module.rds.db_endpoint }
output "db_secret_arn" { value = module.rds.db_secret_arn }

output "mwaa_webserver_url" {
  description = "MWAA Airflow webserver URL."
  value       = module.mwaa.mwaa_webserver_url
}
