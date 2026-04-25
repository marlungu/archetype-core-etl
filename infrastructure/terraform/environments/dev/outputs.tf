output "raw_bucket"       { value = module.s3.raw_bucket_name }
output "processed_bucket" { value = module.s3.processed_bucket_name }
output "audit_bucket"     { value = module.s3.audit_bucket_name }
output "dags_bucket"      { value = module.s3.dags_bucket_name }

output "mwaa_execution_role_arn" {
  value = module.iam.mwaa_execution_role_arn
}
