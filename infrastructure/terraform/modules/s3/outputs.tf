output "raw_bucket_name" { value = aws_s3_bucket.raw.bucket }
output "raw_bucket_arn"  { value = aws_s3_bucket.raw.arn }

output "processed_bucket_name" { value = aws_s3_bucket.processed.bucket }
output "processed_bucket_arn"  { value = aws_s3_bucket.processed.arn }

output "audit_bucket_name" { value = aws_s3_bucket.audit.bucket }
output "audit_bucket_arn"  { value = aws_s3_bucket.audit.arn }

output "dags_bucket_name" { value = aws_s3_bucket.dags.bucket }
output "dags_bucket_arn"  { value = aws_s3_bucket.dags.arn }
