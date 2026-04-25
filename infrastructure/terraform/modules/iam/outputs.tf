output "mwaa_execution_role_arn" {
  value = aws_iam_role.mwaa_execution.arn
}

output "mwaa_execution_role_name" {
  value = aws_iam_role.mwaa_execution.name
}
