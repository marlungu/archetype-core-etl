resource "aws_iam_role" "mwaa_execution" {
  name = "${var.project_name}-${var.environment}-mwaa-execution"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow"
        Principal = { Service = "airflow.amazonaws.com" }
        Action    = "sts:AssumeRole"
      },
      {
        Effect    = "Allow"
        Principal = { Service = "airflow-env.amazonaws.com" }
        Action    = "sts:AssumeRole"
      },
    ]
  })
}

resource "aws_iam_role_policy" "mwaa_execution" {
  name = "${var.project_name}-${var.environment}-mwaa-policy"
  role = aws_iam_role.mwaa_execution.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "S3Access"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:ListBucket",
          "s3:DeleteObject",
          "s3:GetBucketLocation",
        ]
        Resource = [
          var.raw_bucket_arn,
          "${var.raw_bucket_arn}/*",
          var.processed_bucket_arn,
          "${var.processed_bucket_arn}/*",
          var.audit_bucket_arn,
          "${var.audit_bucket_arn}/*",
          var.dags_bucket_arn,
          "${var.dags_bucket_arn}/*",
        ]
      },
      {
        Sid    = "CloudWatchLogs"
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents",
          "logs:GetLogEvents",
          "logs:DescribeLogGroups",
          "logs:DescribeLogStreams",
        ]
        Resource = "arn:aws:logs:${var.region}:${var.account_id}:log-group:airflow-${var.project_name}-${var.environment}*"
      },
      {
        Sid    = "SQSInternal"
        Effect = "Allow"
        Action = [
          "sqs:ChangeMessageVisibility",
          "sqs:DeleteMessage",
          "sqs:GetQueueAttributes",
          "sqs:GetQueueUrl",
          "sqs:ReceiveMessage",
          "sqs:SendMessage",
        ]
        Resource = "arn:aws:sqs:${var.region}:*:airflow-celery-*"
      },
      {
        Sid    = "BedrockInvoke"
        Effect = "Allow"
        Action = [
          "bedrock:InvokeModel",
          "bedrock:InvokeModelWithResponseStream",
        ]
        Resource = [
          "arn:aws:bedrock:${var.region}::foundation-model/anthropic.claude-*",
          "arn:aws:bedrock:${var.region}::foundation-model/us.anthropic.claude-*",
          "arn:aws:bedrock:us-east-1:${var.account_id}:inference-profile/us.anthropic.claude-*",
        ]
      },
      {
        Sid    = "KMSForSQS"
        Effect = "Allow"
        Action = [
          "kms:Decrypt",
          "kms:DescribeKey",
          "kms:GenerateDataKey*",
          "kms:Encrypt",
        ]
        Resource = "*"
        Condition = {
          StringLike = {
            "kms:ViaService" = "sqs.${var.region}.amazonaws.com"
          }
        }
      },
    ]
  })
}
