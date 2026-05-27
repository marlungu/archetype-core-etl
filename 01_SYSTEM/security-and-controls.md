# Security and Controls

## Secrets

- Do not commit `.env`, `.env.*`, `*.tfvars`, private keys, or credentials.
- Use environment variables, local ignored files, or managed secret stores.
- Keep secret values out of logs, exceptions, tests, and screenshots.

## Data Protection

- Use encrypted storage for cloud data stores.
- Block public access to storage buckets unless there is an approved exception.
- Keep least privilege IAM policies for Airflow, S3, Bedrock, RDS, and Databricks.

## AI Controls

- Version prompt files.
- Hash prompt content.
- Record model identifiers.
- Record token counts and cost.
- Keep invalid data from reaching model calls when quality checks can catch it.

## Review Triggers

Request review before changing:
- Prompt behavior
- Audit schema
- IAM permissions
- Terraform destroy behavior
- SQL write patterns
- Data retention behavior
