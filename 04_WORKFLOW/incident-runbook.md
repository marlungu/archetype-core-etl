# Incident Runbook

## First Response

1. Identify the affected pipeline, DAG run, and time range.
2. Preserve logs and audit records.
3. Check whether the issue is data, model, infrastructure, or code.
4. Stop repeat failures only if doing so avoids more damage or cost.
5. Record the decision and owner.

## Common Checks

- Airflow task logs
- Pipeline run ID
- Dead letter records
- Source bucket and key
- Audit table entries
- Model error responses
- Token usage spike
- Databricks write failures
- PostgreSQL write failures

## After Stabilizing

- Document root cause.
- Document records affected.
- Decide whether replay or backfill is needed.
- Add or update tests.
- Add or update monitoring if the issue was not visible soon enough.
