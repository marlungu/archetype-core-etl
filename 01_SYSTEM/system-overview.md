# System Overview

## Runtime Flow

1. Airflow starts a pipeline run.
2. Extractors read records from S3 or Kinesis.
3. The normalizer maps raw fields into the project schema.
4. The quality gate validates required fields and allowed values.
5. The classifier calls the model only after validation passes.
6. Loaders write results to Databricks Delta tables.
7. The audit writer records evidence in PostgreSQL.
8. Failed records are written to dead letter storage.

## Main Components

- `dags/`: Airflow DAG definitions.
- `src/archetype_core_etl/extract/`: Source readers and input schema.
- `src/archetype_core_etl/transform/`: Normalization and quality checks.
- `src/archetype_core_etl/classify/`: Model calls, prompts, rate limiting, and cost tracking.
- `src/archetype_core_etl/load/`: Databricks and PostgreSQL writes.
- `src/archetype_core_etl/common/`: Shared logging, AWS clients, errors, and dead letter handling.
- `infrastructure/terraform/`: Cloud resources.

## System Requirements

- Every pipeline run must be traceable.
- Every model result must be linked to an input record and prompt version.
- Invalid records must fail before model calls when possible.
- Writes must be idempotent where practical.
- Logs must support operational debugging without exposing secrets.
