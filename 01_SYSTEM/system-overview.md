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

## Pipeline Differences

The batch DAG (`dags/pipelines/batch_pipeline_dag.py`) and the streaming DAG (`dags/pipelines/streaming_pipeline_dag.py`) do not share the same validation behavior.

- The batch DAG runs a quality gate (`run_quality_gate` task) before classification. Records missing required fields are rejected before any Bedrock call.
- The streaming DAG does not run a quality gate. Records from Kinesis flow directly from normalization into classification. Invalid records reach the model and either fail at parse time or produce unreliable output.

This is a known gap. The streaming pipeline should add a quality gate equivalent to the batch DAG before being used in production. Until then, treat any streaming output with the caution appropriate for unvalidated data.

## Local Development Model

All development work runs inside Docker containers. There is no project-level Python environment on the host machine.

- `make test`, `make lint`, `make typecheck`, `make precommit`, and `make shell` all run inside the `dev` container.
- `make format` runs inside the `dev-write` container, which is the only service with read-write source mounts.
- The `dev` container runs as a non-root user (`uid 1000`), with `cap_drop ALL`, `no-new-privileges`, and a 4 GB memory limit.
- Source code is mounted read-only into the `dev` container. The only path that modifies source from inside a container is `make format`.
- Tool versions (ruff, mypy, pytest, pre-commit) are pinned in `pyproject.toml` and installed during image build. The same versions run locally, in CI, and in any other developer's environment.

Do not install project dependencies on the host. Do not run pytest, ruff, mypy, or pre-commit on the host. Every validation command goes through `make`.

The only host requirements are Docker, Docker Compose, Make, and git.
