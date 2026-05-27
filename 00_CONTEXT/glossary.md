# Glossary

Terms used throughout this codebase. Add new entries when a term is used in code, docs, or commit messages without explanation and a new reader would not know what it means.

## Pipeline Terms

- **Audit trail**: A durable record of what happened, when it happened, and which inputs, prompts, models, and costs were involved.
- **Pipeline run ID**: The unique runtime identifier passed through a single pipeline execution. In batch DAGs, this is the Airflow `dag_run.run_id`. In streaming DAGs, the same value covers one micro-batch.
- **Quality gate**: A validation step that blocks bad data before expensive or risky processing. The batch DAG runs a quality gate before classification. The streaming DAG does not yet have one.
- **Dead letter**: A location for records that failed processing and need later review. In this project, dead letter records are written to S3 as NDJSON under the `dead-letter/` prefix, partitioned by stage and date.
- **Normalizer**: The component that maps raw input dicts into validated `FederalDocumentRecord` instances. Found in `src/archetype_core_etl/transform/normalizer.py`.
- **Classifier**: The component that calls Amazon Bedrock to score each record's compliance posture. Found in `src/archetype_core_etl/classify/bedrock_classifier.py`.

## Data Terms

- **Bronze table**: A raw or lightly processed table that preserves source-level detail. In Databricks, the project writes every classification result to a bronze table regardless of quality gate outcome.
- **Gold table**: A curated table intended for reporting, operations, or downstream use. In Databricks, the project writes only quality-gated results to gold.
- **Federal Document Record**: The canonical input record schema. Defined in `src/archetype_core_etl/extract/schema.py` as a Pydantic model with `extra="forbid"` and `frozen=True`.
- **Data contract**: A documented expectation for fields, types, allowed values, and ownership. See `01_SYSTEM/data-contracts.md`.

## AI Terms

- **Bedrock**: Amazon's managed runtime for foundation models. This project uses Bedrock's Anthropic Claude integration.
- **Claude Sonnet 4.6**: The specific model used for compliance classification. The Bedrock model ID is `us.anthropic.claude-sonnet-4-6`, accessed via a Cross-Region Inference profile.
- **Prompt hash**: A SHA-256 hash of the prompt text used for a model call. Recorded alongside every classification result so prompt drift is detectable in the audit log.
- **Input record hash**: A SHA-256 hash of the original input record at ingest time. Used to prove the record was not modified between ingest and classification.
- **Rate limiter**: Token-bucket implementation in `src/archetype_core_etl/classify/rate_limiter.py` that enforces requests-per-minute and tokens-per-minute limits on Bedrock calls.
- **Cost tracker**: Component that converts raw token counts into USD using per-1K pricing. Found in `src/archetype_core_etl/classify/cost_tracker.py`.

## Infrastructure Terms

- **Airflow**: The orchestration framework for both pipeline DAGs. This project targets Airflow 3.2 specifically because of breaking changes between 2.x and 3.x.
- **MWAA**: Amazon Managed Workflows for Apache Airflow. The production deployment target.
- **LocalStack**: A local AWS service emulator used in dev. The project uses LocalStack for S3 to keep local development fully sandboxed and cost-free.
- **Delta Lake**: Databricks' transactional storage layer on top of Parquet. Used here for bronze and gold tables.
- **OpenTofu**: A community fork of Terraform with the same configuration language. The project uses `tofu` rather than `terraform` for infrastructure work.

## Operating Terms

- **Dev container**: The Docker container defined in `docker/dev/Dockerfile` and the `dev` service in `docker-compose.yml`. All test, lint, typecheck, and pre-commit work runs inside this container.
- **dev-write service**: A second Docker service identical to `dev` except source mounts are read-write. Used exclusively by `make format` and no other target.
- **Pre-commit**: The validation harness that runs ruff, mypy, and custom checks on commit. Runs inside the dev container in validation-only mode (no silent auto-fixing).
- **`pyproject.toml`**: The single source of truth for project dependencies, tool configuration, and build metadata. There is intentionally no `requirements.txt`.
