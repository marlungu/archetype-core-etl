# archetype-core-etl

Production-grade AI governance ETL platform. Python + Airflow + AWS Bedrock + Databricks Delta. The pipeline classifies documents using Claude via Bedrock, writes to Databricks Delta tables in a medallion pattern (bronze → gold), and runs identically in LocalStack (dev) and AWS (prod) via environment variables.

This is not a tutorial project. The audience is senior data engineers in regulated industries. Governance, traceability, and reliability are first-class concerns, not afterthoughts.

## Stack

- Python 3.13, packaged via `pyproject.toml`
- Airflow 3.2 (LOCAL ONLY — MWAA caps at Airflow 2.10.4; the `infrastructure/terraform/modules/mwaa/` module is for the eventual 2.x deployment path)
- AWS Bedrock with Claude Sonnet 4.6 for classification
- Databricks Delta tables (Unity Catalog, Pro SQL warehouses only)
- Kinesis for streaming intake, S3 for batch intake
- LocalStack + Docker Compose for local dev (S3, Kinesis, DynamoDB, Postgres, Redis)
- Terraform for production infrastructure
- pytest for tests; ruff + mypy gate CI; `.pre-commit-config.yaml` runs hooks on commit

## Repo layout

```
src/archetype_core_etl/
├── classify/        Bedrock client, rate limiter, cost tracker, versioned prompts
├── common/          aws helpers, exceptions, logging, dead_letter
├── config/          settings (Pydantic)
├── extract/         kinesis_reader, s3_reader, schema
├── load/            audit_writer, delta_writer
└── transform/       normalizer, quality_gate

dags/
├── common/          dag_defaults, serialization helpers
└── pipelines/       batch_pipeline_dag, streaming_pipeline_dag

tests/
├── conftest.py
├── fixtures/
└── unit/

infrastructure/terraform/
├── environments/dev/
└── modules/{iam, mwaa, networking, rds, s3}/

scripts/              setup-local.sh, init-localstack.sh, init-db.sql, update-databricks-tables.sql
```

## Commands

- `make dev` — bring up LocalStack, Postgres, Redis, Airflow
- `make test` — run pytest + ruff + mypy
- `./scripts/setup-local.sh` — first-time local environment setup
- `./scripts/init-localstack.sh` — reset LocalStack to a clean state

## AWS context

- Profile: `archetype`
- Account: 594474086522
- IAM user: `archetype-dev`
- Region: `us-east-1`
- Bedrock model: Claude Sonnet 4.6
- Terraform state: S3 bucket `archetype-core-etl-tfstate`, lock table `archetype-core-etl-tflocks`

## Databricks context

- Workspace: `dbc-34205be5-3b63.cloud.databricks.com`
- Warehouse ID: `04ec71b7c548b8bf` (Pro 2XS, named `archetype-etl-warehouse`)
- Catalog: `archetype_core` (auto-created at workspace provisioning — NOT `archetype_etl`)
- Schema: `default`
- Tables: `classifications_bronze`, `classifications_gold` (Delta format)

## Architecture rules

- LocalStack and AWS deployments share identical Python code. The only difference is the `ENVIRONMENT=local|aws` env var and the boto3 endpoint URL. Anything that diverges between dev and prod is a bug.
- Bedrock calls go through `src/archetype_core_etl/classify/bedrock_classifier.py`. DAGs and tasks never call boto3 directly.
- All classification outputs land in `classifications_bronze` before any transform runs. Bronze is immutable.
- DAGs are thin. Business logic lives in `src/archetype_core_etl/` modules.
- Prompts are versioned in `src/archetype_core_etl/classify/prompts/`. Never inline a prompt string in code.
- Tests use moto or LocalStack — never real AWS in CI.
- Quality gate logic lives in `src/archetype_core_etl/transform/quality_gate.py`. Confidence thresholds, validation rules, classification banding all go there.

## Don't do

- Don't create or edit `.env*` files. Claude Code denies these by design. If a rule belongs in `.env.example`, embed the full file content in the prompt and use `sed -i ''` (macOS form) for in-place edits.
- Don't use Databricks Classic SQL warehouses on this AWS account. They consistently fail with `DEADLINE_EXCEEDED`. Pro warehouses only.
- Don't run `CREATE CATALOG` via SQL — fails with `INVALID_STATE`. Use the catalog auto-created at workspace provisioning.
- Don't add Airflow 3.x features to anything that lands in `infrastructure/terraform/modules/mwaa/`. MWAA does not support 3.x.
- Don't log raw Bedrock responses to the bronze table — structured fields only. Raw responses are a governance risk and may contain prompt-injected content.
- Don't bypass the human review gate when classification confidence is below 0.85.
- Don't commit Databricks PATs, AWS keys, or any pattern matching `dapi[0-9a-f]{32}`. The pre-commit hook at `.claude/hooks/pre-commit-secrets.sh` blocks these, but assume it will fail occasionally — `.pre-commit-config.yaml` is the second line of defense.
- Don't inline Bedrock prompts in classifier code. Use `src/archetype_core_etl/classify/prompts/` files.

## Where deeper docs live

- `docs/architecture/` — system diagrams, data flow, governance hooks
- `docs/runbooks/` — operational runbooks
- `docs/adr/` — architecture decision records
- `infrastructure/terraform/environments/dev/` — dev deployment config

## Style

- Plain Python. No clever metaprogramming.
- Type hints required (mypy strict).
- Functions fit on one screen. If they don't, refactor before adding more.
- Docstrings in real sentences, not bullet cascades.

## Voice

Documentation, comments, and commit messages are written in a calm, direct register. Short and medium sentences. No corporate filler. Banned words: utilize, synergy, leverage, cutting-edge, game-changing. The goal is documentation a senior engineer would respect, not LinkedIn copy.
