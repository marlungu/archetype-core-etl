# archetype-core-etl

Production-grade AI governance ETL platform. Python + Airflow + AWS Bedrock + Databricks Delta. The pipeline classifies documents using Claude via Bedrock, writes to Databricks Delta tables in a medallion pattern (bronze → gold), and runs identically in LocalStack (dev) and AWS (prod) via environment variables.

This is not a tutorial project. The audience is senior data engineers in regulated industries. Governance, traceability, and reliability are first-class concerns, not afterthoughts.

## Stack

- Python >=3.12 (`pyproject.toml` floor; CI runs 3.13). mypy checks against 3.12.
- Airflow 3.2 locally. The `infrastructure/terraform/modules/mwaa/` module targets MWAA 3.0.2 (mw1.small). MWAA also supports 3.2 as of May 2026; the module can be bumped when desired. DAGs run on both.
- AWS Bedrock with Claude Sonnet 4.6 for classification
- Databricks Delta tables (Unity Catalog, Pro SQL warehouses only)
- Great Expectations 1.17+ (fluent API) powers the data quality gate
- Kinesis for streaming intake, S3 for batch intake
- LocalStack + Docker Compose for local dev (S3, Kinesis, DynamoDB, Postgres, Redis)
- Terraform for production infrastructure
- pytest for tests; ruff + mypy gate CI; `.pre-commit-config.yaml` runs hooks on commit
- Development runs inside a devcontainer (`.devcontainer/`) so the AI agent chain executes with an in-container Python venv and a default-deny network firewall

## Repo layout

```
src/archetype_core_etl/
├── classify/        bedrock_classifier, rate_limiter, cost_tracker, versioned prompts
├── common/          aws helpers, exceptions, logging, dead_letter
├── config/          settings (Pydantic)
├── extract/         kinesis_reader, s3_reader, schema
├── load/            audit_writer, delta_writer
└── transform/       field_presence, normalizer, quality_gate

dags/
├── common/          dag_defaults, serialization helpers
└── pipelines/       batch_pipeline_dag, streaming_pipeline_dag

tests/
├── conftest.py
├── fixtures/
├── unit/            unit tests for each module
├── integration/     external-system tests (marked slow)
└── acceptance/      spec-level acceptance tests (written by the feature-factory chain)

infrastructure/terraform/
├── environments/dev/
└── modules/{iam, mwaa, networking, rds, s3}/

scripts/              setup-local.sh, init-localstack.sh, init-db.sql,
                      update-databricks-tables.sql, generate_data.py,
                      docker-entrypoint-init.sh
```

## Commands

- `make check` — test + lint + typecheck (the full gate). Auto-detects context: on the host it routes through `docker compose --profile tools`; inside the devcontainer it runs the tools directly. There is no docker daemon inside the devcontainer, so never call `docker compose` directly there.
- `make up` — start LocalStack, Postgres, Redis, Airflow (only if a task needs live services).

Full command reference (setup, teardown, data generation, per-tool targets) is in the README.

## Environment and infrastructure values

All account-specific and infrastructure values live in gitignored files, never in this document:

- `.env` (gitignored) — AWS region/profile, Postgres/Redis credentials, Airflow secrets, Databricks workspace URL and warehouse ID
- `infrastructure/terraform/environments/dev/terraform.tfvars` (gitignored) — AWS account ID, IAM details, Terraform state backend
- `.env.example` — the committed template showing every required variable name with placeholder values

When a value is needed, read it from the environment at runtime. Do not hardcode account IDs, workspace URLs, warehouse IDs, IAM principal names, or bucket names into source or documentation.

- Bedrock model: Claude Sonnet 4.6
- Databricks catalog: `archetype_core` (auto-created at workspace provisioning — NOT `archetype_etl`)
- Databricks schema: `default`
- Databricks tables: `classifications_bronze`, `classifications_gold` (Delta format)

## Architecture rules

- LocalStack and AWS deployments share identical Python code. The only difference is the `ENVIRONMENT=local|aws` env var and the boto3 endpoint URL. Anything that diverges between dev and prod is a bug.
- Bedrock calls go through `src/archetype_core_etl/classify/bedrock_classifier.py`. DAGs and tasks never call boto3 directly.
- All classification outputs land in `classifications_bronze` before any transform runs. Bronze is immutable.
- DAGs are thin. Business logic lives in `src/archetype_core_etl/` modules.
- Prompts are versioned in `src/archetype_core_etl/classify/prompts/`. Never inline a prompt string in code.
- Tests use moto or LocalStack — never real AWS in CI.
- Quality gate logic lives in `src/archetype_core_etl/transform/quality_gate.py`. Confidence thresholds, validation rules, and classification banding all go there. `confidence_band(confidence)` returns a `BandDecision(band, reason)` dataclass.
- The quality gate is built on Great Expectations 1.x using the fluent API: an ephemeral in-memory `DataContext` (`gx.get_context(mode="ephemeral")`), a pandas data source, and a programmatically assembled `ExpectationSuite`. Do not use the legacy `PandasDataset` validator — it was removed in GX 1.0. The `GateResult` dataclass is the stable contract; downstream code must depend on it, not on the GX API surface.

## Don't do

- Don't create or edit `.env*` files. Claude Code denies these by design. If a rule belongs in `.env.example`, embed the full file content in the prompt and use `sed -i ''` (macOS form) for in-place edits.
- Don't hardcode account IDs, Databricks workspace URLs, warehouse IDs, IAM principal names, or S3 bucket names into source or docs. Read them from the environment.
- Don't call `docker compose` directly from inside the devcontainer — there is no docker daemon there. Use the `make` targets, which detect context and run tools directly in-container.
- Don't use Databricks Classic SQL warehouses on this AWS account. They consistently fail with `DEADLINE_EXCEEDED`. Pro warehouses only.
- Don't run `CREATE CATALOG` via SQL — fails with `INVALID_STATE`. Use the catalog auto-created at workspace provisioning.
- Don't log raw Bedrock responses to the bronze table — structured fields only. Raw responses are a governance risk and may contain prompt-injected content.
- Don't bypass the human review gate when classification confidence is below 0.85. At or above 0.85 → auto_approve; 0.60 to 0.85 → human_review; below 0.60 → reject. See `confidence_band` in `src/archetype_core_etl/transform/quality_gate.py`.
- Don't commit Databricks PATs, AWS keys, or any pattern matching `dapi[0-9a-f]{32}`. The pre-commit hook at `.claude/hooks/pre-commit-secrets.sh` blocks these, but assume it will fail occasionally — `.pre-commit-config.yaml` is the second line of defense.
- Don't inline Bedrock prompts in classifier code. Use `src/archetype_core_etl/classify/prompts/` files.
- Don't use the legacy Great Expectations API (`PandasDataset`, `ge.from_pandas`). The quality gate uses the GX 1.x fluent API only. Keep the `GateResult` contract stable so callers never touch the GX surface directly.

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