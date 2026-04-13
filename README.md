# archetype-core-etl

Airflow-orchestrated, Databricks-powered ETL platform for processing federal document records.

![CI](https://github.com/marlungu/archetype-core-etl/actions/workflows/ci.yml/badge.svg?branch=main)
![python](https://img.shields.io/badge/python-3.11%2B-blue)

## Overview

archetype-core-etl ingests federal document records from S3 and Kinesis, normalizes and validates them through a Great Expectations quality gate, classifies each record for compliance risk using Amazon Bedrock (Claude Sonnet 4.6), and writes the results to Databricks Delta Lake tables with a full PostgreSQL audit trail. The system runs as two Airflow DAGs — a six-hourly batch pipeline and a five-minute streaming micro-batch pipeline — both deployed via Docker Compose locally and MWAA in production. All infrastructure is provisioned with Terraform.

## Stack

| Technology | Version | Purpose |
|---|---|---|
| Python | 3.11+ | Core runtime for extract, transform, classify, and load modules |
| Apache Airflow | 3.2 | DAG orchestration — batch and streaming pipeline scheduling |
| Amazon Bedrock (Claude Sonnet 4.6) | `us.anthropic.claude-sonnet-4-6` | Compliance classification with structured JSON output |
| Databricks Delta Lake | SDK ~0.102 | Bronze/Gold table storage via Statement Execution API |
| PostgreSQL | 16.3 | Audit trail persistence with `execute_values` batch inserts |
| Great Expectations | 1.16+ (fluent API) | Data quality validation — agency, priority, schema enforcement |
| Terraform | AWS ~5.0 / Databricks ~1.40 | Infrastructure provisioning — S3, IAM, RDS, MWAA, networking |
| Docker Compose | — | Local development stack — Airflow, Postgres, Redis, LocalStack |

## Repository Layout

```
archetype-core-etl/
├── src/archetype_core_etl/
│   ├── config/          # Pydantic BaseSettings, env-driven configuration
│   ├── common/          # Structured JSON logging, exception hierarchy
│   ├── extract/         # S3Reader, KinesisReader, FederalDocumentRecord schema
│   ├── transform/       # Record normalizer, Great Expectations quality gate
│   ├── classify/        # BedrockClassifier, token-bucket rate limiter, cost tracker
│   └── load/            # DeltaWriter (parameterized SQL), AuditWriter (psycopg2)
├── dags/
│   ├── common/          # Shared DAG default_args
│   └── pipelines/       # batch_pipeline_dag.py, streaming_pipeline_dag.py
├── infrastructure/
│   └── terraform/       # versions.tf, environments/dev/, modules/{s3,iam,networking,rds,mwaa}
├── scripts/             # generate_data.py (synthetic data), docker-entrypoint-init.sh
├── tests/
│   ├── unit/            # Schema, normalizer, quality gate, cost tracker tests
│   └── integration/     # External system tests (marked slow)
├── config/              # Environment-specific configuration
├── notebooks/           # Databricks notebooks
├── docs/                # Architecture and runbooks
├── docker-compose.yml   # Full Airflow stack with Celery executor
├── pyproject.toml       # Build config, dependencies, ruff/mypy/pytest settings
└── .pre-commit-config.yaml  # ruff, mypy, secret detection hooks
```

## Local Development

```bash
# 1. Configure environment
cp .env.example .env
# Fill in all ARCHETYPE_* values — no secrets have hardcoded defaults.

# 2. Start the stack
docker compose up -d

# 3. Verify Airflow health
curl -s http://localhost:8080/api/v2/monitor/health | python -m json.tool

# 4. Run tests
pip install -e ".[dev]"
pytest
```

The init container runs `airflow db migrate` and creates the admin user from `AIRFLOW_ADMIN_USERNAME` / `AIRFLOW_ADMIN_PASSWORD` in `.env`.

## Data Generator

Generate synthetic records matching the `FederalDocumentRecord` schema for local development and testing.

```bash
# Default: 1000 records to data/raw/YYYY/MM/DD/
python scripts/generate_data.py

# Reproducible run with custom count
python scripts/generate_data.py --records 5000 --seed 42

# Write directly to S3 (uses ARCHETYPE_AWS_* settings)
python scripts/generate_data.py --output s3 --output-path raw/federal
```

Records follow weighted distributions: 70/20/10% priority tiers (standard/expedite/emergency), 60/15/10/10/5% agencies (USCIS/CBP/ICE/TSA/FEMA), 80/15/5% flag counts (0/1/2).

## Security Design

- **SecretStr for all credentials** — database URLs, AWS keys, and execution role ARNs are `pydantic.SecretStr` fields, excluded from `repr` and logs by default.
- **No hardcoded secret defaults** — every secret field is required and raises `ValidationError` if missing from the environment.
- **Audit trail per record** — every classification result is written to the PostgreSQL audit table with timestamps, cost, and model metadata.
- **Bandit static analysis in CI** — ruff's `S` (flake8-bandit) rule set runs on every push and pull request.
- **SHA-pinned GitHub Actions** — all CI actions are pinned to full commit SHAs to prevent supply-chain attacks via tag mutation.
- **Parameterized SQL in Databricks** — Delta Lake writes use the Statement Execution API's native `parameters` field with `StatementParameterListItem`. No external values are interpolated into SQL strings.

## Architecture Decisions

### ADR-1: Airflow over Prefect

Airflow was chosen because the target production environment is AWS MWAA, which provides a managed Airflow runtime with IAM integration, S3 DAG storage, and CloudWatch logging out of the box. Prefect would require self-hosting the server and building equivalent AWS integrations manually.

### ADR-2: Great Expectations 1.x fluent API

The quality gate uses GX 1.x's fluent API (`gx.get_context(mode="ephemeral")`, `context.data_sources.add_pandas()`) instead of the legacy `PandasDataset` validator. The legacy API was deprecated in GX 0.18 and removed in 1.0. The fluent API provides a stable interface and avoids deprecation warnings in CI.

### ADR-3: FAB auth manager over SimpleAuthManager

Airflow 3.x's built-in `SimpleAuthManager` generates random passwords on startup and cannot read credentials from environment variables. The FAB (Flask-AppBuilder) auth manager — installed via `apache-airflow-providers-fab` — supports deterministic admin user creation from `.env` values, which is required for automated local development and CI.

### ADR-4: Parameterized Databricks statements over string interpolation

The Delta Lake writer originally constructed SQL `VALUES` clauses via Python string formatting with manual single-quote escaping. This was replaced with the Statement Execution API's native `parameters` field, which passes all external values as typed parameters (`:param_name` syntax). This eliminates SQL injection risk without relying on application-level escaping.

## License

Proprietary
