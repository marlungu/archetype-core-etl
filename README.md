# archetype-core-etl

Airflow-orchestrated, Databricks-powered ETL platform for processing federal document records.

![CI](https://github.com/marlungu/archetype-core-etl/actions/workflows/ci.yml/badge.svg?branch=main)
![python](https://img.shields.io/badge/python-3.12%2B-blue)

## Overview

archetype-core-etl ingests federal document records from S3 and Kinesis, normalizes and validates them through a Great Expectations quality gate, classifies each record for compliance risk using Amazon Bedrock (Claude Sonnet 4.6), and writes the results to Databricks Delta Lake tables with a full PostgreSQL audit trail. The system runs as two Airflow DAGs — a six-hourly batch pipeline and a five-minute streaming micro-batch pipeline — both deployed via Docker Compose locally and MWAA in production. All infrastructure is provisioned with Terraform.

## Stack

| Technology | Version | Purpose |
|---|---|---|
| Python | 3.12+ | Core runtime for extract, transform, classify, and load modules |
| Apache Airflow | 3.2 | DAG orchestration — batch and streaming pipeline scheduling |
| Amazon MWAA | 3.0.2 | Managed Airflow in production — `mw1.small` workers, `PUBLIC_ONLY` endpoint |
| Amazon Bedrock (Claude Sonnet 4.6) | `us.anthropic.claude-sonnet-4-6` | Compliance classification with structured JSON output |
| Databricks Delta Lake | SDK ~0.105 | Bronze/Gold table storage via Statement Execution API |
| PostgreSQL | 16.13 | Audit trail persistence with `execute_values` batch inserts |
| Great Expectations | 1.17+ (fluent API) | Data quality validation — agency, priority, schema enforcement |
| Terraform | AWS ~5.0 / Databricks ~1.40 | Infrastructure provisioning — S3, IAM, RDS, MWAA, networking |
| Docker Compose | — | Local development stack — Airflow, Postgres, Redis, LocalStack |

> **Version note:** Docker Compose runs Airflow 3.2.0 locally. AWS MWAA supports Airflow 3.0.2 in production. DAGs are compatible with both versions.

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
│   ├── common/          # Shared DAG default_args, XCom serialization helpers
│   └── pipelines/       # batch_pipeline_dag.py, streaming_pipeline_dag.py
├── infrastructure/
│   └── terraform/       # versions.tf, environments/dev/, modules/{s3,iam,networking,rds,mwaa}
├── scripts/             # generate_data.py, init-db.sql, init-localstack.sh, setup-local.sh,
│                        #   docker-entrypoint-init.sh, update-databricks-tables.sql
├── tests/
│   ├── unit/            # Schema, normalizer, quality gate, cost tracker, dead letter, prompt versioning
│   └── fixtures/        # Shared test fixtures
├── config/              # Environment-specific configuration (dev/, staging/, prod/)
├── data/                # Placeholder directories for raw/, interim/, processed/, external/ data
├── docs/                # Architecture diagrams (architecture/), ADRs (adr/), runbooks (runbooks/)
├── notebooks/           # Databricks notebooks (exploratory/, production/)
├── .github/             # GitHub Actions CI workflow (workflows/ci.yml)
├── docker-compose.yml   # Full Airflow stack with Celery executor
├── pyproject.toml       # Build config, dependencies, ruff/mypy/pytest settings
└── .pre-commit-config.yaml  # ruff, mypy, secret detection hooks
```

## Local Development

### Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (or Docker Engine + Compose plugin)
- Python 3.12+
- Make

### Quickstart

```bash
make demo
```

This single command will:
1. Generate a `.env` file with auto-generated secrets
2. Start Postgres, Redis, LocalStack, and the full Airflow stack (webserver, scheduler, triggerer, Celery worker)
3. Run DB migrations and create the Airflow admin user
4. Create S3 buckets in LocalStack and seed 500 synthetic test records
5. Print the Airflow UI URL and login credentials

### Step-by-step setup

```bash
# 1. Generate .env with auto-generated secrets
make setup

# 2. Start all services and seed data
make up

# 3. Open Airflow UI
open http://localhost:8080    # admin / admin

# 4. Generate more test data
make generate

# 5. Run tests
make test

# 6. View service logs
make logs

# 7. Tear down (preserves data volumes)
make down

# 8. Full cleanup (removes volumes and .env)
make clean
```

### Available make targets

Run `make help` to see all targets:

| Target | Description |
|---|---|
| `make help` | Show all available targets |
| `make setup` | Generate .env from template with auto-generated secrets |
| `make up` | Start all services, wait for healthy, seed LocalStack |
| `make down` | Stop all services |
| `make restart` | Stop and restart all services |
| `make demo` | Full setup + start (runs setup then up) |
| `make test` | Install dev dependencies and run pytest |
| `make lint` | Run ruff linter |
| `make typecheck` | Run mypy type checker |
| `make generate` | Generate 1000 synthetic test records |
| `make logs` | Tail service logs |
| `make ps` | Show running services |
| `make clean` | Remove all containers, volumes, and .env |

### DAG processor (Airflow 3.x)

Airflow 3.x requires a separate DAG processor service for the UI to detect DAGs. The base `docker-compose.yml` does not include it — create a `docker-compose.override.yml` with the DAG processor service and bind mounts for `dags/` and `src/`:

```bash
cp docker-compose.override.yml.example docker-compose.override.yml
```

Then edit it to add the DAG processor and PYTHONPATH. See the project wiki for the full override template.

### Exposing services on the host

By default, only the Airflow UI (port 8080) is exposed. To access Postgres, Redis, or LocalStack directly from your machine, uncomment the relevant port sections in `docker-compose.override.yml`.

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

### ADR-5: Audit trail hashing for evidence integrity

Every audit entry includes an `input_record_hash` (SHA-256 of the input record) and a `prompt_hash` (SHA-256 of the system prompt). This provides cryptographic proof that:
- The input record was not modified after classification
- The exact prompt version that generated the result can be identified
- Changes to the prompt are detectable across pipeline runs

This is a common pattern in AI governance systems where regulatory compliance requires evidence preservation.

## License

Proprietary
