# archetype-core-etl

<!-- badges: start -->
<!-- TODO: add CI, coverage, PyPI, license, and Python version badges -->
![status](https://img.shields.io/badge/status-scaffolding-lightgrey)
![python](https://img.shields.io/badge/python-3.11%2B-blue)
![license](https://img.shields.io/badge/license-TBD-lightgrey)
<!-- badges: end -->

Production-grade ETL platform: Airflow-orchestrated, Databricks-powered, Terraform-provisioned.

## Overview

> TODO: describe the pipeline, data sources, sinks, and downstream consumers.

## Repository layout

```
archetype-core-etl/
├── src/archetype_core_etl/   # core Python package (extract / transform / load)
├── dags/                     # Airflow DAG definitions
├── notebooks/                # Databricks notebooks
├── infrastructure/terraform/ # IaC for cloud + Databricks workspace
├── config/                   # environment-specific config
├── tests/                    # unit + integration tests
├── scripts/                  # operational / dev scripts
└── docs/                     # architecture + runbooks
```

## Getting started

> TODO: installation, local dev setup, running tests, deploying DAGs.

## Documentation

> TODO: link to architecture docs, runbooks, and data contracts.

## License

TBD
