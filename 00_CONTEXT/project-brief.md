# Project Brief

## Purpose

`archetype-core-etl` processes federal document records through an auditable ETL pipeline.

The pipeline:
- Ingests records from S3 and Kinesis
- Normalizes records into a known schema
- Runs quality checks before classification
- Classifies records with an AI model
- Writes curated outputs to Databricks
- Writes audit evidence to PostgreSQL

## Primary Goals

- Keep data lineage clear.
- Keep classification results traceable.
- Keep costs visible.
- Keep infrastructure reproducible.
- Keep local development close to production behavior.

## Non-Goals

- This project is not a general document management system.
- This project is not a manual labeling tool.
- This project is not a place to store raw secrets or credentials.

## Current Assumptions

- Python is the main application runtime.
- Airflow owns orchestration.
- Terraform/OpenTofu owns cloud infrastructure.
- Prompt files are versioned artifacts.
- Audit records are part of the product, not an afterthought.
