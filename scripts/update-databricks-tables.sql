-- Run this in Databricks SQL Editor to add new columns to existing tables.
-- Note: Databricks ALTER TABLE ADD COLUMNS will fail if columns already exist.
-- Run these only once, or check the table schema first with DESCRIBE TABLE.

USE CATALOG archetype_etl;
USE SCHEMA default;

-- Add pipeline_run_id and token breakdown to bronze table
ALTER TABLE classifications_bronze ADD COLUMNS (
  pipeline_run_id STRING,
  input_tokens    INT,
  output_tokens   INT
);

-- Add pipeline_run_id and token breakdown to gold table
ALTER TABLE classifications_gold ADD COLUMNS (
  pipeline_run_id STRING,
  input_tokens    INT,
  output_tokens   INT
);
