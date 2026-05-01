-- Run this in Databricks SQL Editor to add new columns to existing tables.
-- These ALTER statements are idempotent — they won't fail if columns already exist.

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
