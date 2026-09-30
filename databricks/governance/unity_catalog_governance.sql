-- Unity Catalog governance reference for archetype-core-etl.
-- Prerequisites: Unity Catalog workspace; account groups data_engineers and
-- compliance_reviewers; privileges to create catalogs, schemas, functions,
-- governed tags, and grants.

CREATE CATALOG IF NOT EXISTS archetype_core
COMMENT 'Governed reference catalog for Archetype Core ETL';

CREATE SCHEMA IF NOT EXISTS archetype_core.governed
COMMENT 'Governed classification data and policy functions';

USE CATALOG archetype_core;
USE SCHEMA governed;

GRANT USE CATALOG ON CATALOG archetype_core TO `data_engineers`;
GRANT USE CATALOG ON CATALOG archetype_core TO `compliance_reviewers`;
GRANT USE SCHEMA, SELECT, MODIFY, CREATE TABLE ON SCHEMA archetype_core.governed TO `data_engineers`;
GRANT USE SCHEMA, SELECT ON SCHEMA archetype_core.governed TO `compliance_reviewers`;

CREATE GOVERNED TAG IF NOT EXISTS data_sensitivity
  DESCRIPTION 'Sensitivity classification for governed reference data'
  VALUES ('internal', 'sensitive');

ALTER TABLE archetype_core.governed.classifications_gold
  ALTER COLUMN reasoning
  SET TAGS ('data_sensitivity' = 'sensitive');

CREATE OR REPLACE FUNCTION archetype_core.governed.mask_reasoning(value STRING)
RETURNS STRING
RETURN CASE
  WHEN is_account_group_member('compliance_reviewers') THEN value
  ELSE '[REDACTED]'
END;

ALTER TABLE archetype_core.governed.classifications_gold
  ALTER COLUMN reasoning
  SET MASK archetype_core.governed.mask_reasoning;

CREATE OR REPLACE FUNCTION archetype_core.governed.filter_risk_tier(risk_tier STRING)
RETURNS BOOLEAN
RETURN risk_tier <> 'emergency'
   OR is_account_group_member('compliance_reviewers');

ALTER TABLE archetype_core.governed.classifications_gold
  SET ROW FILTER archetype_core.governed.filter_risk_tier ON (risk_tier);

SHOW GRANTS ON SCHEMA archetype_core.governed;
SELECT * FROM archetype_core.information_schema.column_tags
WHERE schema_name = 'governed'
  AND table_name = 'classifications_gold';
