-- ---------------------------------------------------------------------------
-- archetype-core-etl — Postgres first-boot initialization.
--
-- This file is bind-mounted into the postgres container at
-- /docker-entrypoint-initdb.d/init-db.sql so Postgres runs it once
-- on the very first startup (when the data directory is empty).
--
-- It creates the audit database used by the ETL pipeline.  The main
-- Airflow metadata database is created automatically by the POSTGRES_DB
-- environment variable on the postgres service.
-- ---------------------------------------------------------------------------

SELECT 'CREATE DATABASE archetype_audit'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'archetype_audit')\gexec
