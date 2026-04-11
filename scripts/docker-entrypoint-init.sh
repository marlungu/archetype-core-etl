#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# archetype-core-etl — first-run initialization for the Airflow 3.x stack.
#
# Runs inside the `airflow-init` compose service as root. Responsibilities:
#   1. Wait for Postgres to accept connections.
#   2. Ensure the separate audit database exists (idempotent).
#   3. Run `airflow db migrate` to create/upgrade the metadata schema.
#   4. Create the default admin user if it does not already exist.
#
# Auth manager note: Airflow 3.2 ships with SimpleAuthManager as the default,
# which only supports static users with auto-generated passwords (configured
# via [core] simple_auth_manager_users). To get a deterministic admin
# password from .env, docker-compose.yml installs the FAB provider via
# _PIP_ADDITIONAL_REQUIREMENTS and switches AIRFLOW__CORE__AUTH_MANAGER to
# FabAuthManager. The `airflow users create` command used below is provided
# by that provider — it does NOT exist with the SimpleAuthManager default.
# Reference: https://airflow.apache.org/docs/apache-airflow-providers-fab/stable/cli-ref.html
#
# All credentials are read from environment variables — nothing is mounted
# from the host. The script is safe to re-run; each step is idempotent.
# -----------------------------------------------------------------------------
set -euo pipefail

log() {
    printf '[archetype-init] %s\n' "$*"
}

: "${POSTGRES_USER:?POSTGRES_USER must be set}"
: "${POSTGRES_PASSWORD:?POSTGRES_PASSWORD must be set}"
: "${AIRFLOW_DB_NAME:?AIRFLOW_DB_NAME must be set}"
: "${AUDIT_DB_NAME:?AUDIT_DB_NAME must be set}"
: "${_AIRFLOW_WWW_USER_USERNAME:?_AIRFLOW_WWW_USER_USERNAME must be set}"
: "${_AIRFLOW_WWW_USER_PASSWORD:?_AIRFLOW_WWW_USER_PASSWORD must be set}"
: "${_AIRFLOW_WWW_USER_EMAIL:?_AIRFLOW_WWW_USER_EMAIL must be set}"
: "${_AIRFLOW_WWW_USER_FIRSTNAME:?_AIRFLOW_WWW_USER_FIRSTNAME must be set}"
: "${_AIRFLOW_WWW_USER_LASTNAME:?_AIRFLOW_WWW_USER_LASTNAME must be set}"

log "waiting for postgres to accept connections..."
for attempt in $(seq 1 60); do
    if python - <<'PY' 2>/dev/null
import os, sys
import psycopg2
try:
    psycopg2.connect(
        host="postgres",
        port=5432,
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        dbname=os.environ["AIRFLOW_DB_NAME"],
        connect_timeout=3,
    ).close()
except Exception as exc:
    print(exc, file=sys.stderr)
    sys.exit(1)
PY
    then
        log "postgres is reachable (attempt ${attempt})"
        break
    fi
    sleep 2
done

log "ensuring audit database '${AUDIT_DB_NAME}' exists..."
python - <<'PY'
import os
import psycopg2
from psycopg2 import sql

conn = psycopg2.connect(
    host="postgres",
    port=5432,
    user=os.environ["POSTGRES_USER"],
    password=os.environ["POSTGRES_PASSWORD"],
    dbname=os.environ["AIRFLOW_DB_NAME"],
)
conn.autocommit = True
audit_db = os.environ["AUDIT_DB_NAME"]
with conn.cursor() as cur:
    cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (audit_db,))
    if cur.fetchone():
        print(f"[archetype-init] audit database already exists: {audit_db}")
    else:
        cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(audit_db)))
        print(f"[archetype-init] created audit database: {audit_db}")
conn.close()
PY

log "running airflow db migrate..."
airflow db migrate

log "ensuring admin user '${_AIRFLOW_WWW_USER_USERNAME}' exists..."
if airflow users list 2>/dev/null | awk 'NR>2 {print $3}' | grep -Fxq "${_AIRFLOW_WWW_USER_USERNAME}"; then
    log "admin user already exists — leaving it untouched"
else
    airflow users create \
        --username "${_AIRFLOW_WWW_USER_USERNAME}" \
        --password "${_AIRFLOW_WWW_USER_PASSWORD}" \
        --firstname "${_AIRFLOW_WWW_USER_FIRSTNAME}" \
        --lastname "${_AIRFLOW_WWW_USER_LASTNAME}" \
        --role Admin \
        --email "${_AIRFLOW_WWW_USER_EMAIL}"
    log "admin user created"
fi

log "initialization complete."
