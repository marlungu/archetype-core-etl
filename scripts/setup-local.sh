#!/usr/bin/env bash
set -euo pipefail

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

ok()   { printf "${GREEN}  ✓ %s${NC}\n" "$1"; }
fail() { printf "${RED}  ✗ %s${NC}\n" "$1"; exit 1; }
warn() { printf "${YELLOW}  ⚠ %s${NC}\n" "$1"; }

echo ""
echo "========================================="
echo "  archetype-core-etl — local setup"
echo "========================================="
echo ""

# --- Prerequisites ---
command -v docker >/dev/null 2>&1 || fail "docker is not installed"
ok "docker found"
docker compose version >/dev/null 2>&1 || fail "docker compose plugin not found"
ok "docker compose found"
command -v python3 >/dev/null 2>&1 || fail "python3 is not installed"
ok "python3 found"

# --- Check for existing .env ---
if [ -f .env ]; then
    printf "${YELLOW}  A .env file already exists. Overwrite? [y/N] ${NC}"
    read -r answer
    if [ "$answer" != "y" ] && [ "$answer" != "Y" ]; then
        echo "  Aborted."
        exit 0
    fi
fi

# --- Generate secrets ---
PG_PASS=$(python3 -c "import secrets; print(secrets.token_urlsafe(16))")
REDIS_PASS=$(python3 -c "import secrets; print(secrets.token_urlsafe(16))")
FERNET_KEY=$(python3 -c "import base64,os; print(base64.urlsafe_b64encode(os.urandom(32)).decode())")
WEBSERVER_SECRET=$(python3 -c "import secrets; print(secrets.token_urlsafe(48))")
AUDIT_URL="postgresql+psycopg2://archetype:${PG_PASS}@postgres:5432/archetype_audit"

# --- Build .env from .env.example ---
cp .env.example .env

# macOS-compatible sed (uses -i '' instead of -i)
if [[ "$OSTYPE" == "darwin"* ]]; then
    SED_INPLACE=(sed -i '')
else
    SED_INPLACE=(sed -i)
fi

"${SED_INPLACE[@]}" "s|POSTGRES_PASSWORD=CHANGE_ME|POSTGRES_PASSWORD=${PG_PASS}|" .env
"${SED_INPLACE[@]}" "s|REDIS_PASSWORD=CHANGE_ME|REDIS_PASSWORD=${REDIS_PASS}|" .env
"${SED_INPLACE[@]}" "s|AIRFLOW_FERNET_KEY=CHANGE_ME|AIRFLOW_FERNET_KEY=${FERNET_KEY}|" .env
"${SED_INPLACE[@]}" "s|AIRFLOW_WEBSERVER_SECRET_KEY=CHANGE_ME|AIRFLOW_WEBSERVER_SECRET_KEY=${WEBSERVER_SECRET}|" .env
"${SED_INPLACE[@]}" "s|AIRFLOW_ADMIN_PASSWORD=CHANGE_ME|AIRFLOW_ADMIN_PASSWORD=admin|" .env
"${SED_INPLACE[@]}" "s|ARCHETYPE_DB_AUDIT_URL=CHANGE_ME|ARCHETYPE_DB_AUDIT_URL=${AUDIT_URL}|" .env

echo ""
ok ".env file created"
ok "Postgres password: ${PG_PASS:0:4}..."
ok "Airflow UI credentials: admin / admin"
echo ""
echo "  Next step: make up"
echo ""
