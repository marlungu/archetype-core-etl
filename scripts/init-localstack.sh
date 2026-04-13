#!/usr/bin/env bash
set -euo pipefail

GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

ok()   { printf "${GREEN}  ✓ %s${NC}\n" "$1"; }
fail() { printf "${RED}  ✗ %s${NC}\n" "$1"; exit 1; }

echo ""
echo "--- Initializing LocalStack ---"

# --- Check LocalStack is running ---
docker compose ps 2>/dev/null | grep localstack | grep -qi "up"

# --- Wait for healthy ---
printf "  Waiting for LocalStack"
for i in $(seq 1 30); do
    if docker compose exec -T localstack curl -fsS http://localhost:4566/_localstack/health >/dev/null 2>&1; then
        echo ""
        ok "LocalStack is healthy"
        break
    fi
    printf "."
    sleep 2
    if [ "$i" -eq 30 ]; then
        echo ""
        fail "LocalStack did not become healthy in time"
    fi
done

# --- Create S3 buckets ---
for bucket in archetype-raw archetype-processed archetype-audit; do
    docker compose exec -T localstack awslocal s3 mb "s3://${bucket}" 2>/dev/null || true
    ok "Bucket: ${bucket}"
done

# --- Install project if needed ---
python3 -c "import archetype_core_etl" 2>/dev/null || pip install -e "." --quiet
ok "Project package available"

# --- Generate synthetic data ---
python3 scripts/generate_data.py --records 500 --seed 42
DATA_FILE=$(find data/raw -name "*.ndjson" -type f | sort | tail -1)
if [ -z "$DATA_FILE" ]; then
    fail "No NDJSON file generated"
fi
RECORD_COUNT=$(wc -l < "$DATA_FILE" | tr -d ' ')
ok "Generated ${RECORD_COUNT} records: ${DATA_FILE}"

# --- Upload to S3 ---
cat "$DATA_FILE" | docker compose exec -T localstack awslocal s3 cp - s3://archetype-raw/federal-documents/seed-records.ndjson
ok "Uploaded to s3://archetype-raw/federal-documents/seed-records.ndjson"

# --- Verify ---
echo ""
echo "  S3 contents:"
docker compose exec -T localstack awslocal s3 ls s3://archetype-raw/federal-documents/ --recursive
echo ""
ok "LocalStack initialization complete"
echo ""
