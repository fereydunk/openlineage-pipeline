#!/usr/bin/env bash
# Preflight the openlineage-pipeline demo and print the run sequence.
# This pipeline has heavy external deps (Docker, Confluent Cloud, AWS), so rather
# than one button it guides you through the ordered steps.
set -euo pipefail
REPO_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$REPO_DIR"
[[ -f .env ]] || cp .env.example .env

ok(){ echo "  ✓ $1"; }
no(){ echo "  ✗ $1"; }

echo "openlineage-pipeline — preflight"
command -v confluent >/dev/null && confluent kafka cluster list >/dev/null 2>&1 && ok "confluent CLI logged in" || no "confluent not logged in → confluent login --save"
command -v docker >/dev/null && docker ps >/dev/null 2>&1 && ok "docker daemon running" || no "docker daemon not running → start Docker/OrbStack"
command -v python3 >/dev/null && ok "python3 present" || no "python3 missing"
[[ -d .venv ]] && ok ".venv present" || no ".venv missing → make venv"
( command -v aws >/dev/null && aws sts get-caller-identity >/dev/null 2>&1 ) && ok "AWS credentials valid" || no "AWS creds not set (needed for Tableflow) → export AWS_ACCESS_KEY_ID etc."

cat <<'STEPS'

Run the pipeline in order:

  make venv          # one-time: Python deps
  make discover      # resolve SR + mint CC API keys into .env  (confluent login first)
  make up            # start DB2 + Connect in Docker  (DB2 ~3-5 min first boot)
  make seed          # create ORDERS table + rows in DB2
  make topics        # create db2-orders on Confluent Cloud
  make connector     # deploy JDBC source  (DB2 -> db2-orders)
  make flink         # Flink statement     (db2-orders -> orders-enriched)
  make aws-setup     # S3 + Glue + IAM   (needs AWS creds); paste AWS_* into .env
  make tableflow     # enable Tableflow (Iceberg) on orders-enriched
  make produce-more  # add rows to trigger the first Tableflow S3 flush
  make read          # Spark reads the Iceberg table + aggregates  (the end consumer)

See README.md for the full walkthrough and TROUBLESHOOTING.md if a step stalls.
STEPS
