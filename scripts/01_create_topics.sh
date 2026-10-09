#!/usr/bin/env bash
# Create the raw topic the JDBC connector produces to. (orders-enriched is
# created by the Flink CTAS statement, so we don't create it here.)
set -euo pipefail
REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
source "${REPO_DIR}/.env"
: "${DB2_TOPIC:=db2-orders}"

echo "→ creating topic ${DB2_TOPIC} ..."
confluent kafka topic create "$DB2_TOPIC" \
  --cluster "$CLUSTER_ID" --environment "$ENV_ID" \
  --partitions 3 --if-not-exists
echo "  ✓ ${DB2_TOPIC} ready"
echo ""
echo "Next: make connector   (deploys the JDBC source to the Connect worker)"
