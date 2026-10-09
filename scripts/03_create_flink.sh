#!/usr/bin/env bash
# Create the Flink SQL statement that derives orders-enriched from db2-orders.
# Uses --database <CLUSTER_ID> so unqualified table names resolve to this
# cluster's topics, and --wait so a broken statement fails loudly.
set -euo pipefail
REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
source "${REPO_DIR}/.env"
: "${FLINK_COMPUTE_POOL:?set FLINK_COMPUTE_POOL in .env}"

STMT_NAME="olpipe-enrich-orders"
SQL=$(cat "${REPO_DIR}/flink/enrich-orders.sql")

# region for list/delete flags, from the compute pool
REGION=$(confluent flink compute-pool describe "$FLINK_COMPUTE_POOL" --environment "$ENV_ID" -o json 2>/dev/null \
         | python3 -c "import sys,json; print(json.load(sys.stdin).get('region','') )" 2>/dev/null || true)
: "${REGION:=${CLOUD_REGION:-us-east-2}}"
: "${CLOUD_PROVIDER:=aws}"

# drop any previous run of this statement (CTAS fails if orders-enriched exists)
confluent flink statement delete "$STMT_NAME" --environment "$ENV_ID" \
  --cloud "$CLOUD_PROVIDER" --region "$REGION" --force >/dev/null 2>&1 || true

echo "→ creating Flink statement ${STMT_NAME} (pool ${FLINK_COMPUTE_POOL}, db ${CLUSTER_ID}) ..."
confluent flink statement create "$STMT_NAME" \
  --sql "$SQL" \
  --compute-pool "$FLINK_COMPUTE_POOL" \
  --database "$CLUSTER_ID" \
  --environment "$ENV_ID" \
  --wait
echo "  ✓ statement running"
echo ""
echo "Verify:  confluent kafka topic consume ${ENRICHED_TOPIC:-orders-enriched} --from-beginning --value-format avro ..."
echo "Next:    make aws-setup && make tableflow"
