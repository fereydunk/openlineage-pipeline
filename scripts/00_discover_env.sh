#!/usr/bin/env bash
# Resolve Schema Registry endpoint and mint the API keys this pipeline needs:
#   KAFKA_API_KEY   (cluster-scoped) — Connect worker + console tools
#   SR_API_KEY      (SR-scoped)      — Avro converter
#   CLOUD_API_KEY   (org/cloud)      — Tableflow provider integration + enable
# Idempotent: only mints keys that are empty in .env.
#
# Prereq: `confluent login --save`. Env/cluster/SR/pool IDs come from .env.
set -euo pipefail
REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
ENV_FILE="${REPO_DIR}/.env"
[[ -f "$ENV_FILE" ]] || cp "${REPO_DIR}/.env.example" "$ENV_FILE"
source "$ENV_FILE"

upsert() {
  local k="$1" v="$2"
  if grep -qE "^${k}=" "$ENV_FILE"; then
    sed -i '' "s|^${k}=.*|${k}=${v}|" "$ENV_FILE"
  else
    echo "${k}=${v}" >> "$ENV_FILE"
  fi
}

[[ -n "${ENV_ID:-}"     ]] || { echo "ERROR: ENV_ID not set in .env" >&2; exit 1; }
[[ -n "${CLUSTER_ID:-}" ]] || { echo "ERROR: CLUSTER_ID not set in .env" >&2; exit 1; }

# ── Bootstrap + SR endpoint ──────────────────────────────────────────────────
if [[ -z "${BOOTSTRAP_SERVERS:-}" ]]; then
  desc=$(confluent kafka cluster describe "$CLUSTER_ID" --environment "$ENV_ID" -o json)
  BOOTSTRAP_SERVERS=$(echo "$desc" | python3 -c "import sys,json; print(json.load(sys.stdin).get('endpoint','').replace('SASL_SSL://',''))")
  upsert BOOTSTRAP_SERVERS "$BOOTSTRAP_SERVERS"
fi
echo "✓ cluster ${CLUSTER_ID} → ${BOOTSTRAP_SERVERS}"

if [[ -z "${SR_URL:-}" ]]; then
  sr_json=$(confluent schema-registry cluster describe --environment "$ENV_ID" -o json)
  SR_URL=$(echo "$sr_json" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('endpoint_url') or d.get('endpoint',''))")
  SR_ID=$(echo "$sr_json"  | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('cluster') or d.get('id',''))")
  upsert SR_URL "$SR_URL"; upsert SR_ID "$SR_ID"
fi
echo "✓ schema registry ${SR_ID} → ${SR_URL}"

mint() {
  # mint RESOURCE DESC KEYVAR SECRETVAR  — mints only if KEYVAR is empty
  local resource="$1" desc="$2" keyvar="$3" secvar="$4"
  if [[ -z "${!keyvar:-}" ]]; then
    echo "→ minting ${keyvar} (resource ${resource}) ..."
    local out; out=$(confluent api-key create --resource "$resource" --environment "$ENV_ID" --description "$desc" -o json)
    local k s
    k=$(echo "$out" | python3 -c "import sys,json; print(json.load(sys.stdin)['api_key'])")
    s=$(echo "$out" | python3 -c "import sys,json; print(json.load(sys.stdin)['api_secret'])")
    upsert "$keyvar" "$k"; upsert "$secvar" "$s"
    echo "  ✓ ${k}"
  else
    echo "✓ ${keyvar} already set"
  fi
}

mint "$CLUSTER_ID" "olpipe-kafka" KAFKA_API_KEY KAFKA_API_SECRET
mint "$SR_ID"      "olpipe-sr"    SR_API_KEY    SR_API_SECRET
mint "cloud"       "olpipe-cloud" CLOUD_API_KEY CLOUD_API_SECRET

echo ""
echo "Discovery complete. Next: start Docker, then 'make up' and 'make connector'."
