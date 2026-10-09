#!/usr/bin/env bash
# Deploy (or update) the Db2 JDBC source connector on the self-managed Connect
# worker. envsubst fills the DB2_* / topic values from .env into the config JSON,
# then PUTs it to the worker REST API (idempotent).
set -euo pipefail
REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
source "${REPO_DIR}/.env"
: "${CONNECT_REST:=http://localhost:8083}"
: "${DB2_DATABASE:=TESTDB}" "${DB2_USER:=db2inst1}" "${DB2_PASSWORD:=olpipeDb2pw}"
: "${DB2_TABLE:=ORDERS}" "${DB2_TOPIC:=db2-orders}"
export DB2_DATABASE DB2_USER DB2_PASSWORD DB2_TABLE DB2_TOPIC

# wait for the worker to be up
echo "→ waiting for Connect worker at ${CONNECT_REST} ..."
for _ in $(seq 1 60); do
  curl -sf "${CONNECT_REST}/connectors" >/dev/null 2>&1 && break
  sleep 2
done
curl -sf "${CONNECT_REST}/connectors" >/dev/null 2>&1 || {
  echo "ERROR: Connect worker not reachable at ${CONNECT_REST} (is 'make up' done and the worker healthy?)" >&2
  exit 1
}

NAME="db2-orders-source"
CONFIG=$(envsubst < "${REPO_DIR}/connect/jdbc-db2-source.json" | python3 -c "import sys,json; print(json.dumps(json.load(sys.stdin)['config']))")

echo "→ deploying connector ${NAME} ..."
code=$(curl -s -o /tmp/olpipe_conn.json -w "%{http_code}" \
  -X PUT "${CONNECT_REST}/connectors/${NAME}/config" \
  -H "Content-Type: application/json" -d "$CONFIG")
if [[ "$code" == "200" || "$code" == "201" ]]; then
  echo "  ✓ deployed"
else
  echo "  ✗ HTTP ${code}"; cat /tmp/olpipe_conn.json; echo; exit 1
fi

sleep 3
echo "→ status:"
curl -s "${CONNECT_REST}/connectors/${NAME}/status" | python3 -c "import sys,json; d=json.load(sys.stdin); print('  connector:',d['connector']['state']); [print('  task',t['id'],t['state'], t.get('trace','')[:200]) for t in d.get('tasks',[])]" 2>/dev/null || cat /tmp/olpipe_conn.json
echo ""
echo "Verify rows:  confluent kafka topic consume ${DB2_TOPIC} --from-beginning --value-format avro ..."
echo "Next:         make flink"
