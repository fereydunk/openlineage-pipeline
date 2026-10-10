#!/usr/bin/env bash
# Run a PATCHED Marquez web UI on :3000 that shows typed node labels
# (Kafka Topic / Flink / Kafka Connect / Tableflow / Iceberg / DB2 Table / Spark)
# instead of the stock generic "JOB" / "DATASET".
#
# This is the openlineage-pipeline equivalent of the sibling project's
# marquez-ui-patch fork: it clones marquez-web, applies marquez-ui/typed-labels.patch,
# and runs the webpack dev server on :3000 proxying /api -> the Marquez API (:5002).
#
# Requires: node + npm (brew install node), the Marquez stack up (make marquez-up).
# Usage:  bash scripts/marquez_ui_patched.sh
set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
PATCH="${REPO_DIR}/marquez-ui/typed-labels.patch"
UI_DIR="${MARQUEZ_UI_PATCH_DIR:-$HOME/marquez-ui-patch}"
WEB="${UI_DIR}/web"
API_PORT="${MARQUEZ_PORT:-5002}"
# Pin to the released tag that matches the stock marquez image (react 18). `main`
# is ahead on react 19 and crashes the UI (Sidenav "Element type is invalid").
MARQUEZ_TAG="${MARQUEZ_TAG:-0.51.1}"
export PATH="/opt/homebrew/bin:${PATH}"

command -v node >/dev/null || { echo "node not found — brew install node"; exit 1; }

if [ ! -d "${WEB}" ]; then
  echo "→ cloning marquez ${MARQUEZ_TAG} into ${UI_DIR} ..."
  git clone --depth 1 --branch "${MARQUEZ_TAG}" https://github.com/MarquezProject/marquez.git "${UI_DIR}"
fi

cd "${WEB}"
# apply the typed-label patch once (idempotent: skip if already applied)
if git apply --reverse --check "${PATCH}" >/dev/null 2>&1; then
  echo "✓ typed-label patch already applied"
else
  echo "→ applying typed-label patch ..."
  git apply "${PATCH}"
fi

[ -d node_modules ] || { echo "→ npm install (one-time, ~2000 pkgs) ..."; npm install --no-audit --no-fund; }

# free :3000 from the stock Marquez UI container if it holds it
docker stop olpipe-marquez-ui >/dev/null 2>&1 || true

echo "→ starting patched Marquez UI on http://localhost:3000  (api proxy -> :${API_PORT})"
echo "  leave this running; Ctrl-C to stop. Lineage graph:"
echo "  http://localhost:3000/lineage/job/flink%3A%2F%2Fenv-m2qxq/olpipe-enrich-orders"
exec env MARQUEZ_HOST=localhost MARQUEZ_PORT="${API_PORT}" \
  npx webpack serve --config webpack.dev.js --port 3000
