#!/usr/bin/env bash
# Integration test for the YAML pipeline preview endpoint.
#
# Covers:
#   1. POST /api/pipelines/preview-yaml round-trips a valid YAML doc to JSON
#   2. Invalid YAML returns 400 with line/col error info
#
# Note: pipelines_dir auto-load (GitOps YAML pipelines) was removed —
# pipelines now live only in the catalog (created via UI or
# /api/pipelines/from-template). This test only covers YAML→JSON preview,
# which is unrelated and still used by the UI's YAML import/paste feature.

set -u
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
GW="$ROOT/build/release/bin/napastak_gateway"
PORT=19280
DATA="$(mktemp -d -t dfo_yaml_XXXX)"
SECRET="yaml-test-$$"
PASS=0; FAIL=0

cleanup() { [[ -n "${GW_PID:-}" ]] && kill "$GW_PID" 2>/dev/null; rm -rf "$DATA"; }
trap cleanup EXIT
check() { if eval "$2"; then echo "PASS: $1"; PASS=$((PASS+1)); else echo "FAIL: $1   ($2)"; FAIL=$((FAIL+1)); fi; }

[[ -x "$GW" ]] || { echo "missing $GW"; exit 1; }

cat > "$DATA/cfg.json" <<EOF
{"port":$PORT,"data_dir":"$DATA","auth_enabled":true,
 "jwt_secret":"$SECRET","admin_password":"admin"}
EOF
"$GW" -c "$DATA/cfg.json" > "$DATA/gw.log" 2>&1 &
GW_PID=$!
for i in {1..30}; do
  curl -sf "http://localhost:$PORT/health" >/dev/null 2>&1 && break
  sleep 0.2
done
TOKEN=$(curl -sf -X POST "http://localhost:$PORT/api/auth/token" \
  -H 'Content-Type: application/json' \
  -d '{"username":"admin","password":"admin"}' \
  | python3 -c "import sys,json;print(json.load(sys.stdin)['token'])")
[[ -n "$TOKEN" ]] || { echo "FAIL: no admin token"; exit 1; }

# 1. preview-yaml with valid input
RESP=$(curl -sf -X POST "http://localhost:$PORT/api/pipelines/preview-yaml" \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: text/yaml' \
  --data-binary @- <<'EOF'
name: my_test
enabled: true
triggers:
  - type: cron
    cron_expr: "@hourly"
steps:
  - id: x
    transform_sql: SELECT 1
    target_table: t
EOF
)
check "preview-yaml: valid YAML returns 200 JSON with name" \
  "echo '$RESP' | grep -q '\"name\":\"my_test\"'"
check "preview-yaml: triggers[] preserved" \
  "echo '$RESP' | grep -q '\"type\":\"cron\"'"

# 2. preview-yaml with malformed YAML
STATUS=$(curl -s -o "$DATA/err.json" -w "%{http_code}" -X POST \
  "http://localhost:$PORT/api/pipelines/preview-yaml" \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: text/yaml' \
  --data-binary $'just bare text\nno colon')
check "preview-yaml: bare text returns 400" "[[ '$STATUS' == '400' ]]"
check "preview-yaml: error response includes 'line'" \
  "grep -q '\"line\"' '$DATA/err.json'"

echo ""
echo "YAML pipelines test: $PASS passed, $FAIL failed"
[[ $FAIL -eq 0 ]]
