#!/usr/bin/env bash
# Integration test for the Greenplum connector (gp_connector.so).
#
# Two layers:
#   A. Structural (always run, no DB): the plugin builds, exports
#      dfo_connector_entry @ ABI v2, the gateway maps connector_type=greenplum →
#      gp_connector.so and loads it (probe → "list_entities failed" against a
#      dead host proves load+create ran; "connector load failed" would mean the
#      .so/mapping is broken).
#   B. End-to-end (opt-in): when GP_TEST_* env vars point at a real Greenplum,
#      create a pipeline, run it, and verify rows land in a DFO table.
#
# Env for layer B:
#   GP_TEST_HOST GP_TEST_USER GP_TEST_PASS GP_TEST_DBNAME GP_TEST_SCHEMA
#   GP_TEST_ENTITY (default: a small table in the schema)
#
# Run:  ./tests/integration/test_gp_connector.sh
#       GP_TEST_HOST=gp GP_TEST_USER=u GP_TEST_PASS=p GP_TEST_DBNAME=d \
#         GP_TEST_SCHEMA=public GP_TEST_ENTITY=mytable ./tests/integration/test_gp_connector.sh

set -u
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
GW="$ROOT/build/release/bin/napastak_gateway"
SO="$ROOT/build/release/lib/gp_connector.so"
PORT=19296
DATA="$(mktemp -d -t dfo_gp_XXXX)"
SECRET="gp-test-$$"
PASS=0; FAIL=0; SKIP=0

cleanup() { [[ -n "${GW_PID:-}" ]] && kill "$GW_PID" 2>/dev/null; rm -rf "$DATA"; }
trap cleanup EXIT
check() { if eval "$2"; then echo "PASS: $1"; PASS=$((PASS+1)); else echo "FAIL: $1   ($2)"; FAIL=$((FAIL+1)); fi; }
skip()  { echo "SKIP: $1 — $2"; SKIP=$((SKIP+1)); }

[[ -x "$GW" ]] || { echo "missing $GW — run: make release"; exit 1; }

# ── Layer A: structural ───────────────────────────────────────────────────
if [[ ! -f "$SO" ]]; then
  skip "gp_connector.so present" "not built (needs libpq) — run: make gp"
  echo ""; echo "Greenplum-connector test: $PASS passed, $FAIL failed, $SKIP skipped"
  [[ $FAIL -eq 0 ]]; exit $?
fi
check "gp_connector.so exports dfo_connector_entry" \
  "nm '$SO' 2>/dev/null | grep -q dfo_connector_entry"
check "connector name 'greenplum' embedded" \
  "strings '$SO' | grep -q greenplum"

cat > "$DATA/cfg.json" <<EOF
{"port":$PORT,"data_dir":"$DATA","auth_enabled":true,
 "jwt_secret":"$SECRET","admin_password":"admin","plugins_dir":"$ROOT/build/release/lib"}
EOF
"$GW" -c "$DATA/cfg.json" > "$DATA/gw.log" 2>&1 &
GW_PID=$!
for i in {1..30}; do curl -sf "http://localhost:$PORT/health" >/dev/null 2>&1 && break; sleep 0.2; done
TOKEN=$(curl -sf -X POST "http://localhost:$PORT/api/auth/token" \
  -H 'Content-Type: application/json' -d '{"username":"admin","password":"admin"}' \
  | python3 -c "import sys,json;print(json.load(sys.stdin)['token'])")
[[ -n "$TOKEN" ]] || { echo "FAIL: no admin token"; exit 1; }
AUTH=(-H "Authorization: Bearer $TOKEN")
echo "Gateway ready on :$PORT"

# Probe against a dead host: proves the gateway maps greenplum→gp_connector.so,
# loads it (ABI v2 accepted) and runs create(). A loaded-but-no-DB connector
# fails at list_entities; a missing/incompatible .so fails at load.
PROBE=$(curl -s -X POST "http://localhost:$PORT/api/connector/probe/entities" "${AUTH[@]}" \
  -H 'Content-Type: application/json' \
  -d '{"type":"greenplum","config":{"host":"127.0.0.1","port":"5999","dbname":"x","user":"u","password":"p","schema":"s"}}')
check "greenplum maps to gp_connector.so and loads (ABI v2)" \
  "echo '$PROBE' | grep -q 'list_entities failed'"
check "gateway logged 'loaded connector greenplum'" \
  "grep -q 'loaded connector greenplum' '$DATA/gw.log'"

# ── Layer B: end-to-end against a real Greenplum ───────────────────────────
if [[ -z "${GP_TEST_HOST:-}" ]]; then
  skip "e2e against real Greenplum" "set GP_TEST_HOST/USER/PASS/DBNAME/SCHEMA to enable"
  echo ""; echo "Greenplum-connector test: $PASS passed, $FAIL failed, $SKIP skipped"
  [[ $FAIL -eq 0 ]]; exit $?
fi

GP_TEST_PORT="${GP_TEST_PORT:-5432}"
GP_TEST_SCHEMA="${GP_TEST_SCHEMA:-public}"
GP_TEST_ENTITY="${GP_TEST_ENTITY:-}"
[[ -n "$GP_TEST_ENTITY" ]] || { skip "e2e" "set GP_TEST_ENTITY to a readable table"; \
  echo ""; echo "Greenplum-connector test: $PASS passed, $FAIL failed, $SKIP skipped"; [[ $FAIL -eq 0 ]]; exit $?; }

PIPELINE=$(cat <<JSON
{"name":"gp_e2e","enabled":true,
 "steps":[{
   "id":"s1","name":"extract",
   "connector_type":"greenplum",
   "connector_config":{
     "host":"$GP_TEST_HOST","port":"$GP_TEST_PORT","dbname":"$GP_TEST_DBNAME",
     "user":"$GP_TEST_USER","password":"$GP_TEST_PASS","schema":"$GP_TEST_SCHEMA"},
   "target_table":"gp_e2e_out",
   "transform_sql":"",
   "max_retries":0,
   "deps":[]
 }]}
JSON
)
# entity goes in transform_sql as a bare table name → connector reads it
PIPELINE="${PIPELINE/\"transform_sql\":\"\"/\"transform_sql\":\"$GP_TEST_ENTITY\"}"

RESP=$(curl -sf -X POST "http://localhost:$PORT/api/pipelines" "${AUTH[@]}" \
  -H 'Content-Type: application/json' -d "$PIPELINE")
PID=$(echo "$RESP" | python3 -c "import sys,json;print(json.load(sys.stdin).get('id',''))")
check "e2e pipeline created" "[[ -n '$PID' ]]"

curl -sf -X POST "http://localhost:$PORT/api/pipelines/$PID/run" "${AUTH[@]}" >/dev/null
for i in {1..30}; do
  CNT=$(curl -sf -X POST "http://localhost:$PORT/api/tables/query" "${AUTH[@]}" \
    -H 'Content-Type: application/json' \
    -d '{"sql":"SELECT COUNT(*) AS n FROM gp_e2e_out"}' 2>/dev/null \
    | python3 -c "import sys,json
try:
  d=json.load(sys.stdin); print(d['rows'][0][0] if d.get('rows') else 0)
except: print(0)" 2>/dev/null)
  [[ "${CNT:-0}" != "0" ]] && break
  sleep 1
done
check "e2e: gp_e2e_out populated (rows=${CNT:-0})" "[[ '${CNT:-0}' != '0' ]]"
check "e2e: table appears in /api/tables" \
  "curl -sf 'http://localhost:$PORT/api/tables' \"\${AUTH[@]}\" | grep -q gp_e2e_out"

echo ""
echo "Greenplum-connector test: $PASS passed, $FAIL failed, $SKIP skipped"
[[ $FAIL -eq 0 ]]
