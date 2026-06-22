#!/usr/bin/env bash
# Integration test for the Oracle connector (oracle_connector.so).
#
# Oracle requires ODPI-C at build time and Oracle Instant Client at runtime, so
# the .so is often absent in CI. The test degrades gracefully:
#   A. Source-level (always): the connector source exists, uses the ABI macro
#      (not a hard-coded version), wires read-only fields to NULL, and the
#      gateway recognises connector_type=oracle (resolves oracle_connector.so).
#   B. Load-level (when built): the .so exports dfo_connector_entry @ ABI v2 and
#      the gateway loads it (probe against a dead host → "list_entities failed").
#   C. End-to-end (opt-in): when ORACLE_TEST_* point at a real Oracle, run a
#      pipeline and verify rows land in a DFO table.
#
# Env for layer C:
#   ORACLE_TEST_HOST ORACLE_TEST_USER ORACLE_TEST_PASS ORACLE_TEST_SERVICE
#   ORACLE_TEST_SCHEMA ORACLE_TEST_ENTITY
#
# Run:  ./tests/integration/test_oracle_connector.sh
#       ORACLE_TEST_HOST=... ./tests/integration/test_oracle_connector.sh

set -u
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
GW="$ROOT/build/release/bin/dfo_gateway"
SO="$ROOT/build/release/lib/oracle_connector.so"
SRC="$ROOT/lib/connector/plugins/oracle/oracle_connector.c"
PORT=19297
DATA="$(mktemp -d -t dfo_ora_XXXX)"
SECRET="ora-test-$$"
PASS=0; FAIL=0; SKIP=0

cleanup() { [[ -n "${GW_PID:-}" ]] && kill "$GW_PID" 2>/dev/null; rm -rf "$DATA"; }
trap cleanup EXIT
check() { if eval "$2"; then echo "PASS: $1"; PASS=$((PASS+1)); else echo "FAIL: $1   ($2)"; FAIL=$((FAIL+1)); fi; }
skip()  { echo "SKIP: $1 — $2"; SKIP=$((SKIP+1)); }

[[ -x "$GW" ]] || { echo "missing $GW — run: make release"; exit 1; }

# ── Layer A: source-level (no toolchain needed) ───────────────────────────
check "oracle_connector.c exists" "[[ -f '$SRC' ]]"
check "uses ABI version macro (loadable across ABI bumps)" \
  "grep -q 'DFO_CONNECTOR_ABI_VERSION' '$SRC'"
check "read-only: write_batch wired to NULL" \
  "grep -qE '\.write_batch *= *NULL' '$SRC'"
check "cdc_start wired to NULL (LogMiner is a later phase)" \
  "grep -qE '\.cdc_start *= *NULL' '$SRC'"
check "11g pagination via ROWNUM subquery present" \
  "grep -q 'dfo__rn' '$SRC'"
check "12c+ pagination via OFFSET/FETCH present" \
  "grep -q 'OFFSET' '$SRC' && grep -q 'FETCH NEXT' '$SRC'"
check "NLS formats set on connect" \
  "grep -q 'NLS_TIMESTAMP_FORMAT' '$SRC'"
check "version autodetect via dpiConn_getServerVersion" \
  "grep -q 'dpiConn_getServerVersion' '$SRC'"

# ── Spin up the gateway for load/probe checks ─────────────────────────────
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
echo "Gateway ready on :$PORT (oracle .so present: $([[ -f "$SO" ]] && echo yes || echo no))"

PROBE=$(curl -s -X POST "http://localhost:$PORT/api/connector/probe/entities" "${AUTH[@]}" \
  -H 'Content-Type: application/json' \
  -d '{"type":"oracle","config":{"host":"127.0.0.1","port":"1521","service_name":"X","user":"u","password":"p","schema":"S"}}')

if [[ ! -f "$SO" ]]; then
  # .so not built (no ODPI-C). The gateway must still recognise the type and
  # try the correct path — proven by the dlopen attempt in the log.
  check "gateway resolves connector_type=oracle → oracle_connector.so path" \
    "grep -q 'oracle_connector.so' '$DATA/gw.log'"
  skip "oracle .so load + e2e" "oracle_connector.so not built — install ODPI-C, run: make oracle"
  echo ""; echo "Oracle-connector test: $PASS passed, $FAIL failed, $SKIP skipped"
  [[ $FAIL -eq 0 ]]; exit $?
fi

# ── Layer B: .so built ────────────────────────────────────────────────────
check "oracle_connector.so exports dfo_connector_entry" \
  "nm '$SO' 2>/dev/null | grep -q dfo_connector_entry"
check "oracle maps and loads (ABI v2; dead host → list_entities failed)" \
  "echo '$PROBE' | grep -q 'list_entities failed'"

# ── Layer C: end-to-end against a real Oracle ─────────────────────────────
if [[ -z "${ORACLE_TEST_HOST:-}" ]]; then
  skip "e2e against real Oracle" "set ORACLE_TEST_HOST/USER/PASS/SERVICE/SCHEMA/ENTITY to enable"
  echo ""; echo "Oracle-connector test: $PASS passed, $FAIL failed, $SKIP skipped"
  [[ $FAIL -eq 0 ]]; exit $?
fi

ORACLE_TEST_PORT="${ORACLE_TEST_PORT:-1521}"
ORACLE_TEST_SERVICE="${ORACLE_TEST_SERVICE:-ORCL}"
ORACLE_TEST_ENTITY="${ORACLE_TEST_ENTITY:-}"
[[ -n "$ORACLE_TEST_ENTITY" ]] || { skip "e2e" "set ORACLE_TEST_ENTITY to a readable table"; \
  echo ""; echo "Oracle-connector test: $PASS passed, $FAIL failed, $SKIP skipped"; [[ $FAIL -eq 0 ]]; exit $?; }

PIPELINE=$(cat <<JSON
{"name":"oracle_e2e","enabled":true,
 "steps":[{
   "id":"s1","name":"extract",
   "connector_type":"oracle",
   "connector_config":{
     "host":"$ORACLE_TEST_HOST","port":"$ORACLE_TEST_PORT",
     "service_name":"$ORACLE_TEST_SERVICE","user":"$ORACLE_TEST_USER",
     "password":"$ORACLE_TEST_PASS","schema":"${ORACLE_TEST_SCHEMA:-}"},
   "target_table":"oracle_e2e_out",
   "transform_sql":"__ENT__",
   "max_retries":0,
   "deps":[]
 }]}
JSON
)
PIPELINE="${PIPELINE/__ENT__/$ORACLE_TEST_ENTITY}"

RESP=$(curl -sf -X POST "http://localhost:$PORT/api/pipelines" "${AUTH[@]}" \
  -H 'Content-Type: application/json' -d "$PIPELINE")
PID=$(echo "$RESP" | python3 -c "import sys,json;print(json.load(sys.stdin).get('id',''))")
check "e2e pipeline created" "[[ -n '$PID' ]]"

curl -sf -X POST "http://localhost:$PORT/api/pipelines/$PID/run" "${AUTH[@]}" >/dev/null
for i in {1..30}; do
  CNT=$(curl -sf -X POST "http://localhost:$PORT/api/tables/query" "${AUTH[@]}" \
    -H 'Content-Type: application/json' \
    -d '{"sql":"SELECT COUNT(*) AS n FROM oracle_e2e_out"}' 2>/dev/null \
    | python3 -c "import sys,json
try:
  d=json.load(sys.stdin); print(d['rows'][0][0] if d.get('rows') else 0)
except: print(0)" 2>/dev/null)
  [[ "${CNT:-0}" != "0" ]] && break
  sleep 1
done
check "e2e: oracle_e2e_out populated (rows=${CNT:-0})" "[[ '${CNT:-0}' != '0' ]]"

echo ""
echo "Oracle-connector test: $PASS passed, $FAIL failed, $SKIP skipped"
[[ $FAIL -eq 0 ]]
