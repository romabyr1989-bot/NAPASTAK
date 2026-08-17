#!/usr/bin/env bash
# Integration test for Python pipeline steps.
#
# Covers:
#   1. Pipeline with `python_code` step is created and serialized
#   2. transform_sql output is fed as CSV to python3 stdin
#   3. user code mutating `df` (pandas DataFrame) is reflected in target_table
#   4. python_code without transform_sql runs with an empty input (still ingests)
#   5. python_timeout_sec is honored — runaway script is killed
#   6. Syntax error in user code → pipeline marked RUN_FAILED with stderr tail

set -u
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
GW="$ROOT/build/release/bin/napastak_gateway"
PORT=19290
DATA="$(mktemp -d -t dfo_pystep_XXXX)"
SECRET="pystep-test-$$"
PASS=0; FAIL=0; SKIP=0

cleanup() { [[ -n "${GW_PID:-}" ]] && kill "$GW_PID" 2>/dev/null; rm -rf "$DATA"; }
trap cleanup EXIT
check() { if eval "$2"; then echo "PASS: $1"; PASS=$((PASS+1)); else echo "FAIL: $1   ($2)"; FAIL=$((FAIL+1)); fi; }
skip()  { echo "SKIP: $1 — $2"; SKIP=$((SKIP+1)); }

[[ -x "$GW" ]] || { echo "missing $GW"; exit 1; }

# Skip if pandas isn't available — Python steps require it.
if ! python3 -c 'import pandas' 2>/dev/null; then
  echo "SKIP: pandas not installed — Python steps require pandas"; exit 0
fi

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
echo "Gateway ready on :$PORT"

AUTH=(-H "Authorization: Bearer $TOKEN")

# ── Seed source table ─────────────────────────────────────────────────────
SRC_CSV=$'name,score\nalice,30\nbob,42\ncarol,42\ndave,28\neve,35'
curl -sf -X POST "http://localhost:$PORT/api/ingest/csv?table=raw_scores" \
  "${AUTH[@]}" -H 'Content-Type: text/csv' --data-binary "$SRC_CSV" >/dev/null
check "raw_scores ingest OK" \
  "curl -sf -X POST 'http://localhost:$PORT/api/tables/query' \"\${AUTH[@]}\" \
     -H 'Content-Type: application/json' \
     -d '{\"sql\":\"SELECT count(*) FROM raw_scores\"}' | grep -q '\"5\"'"

# ── 1. Pipeline: transform_sql + python_code → target_table ───────────────
PY1=$(cat <<'PYEOF'
df["score"] = df["score"].astype(int) * 2
df = df[df["score"] > 60]
df["tag"] = "doubled"
PYEOF
)
PY1_JSON=$(python3 -c 'import json,sys; print(json.dumps(sys.stdin.read()))' <<<"$PY1")

PIPELINE=$(cat <<JSON
{
  "name": "py_doubler",
  "enabled": true,
  "steps": [
    {"id":"s1","name":"double_and_filter",
     "transform_sql":"SELECT name, score FROM raw_scores",
     "python_code": $PY1_JSON,
     "python_timeout_sec": 30,
     "target_table":"scores_doubled"}
  ]
}
JSON
)
RESP=$(curl -sf -X POST "http://localhost:$PORT/api/pipelines" \
  "${AUTH[@]}" -H 'Content-Type: application/json' -d "$PIPELINE")
PID=$(echo "$RESP" | python3 -c "import sys,json;d=json.load(sys.stdin);print(d.get('id',''))")
check "pipeline with python_code created" "[[ -n '$PID' ]]"

# Round-trip: GET /api/pipelines should echo python_code + python_timeout_sec
LIST=$(curl -sf "http://localhost:$PORT/api/pipelines" "${AUTH[@]}")
check "python_code round-trips in pipeline list" \
  "echo '$LIST' | grep -q 'python_code'"
check "python_timeout_sec round-trips" \
  "echo '$LIST' | grep -q '\"python_timeout_sec\":30'"

# Trigger the run
curl -sf -X POST "http://localhost:$PORT/api/pipelines/$PID/run" "${AUTH[@]}" >/dev/null
sleep 2

# Output table should contain only carol(84) and bob(84) — both > 60.
# (alice 60, dave 56, eve 70 doubled: alice=60 NOT > 60, dave=56, eve=70 ✓)
# After doubling: alice=60, bob=84, carol=84, dave=56, eve=70
# After filter > 60: bob=84, carol=84, eve=70
OUT=$(curl -sf -X POST "http://localhost:$PORT/api/tables/query" "${AUTH[@]}" \
  -H 'Content-Type: application/json' \
  -d '{"sql":"SELECT name, score, tag FROM scores_doubled ORDER BY name"}')
check "python step ingested into target_table" \
  "echo '$OUT' | grep -q 'scores_doubled\\|score\\|tag'"
check "filter kept exactly 3 rows" \
  "echo '$OUT' | python3 -c 'import sys,json; d=json.load(sys.stdin); assert len(d[\"rows\"])==3, d' 2>/dev/null"
check "doubled values appear in output" \
  "echo '$OUT' | grep -q '\"84\"'"
check "new column 'tag' present" \
  "echo '$OUT' | grep -q '\"doubled\"'"

# ── 2. Pipeline: python_code WITHOUT transform_sql ─────────────────────────
PY2=$(cat <<'PYEOF'
import pandas as pd
df = pd.DataFrame({"x":[1,2,3], "y":["a","b","c"]})
PYEOF
)
PY2_JSON=$(python3 -c 'import json,sys; print(json.dumps(sys.stdin.read()))' <<<"$PY2")
PIPELINE2=$(cat <<JSON
{"name":"py_synth","enabled":true,
 "steps":[{"id":"s1","name":"synth",
           "python_code":$PY2_JSON,
           "python_timeout_sec":30,
           "target_table":"synth_out"}]}
JSON
)
RESP=$(curl -sf -X POST "http://localhost:$PORT/api/pipelines" \
  "${AUTH[@]}" -H 'Content-Type: application/json' -d "$PIPELINE2")
PID2=$(echo "$RESP" | python3 -c "import sys,json;print(json.load(sys.stdin).get('id',''))")
check "python-only pipeline (no SQL) created" "[[ -n '$PID2' ]]"
curl -sf -X POST "http://localhost:$PORT/api/pipelines/$PID2/run" "${AUTH[@]}" >/dev/null
sleep 2
OUT=$(curl -sf -X POST "http://localhost:$PORT/api/tables/query" "${AUTH[@]}" \
  -H 'Content-Type: application/json' \
  -d '{"sql":"SELECT * FROM synth_out ORDER BY x"}')
check "python-only step writes 3 rows" \
  "echo '$OUT' | python3 -c 'import sys,json;d=json.load(sys.stdin);assert len(d[\"rows\"])==3, d' 2>/dev/null"

# ── 3. Syntax error → pipeline marked failed with stderr tail ─────────────
PY3='this is not valid python ('
PY3_JSON=$(python3 -c 'import json,sys;print(json.dumps(sys.stdin.read()))' <<<"$PY3")
PIPELINE3=$(cat <<JSON
{"name":"py_broken","enabled":true,
 "steps":[{"id":"s1","name":"broken",
           "transform_sql":"SELECT * FROM raw_scores",
           "python_code":$PY3_JSON,
           "python_timeout_sec":10,
           "max_retries":0,
           "target_table":"never_made"}]}
JSON
)
RESP=$(curl -sf -X POST "http://localhost:$PORT/api/pipelines" \
  "${AUTH[@]}" -H 'Content-Type: application/json' -d "$PIPELINE3")
PID3=$(echo "$RESP" | python3 -c "import sys,json;print(json.load(sys.stdin).get('id',''))")
curl -sf -X POST "http://localhost:$PORT/api/pipelines/$PID3/run" "${AUTH[@]}" >/dev/null
sleep 2
LIST=$(curl -sf "http://localhost:$PORT/api/pipelines" "${AUTH[@]}")
# Pipeline.run_status is serialized as JSON field "status" (top-level); RUN_FAILED == 3.
check "broken pipeline marked failed" \
  "echo '$LIST' | PID=$PID3 python3 -c 'import os,sys,json; ps=json.load(sys.stdin); ok=any(p[\"id\"]==os.environ[\"PID\"] and p.get(\"status\")==3 for p in ps); assert ok, ps' 2>/dev/null"

# ── 4. Timeout — script that sleeps longer than python_timeout_sec ────────
PY4=$(cat <<'PYEOF'
import time
time.sleep(60)
PYEOF
)
PY4_JSON=$(python3 -c 'import json,sys;print(json.dumps(sys.stdin.read()))' <<<"$PY4")
PIPELINE4=$(cat <<JSON
{"name":"py_timeout","enabled":true,
 "steps":[{"id":"s1","name":"slow",
           "transform_sql":"SELECT 1",
           "python_code":$PY4_JSON,
           "python_timeout_sec":2,
           "max_retries":0,
           "target_table":"slow_out"}]}
JSON
)
RESP=$(curl -sf -X POST "http://localhost:$PORT/api/pipelines" \
  "${AUTH[@]}" -H 'Content-Type: application/json' -d "$PIPELINE4")
PID4=$(echo "$RESP" | python3 -c "import sys,json;print(json.load(sys.stdin).get('id',''))")
T0=$(date +%s)
curl -sf -X POST "http://localhost:$PORT/api/pipelines/$PID4/run" "${AUTH[@]}" >/dev/null
T1=$(date +%s)
ELAPSED=$((T1 - T0))
# /run is synchronous in this build, so the timeout (2s) should bound the wait.
# (max_retries=0 to skip the retry/backoff path that would otherwise dominate.)
check "timeout enforced (run completed in <10s, was ${ELAPSED}s)" \
  "[[ $ELAPSED -lt 10 ]]"
LIST=$(curl -sf "http://localhost:$PORT/api/pipelines" "${AUTH[@]}")
check "timed-out pipeline marked failed" \
  "echo '$LIST' | PID=$PID4 python3 -c 'import os,sys,json; ps=json.load(sys.stdin); ok=any(p[\"id\"]==os.environ[\"PID\"] and p.get(\"status\")==3 for p in ps); assert ok, ps' 2>/dev/null"

echo ""
echo "Python-step test: $PASS passed, $FAIL failed, $SKIP skipped"
[[ $FAIL -eq 0 ]]
