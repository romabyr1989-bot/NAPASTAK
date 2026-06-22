#!/usr/bin/env bash
# Integration test for Scala pipeline steps (Spark DataFrame).
#
# Covers:
#   1. Pipeline with `scala_code` step is created and serialized
#   2. scala_code + scala_timeout_sec round-trip through GET /api/pipelines
#   3. (scala-cli present) transform_sql output is fed as CSV to scala-cli
#      stdin, user code mutating `df` (Spark DataFrame) lands in target_table
#   4. (scala-cli present) compile error → pipeline marked RUN_FAILED
#
# The serialization checks always run; the execution checks are skipped when
# `scala-cli` is not installed (Spark steps require it, just like the Python
# step requires pandas).

set -u
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
GW="$ROOT/build/release/bin/dfo_gateway"
PORT=19291
DATA="$(mktemp -d -t dfo_scalastep_XXXX)"
SECRET="scalastep-test-$$"
PASS=0; FAIL=0; SKIP=0

cleanup() { [[ -n "${GW_PID:-}" ]] && kill "$GW_PID" 2>/dev/null; rm -rf "$DATA"; }
trap cleanup EXIT
check() { if eval "$2"; then echo "PASS: $1"; PASS=$((PASS+1)); else echo "FAIL: $1   ($2)"; FAIL=$((FAIL+1)); fi; }
skip()  { echo "SKIP: $1 — $2"; SKIP=$((SKIP+1)); }

[[ -x "$GW" ]] || { echo "missing $GW"; exit 1; }

HAVE_SCALA=0
if command -v scala-cli >/dev/null 2>&1; then HAVE_SCALA=1; fi

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
echo "Gateway ready on :$PORT (scala-cli present: $HAVE_SCALA)"

AUTH=(-H "Authorization: Bearer $TOKEN")

# ── Seed source table ─────────────────────────────────────────────────────
SRC_CSV=$'name,score\nalice,30\nbob,42\ncarol,42\ndave,28\neve,35'
curl -sf -X POST "http://localhost:$PORT/api/ingest/csv?table=raw_scores" \
  "${AUTH[@]}" -H 'Content-Type: text/csv' --data-binary "$SRC_CSV" >/dev/null
check "raw_scores ingest OK" \
  "curl -sf -X POST 'http://localhost:$PORT/api/tables/query' \"\${AUTH[@]}\" \
     -H 'Content-Type: application/json' \
     -d '{\"sql\":\"SELECT count(*) FROM raw_scores\"}' | grep -q '\"5\"'"

# ── 1. Create a Scala-step pipeline + serialization round-trip ────────────
SC1=$(cat <<'SCEOF'
import org.apache.spark.sql.functions._
df = df.withColumn("score", col("score").cast("int") * 2)
df = df.filter(col("score") > 60)
df = df.withColumn("tag", lit("doubled"))
SCEOF
)
SC1_JSON=$(python3 -c 'import json,sys; print(json.dumps(sys.stdin.read()))' <<<"$SC1")

PIPELINE=$(cat <<JSON
{
  "name": "scala_doubler",
  "enabled": true,
  "steps": [
    {"id":"s1","name":"double_and_filter",
     "transform_sql":"SELECT name, score FROM raw_scores",
     "scala_code": $SC1_JSON,
     "scala_timeout_sec": 600,
     "target_table":"scores_doubled"}
  ]
}
JSON
)
RESP=$(curl -sf -X POST "http://localhost:$PORT/api/pipelines" \
  "${AUTH[@]}" -H 'Content-Type: application/json' -d "$PIPELINE")
PID=$(echo "$RESP" | python3 -c "import sys,json;d=json.load(sys.stdin);print(d.get('id',''))")
check "pipeline with scala_code created" "[[ -n '$PID' ]]"

LIST=$(curl -sf "http://localhost:$PORT/api/pipelines" "${AUTH[@]}")
check "scala_code round-trips in pipeline list" \
  "echo '$LIST' | grep -q 'scala_code'"
check "scala_timeout_sec round-trips" \
  "echo '$LIST' | grep -q '\"scala_timeout_sec\":600'"

if [[ "$HAVE_SCALA" -eq 0 ]]; then
  skip "scala execution tests" "scala-cli not installed"
  echo ""
  echo "Scala-step test: $PASS passed, $FAIL failed, $SKIP skipped"
  [[ $FAIL -eq 0 ]]
  exit $?
fi

# ── 2. Execute: double + filter → target_table ────────────────────────────
# After doubling: alice=60, bob=84, carol=84, dave=56, eve=70
# After filter > 60: bob=84, carol=84, eve=70  (3 rows)
curl -sf -X POST "http://localhost:$PORT/api/pipelines/$PID/run" "${AUTH[@]}" >/dev/null
OUT=$(curl -sf -X POST "http://localhost:$PORT/api/tables/query" "${AUTH[@]}" \
  -H 'Content-Type: application/json' \
  -d '{"sql":"SELECT name, score, tag FROM scores_doubled ORDER BY name"}')
check "scala step filter kept exactly 3 rows" \
  "echo '$OUT' | python3 -c 'import sys,json; d=json.load(sys.stdin); assert len(d[\"rows\"])==3, d' 2>/dev/null"
check "doubled values appear in output" \
  "echo '$OUT' | grep -q '\"84\"'"
check "new column 'tag' present" \
  "echo '$OUT' | grep -q '\"doubled\"'"

# ── 3. Compile error → pipeline marked failed ─────────────────────────────
SC3='this is not valid scala {{{'
SC3_JSON=$(python3 -c 'import json,sys;print(json.dumps(sys.stdin.read()))' <<<"$SC3")
PIPELINE3=$(cat <<JSON
{"name":"scala_broken","enabled":true,
 "steps":[{"id":"s1","name":"broken",
           "transform_sql":"SELECT * FROM raw_scores",
           "scala_code":$SC3_JSON,
           "scala_timeout_sec":600,
           "max_retries":0,
           "target_table":"never_made"}]}
JSON
)
RESP=$(curl -sf -X POST "http://localhost:$PORT/api/pipelines" \
  "${AUTH[@]}" -H 'Content-Type: application/json' -d "$PIPELINE3")
PID3=$(echo "$RESP" | python3 -c "import sys,json;print(json.load(sys.stdin).get('id',''))")
curl -sf -X POST "http://localhost:$PORT/api/pipelines/$PID3/run" "${AUTH[@]}" >/dev/null
LIST=$(curl -sf "http://localhost:$PORT/api/pipelines" "${AUTH[@]}")
# Pipeline.run_status is serialized as JSON field "status" (top-level); RUN_FAILED == 3.
check "broken scala pipeline marked failed" \
  "echo '$LIST' | PID=$PID3 python3 -c 'import os,sys,json; ps=json.load(sys.stdin); ok=any(p[\"id\"]==os.environ[\"PID\"] and p.get(\"status\")==3 for p in ps); assert ok, ps' 2>/dev/null"

echo ""
echo "Scala-step test: $PASS passed, $FAIL failed, $SKIP skipped"
[[ $FAIL -eq 0 ]]
