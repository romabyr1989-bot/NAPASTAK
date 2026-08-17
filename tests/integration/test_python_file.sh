#!/usr/bin/env bash
# Integration test for python_file and python_context_dir (stateful Python steps).
#
# Covers:
#   1. python_file with a small script → executes like inline python_code.
#   2. python_file > 8 KB (the old inline limit) → accepted and executed.
#   3. python_context_dir → DFO_CONTEXT_DIR shared between steps A and B.
#   4. nonexistent python_file → step FAILED, "cannot open python_file" message.
#   5. python_file > 512 KB → step FAILED, "python_file too large" message.
#   6. existing inline python_code path (test_python_step.sh) still passes.
#
# Error-message assertions use POST /api/pipelines/preview-step, which returns
# {"error": "<step error_msg>"} synchronously on failure.

set -u
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
GW="$ROOT/build/release/bin/napastak_gateway"
PORT=19306
DATA="$(mktemp -d -t dfo_pyfile_XXXX)"
SECRET="pyfile-test-$$"
PASS=0; FAIL=0; SKIP=0

cleanup() { [[ -n "${GW_PID:-}" ]] && kill "$GW_PID" 2>/dev/null; rm -rf "$DATA"; }
trap cleanup EXIT
check() { if eval "$2"; then echo "PASS: $1"; PASS=$((PASS+1)); else echo "FAIL: $1   ($2)"; FAIL=$((FAIL+1)); fi; }
skip()  { echo "SKIP: $1 — $2"; SKIP=$((SKIP+1)); }

[[ -x "$GW" ]] || { echo "missing $GW"; exit 1; }
if ! python3 -c 'import pandas' 2>/dev/null; then
  echo "SKIP: pandas not installed — Python steps require pandas"; exit 0
fi

cat > "$DATA/cfg.json" <<EOF
{"port":$PORT,"data_dir":"$DATA","auth_enabled":true,
 "jwt_secret":"$SECRET","admin_password":"admin"}
EOF
"$GW" -c "$DATA/cfg.json" > "$DATA/gw.log" 2>&1 &
GW_PID=$!
for i in {1..30}; do curl -sf "http://localhost:$PORT/health" >/dev/null 2>&1 && break; sleep 0.2; done
TOKEN=$(curl -sf -X POST "http://localhost:$PORT/api/auth/token" \
  -H 'Content-Type: application/json' -d '{"username":"admin","password":"admin"}' \
  | python3 -c "import sys,json;print(json.load(sys.stdin)['token'])")
[[ -n "$TOKEN" ]] || { echo "FAIL: no admin token"; exit 1; }
echo "Gateway ready on :$PORT"
AUTH=(-H "Authorization: Bearer $TOKEN")

qval() {  # run SQL, print rows[0][0]
  curl -sf -X POST "http://localhost:$PORT/api/tables/query" "${AUTH[@]}" \
    -H 'Content-Type: application/json' -d "{\"sql\":\"$1\"}" 2>/dev/null \
    | python3 -c "import sys,json
try:
  d=json.load(sys.stdin); print(d['rows'][0][0] if d.get('rows') else '')
except: print('')"
}
preview_err() {  # POST a single-step pipeline to preview-step, print .error
  curl -s -X POST "http://localhost:$PORT/api/pipelines/preview-step?save=0" "${AUTH[@]}" \
    -H 'Content-Type: application/json' -d "$1" \
    | python3 -c "import sys,json
try: print(json.load(sys.stdin).get('error',''))
except: print('')"
}

# Seed source
SRC=$'name,score\nalice,30\nbob,42\ncarol,21'
curl -sf -X POST "http://localhost:$PORT/api/ingest/csv?table=raw_scores" "${AUTH[@]}" \
  -H 'Content-Type: text/csv' --data-binary "$SRC" >/dev/null

# ── Test 1: python_file with a small script ──────────────────────────────────
cat > "$DATA/double.py" <<'PYEOF'
df['score'] = df['score'].astype(int) * 2
PYEOF
P1="{\"name\":\"pf1\",\"enabled\":true,\"steps\":[{\"id\":\"s1\",
  \"transform_sql\":\"SELECT name, score FROM raw_scores\",
  \"python_file\":\"$DATA/double.py\",\"target_table\":\"doubled\"}]}"
PID=$(curl -sf -X POST "http://localhost:$PORT/api/pipelines" "${AUTH[@]}" \
  -H 'Content-Type: application/json' -d "$P1" | python3 -c "import sys,json;print(json.load(sys.stdin).get('id',''))")
curl -sf -X POST "http://localhost:$PORT/api/pipelines/$PID/run" "${AUTH[@]}" >/dev/null
check "python_file: alice score doubled to 60" "[[ \"$(qval "SELECT score FROM doubled WHERE name='alice'")\" == '60' ]]"
check "python_file round-trips in pipeline list" \
  "curl -sf 'http://localhost:$PORT/api/pipelines' \"\${AUTH[@]}\" | grep -q 'python_file'"

# ── Test 2: python_file > 8 KB (old inline limit) ────────────────────────────
{ for i in $(seq 1 1200); do echo "# padding line $i to exceed 8KB inline limit"; done
  echo "df['score'] = df['score'].astype(int) + 1"; } > "$DATA/big.py"
BIGSZ=$(wc -c < "$DATA/big.py")
check "python_file: big.py is > 8192 bytes (${BIGSZ})" "[[ $BIGSZ -gt 8192 ]]"
P2="{\"name\":\"pf2\",\"enabled\":true,\"steps\":[{\"id\":\"s1\",
  \"transform_sql\":\"SELECT name, score FROM raw_scores\",
  \"python_file\":\"$DATA/big.py\",\"target_table\":\"plus1\"}]}"
PID2=$(curl -sf -X POST "http://localhost:$PORT/api/pipelines" "${AUTH[@]}" \
  -H 'Content-Type: application/json' -d "$P2" | python3 -c "import sys,json;print(json.load(sys.stdin).get('id',''))")
curl -sf -X POST "http://localhost:$PORT/api/pipelines/$PID2/run" "${AUTH[@]}" >/dev/null
check "python_file >8KB: executes (bob 42→43)" "[[ \"$(qval "SELECT score FROM plus1 WHERE name='bob'")\" == '43' ]]"

# ── Test 3: python_context_dir — state passed between steps A and B ───────────
cat > "$DATA/step_a.py" <<'PYEOF'
import os, json
ctx = os.environ['DFO_CONTEXT_DIR']
with open(f'{ctx}/state.json', 'w') as f:
    json.dump({'count': len(df)}, f)
PYEOF
cat > "$DATA/step_b.py" <<'PYEOF'
import os, json, pandas as pd
ctx = os.environ['DFO_CONTEXT_DIR']
with open(f'{ctx}/state.json') as f:
    state = json.load(f)
df = pd.DataFrame([state])
PYEOF
CTX="$DATA/ctx_run"
P3="{\"name\":\"pf_ctx\",\"enabled\":true,\"steps\":[
  {\"id\":\"a\",\"transform_sql\":\"SELECT * FROM raw_scores\",
   \"python_file\":\"$DATA/step_a.py\",\"python_context_dir\":\"$CTX\"},
  {\"id\":\"b\",\"python_file\":\"$DATA/step_b.py\",
   \"python_context_dir\":\"$CTX\",\"target_table\":\"ctx_result\",\"deps\":[0]}]}"
PID3=$(curl -sf -X POST "http://localhost:$PORT/api/pipelines" "${AUTH[@]}" \
  -H 'Content-Type: application/json' -d "$P3" | python3 -c "import sys,json;print(json.load(sys.stdin).get('id',''))")
curl -sf -X POST "http://localhost:$PORT/api/pipelines/$PID3/run" "${AUTH[@]}" >/dev/null
check "python_context_dir: dir was created" "[[ -d '$CTX' ]]"
check "python_context_dir: state.json written by step A" "[[ -f '$CTX/state.json' ]]"
check "python_context_dir: step B read count=3 from step A" "[[ \"$(qval 'SELECT count FROM ctx_result')\" == '3' ]]"

# ── Test 4: nonexistent python_file → FAILED with clear message ───────────────
P4='{"name":"pf_bad","enabled":false,"steps":[{"id":"s1",
  "transform_sql":"SELECT * FROM raw_scores",
  "python_file":"/nonexistent/does_not_exist.py","target_table":"never","max_retries":0}]}'
ERR=$(preview_err "$P4")
check "python_file missing → 'cannot open python_file'" "[[ '$ERR' == *'cannot open python_file'* ]]"

# ── Test 5: python_file > 512 KB → FAILED with 'too large' ───────────────────
python3 -c "open('$DATA/huge.py','w').write('# pad\n'*100000)"   # ~600 KB
HUGESZ=$(wc -c < "$DATA/huge.py")
check "python_file: huge.py is > 524288 bytes (${HUGESZ})" "[[ $HUGESZ -gt 524288 ]]"
P5="{\"name\":\"pf_huge\",\"enabled\":false,\"steps\":[{\"id\":\"s1\",
  \"transform_sql\":\"SELECT * FROM raw_scores\",
  \"python_file\":\"$DATA/huge.py\",\"target_table\":\"never\",\"max_retries\":0}]}"
ERR5=$(preview_err "$P5")
check "python_file >512KB → 'too large'" "[[ '$ERR5' == *'too large'* ]]"

# ── Test 6: inline python_code path unbroken ─────────────────────────────────
bash "$ROOT/tests/integration/test_python_step.sh" >/dev/null 2>&1
check "inline python_code path: test_python_step.sh still passes" "[[ $? -eq 0 ]]"

echo ""
echo "python_file test: $PASS passed, $FAIL failed, $SKIP skipped"
[[ $FAIL -eq 0 ]]
