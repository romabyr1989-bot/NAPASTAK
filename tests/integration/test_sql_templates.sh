#!/usr/bin/env bash
# SQL-template engine: file templates, {param} substitution, @computed params.
set -u
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
GW_BIN="$ROOT/build/release/bin/napastak_gateway"
PORT=19396
DATA="$(mktemp -d -t dfo_tmpl_XXXX)"
SECRET="tmpl-secret-$$"
PASS=0; FAIL=0

cleanup() { [[ -n "${GW_PID:-}" ]] && kill "$GW_PID" 2>/dev/null; rm -rf "$DATA"; }
trap cleanup EXIT
check() { if eval "$2"; then echo "PASS: $1"; PASS=$((PASS+1)); else echo "FAIL: $1   ($2)"; FAIL=$((FAIL+1)); fi; }

[[ -x "$GW_BIN" ]] || { echo "missing $GW_BIN"; exit 1; }
python3 -c 'import json' 2>/dev/null || { echo "SKIP: python3 missing"; exit 0; }

TOKEN=$(python3 -c "
import hmac,hashlib,base64,json,time
b=lambda x: base64.urlsafe_b64encode(x).rstrip(b'=').decode()
h=b(b'{\"alg\":\"HS256\",\"typ\":\"JWT\"}')
p=b(json.dumps({'sub':'admin','role':0,'exp':int(time.time())+3600},separators=(',',':')).encode())
s=b(hmac.new('$SECRET'.encode(),f'{h}.{p}'.encode(),hashlib.sha256).digest())
print(f'{h}.{p}.{s}')")

mkdir -p "$DATA/sql"
cat > "$DATA/cfg.json" <<EOF
{"port":$PORT,"data_dir":"$DATA","auth_enabled":true,"jwt_secret":"$SECRET",
 "admin_password":"admin","sql_templates_dir":"$DATA/sql"}
EOF
"$GW_BIN" -c "$DATA/cfg.json" > "$DATA/gw.log" 2>&1 &
GW_PID=$!
for i in {1..30}; do curl -sf "http://localhost:$PORT/health" >/dev/null 2>&1 && break; sleep 0.2; done
GW="http://localhost:$PORT"; AUTH=(-H "Authorization: Bearer $TOKEN")
echo "Gateway ready on :$PORT"

query_val() { curl -s -X POST "$GW/api/tables/query" "${AUTH[@]}" -H 'Content-Type: application/json' \
  -d "{\"sql\":$(python3 -c 'import json,sys;print(json.dumps(sys.argv[1]))' "$1")}" \
  | python3 -c "import sys,json
try:
  d=json.load(sys.stdin); print(d['rows'][0][0] if d.get('rows') else '')
except: print('')"; }
mkpipe() { curl -sf -X POST "$GW/api/pipelines" "${AUTH[@]}" -H 'Content-Type: application/json' -d "$1" \
  | python3 -c "import sys,json;print(json.load(sys.stdin).get('id',''))"; }
run_pipe() { curl -s -X POST "$GW/api/pipelines/$1/run" "${AUTH[@]}" >/dev/null; }
run_error() { curl -s "$GW/api/pipelines/$1/runs" "${AUTH[@]}" \
  | python3 -c "import sys,json
try:
  d=json.load(sys.stdin); print(d[0].get('error','') if d else '')
except: print('')"; }

# Seed
curl -sf -X POST "$GW/api/ingest/csv?table=scores" "${AUTH[@]}" -H 'Content-Type: text/csv' \
  --data-binary $'name,score\nAlice,80\nBob,20\nCarol,90' >/dev/null

# ── 1. file template + literal params (_raw identifier + number) ──
printf 'SELECT * FROM {table_raw} WHERE score > {min_score}\n' > "$DATA/sql/simple.sql"
P1=$(mkpipe '{"name":"tmpl_test","enabled":true,"steps":[{"id":"filter","sql_template":"simple.sql","sql_params":"{\"table_raw\":\"scores\",\"min_score\":50}","target_table":"high_scores"}]}')
run_pipe "$P1"
check "template: file loaded + params substituted (2 rows > 50)" \
  "[[ \"\$(query_val 'SELECT count(*) FROM high_scores')\" == '2' ]]"
check "template: _raw used as identifier (Alice present)" \
  "[[ \"\$(query_val \"SELECT name FROM high_scores WHERE name='Alice'\")\" == 'Alice' ]]"

# ── 2. @now computed + string param quoted ──
printf 'SELECT name, {@now} AS captured_at FROM scores WHERE name = {target_name}\n' > "$DATA/sql/with_now.sql"
P2=$(mkpipe '{"name":"now_test","enabled":true,"steps":[{"id":"cap","sql_template":"with_now.sql","sql_params":"{\"target_name\":\"Bob\"}","target_table":"captured"}]}')
run_pipe "$P2"
check "template: @now substituted (captured_at non-empty)" \
  "[[ -n \"\$(query_val 'SELECT captured_at FROM captured LIMIT 1')\" ]]"
check "template: string param quoted (Bob matched)" \
  "[[ \"\$(query_val 'SELECT name FROM captured')\" == 'Bob' ]]"

# ── 3. @step:<id>:<col> list from a prior step ──
printf 'SELECT * FROM scores WHERE name IN ({@step:pick:name})\n' > "$DATA/sql/in_list.sql"
P3=$(mkpipe '{"name":"step_test","enabled":true,"steps":[{"id":"pick","transform_sql":"SELECT name FROM scores WHERE score >= 80","target_table":"picked"},{"id":"expand","sql_template":"in_list.sql","target_table":"expanded","deps":[0]}]}')
run_pipe "$P3"
check "template: @step IN-list expanded (Alice+Carol = 2)" \
  "[[ \"\$(query_val 'SELECT count(*) FROM expanded')\" == '2' ]]"

# ── 4. path traversal blocked ──
P4=$(mkpipe '{"name":"evil","enabled":true,"steps":[{"id":"x","sql_template":"../../../etc/passwd","target_table":"hacked"}]}')
run_pipe "$P4"
check "template: path traversal blocked (no 'hacked' table)" \
  "[[ -z \"\$(query_val 'SELECT count(*) FROM hacked')\" ]]"
check "template: traversal error reported" \
  "[[ \"\$(run_error \"$P4\")\" == *\"must not contain\"* ]]"

# ── 5. unresolved param → FAILED with clear message ──
printf 'SELECT * FROM scores WHERE name = {nonexistent}\n' > "$DATA/sql/missing.sql"
P5=$(mkpipe '{"name":"miss","enabled":true,"steps":[{"id":"m","sql_template":"missing.sql","sql_params":"{}","target_table":"x"}]}')
run_pipe "$P5"
check "template: unresolved param → error 'unresolved SQL param'" \
  "[[ \"\$(run_error \"$P5\")\" == *'unresolved SQL param'* ]]"

# ── 6. regression: plain transform_sql (no braces) still works ──
P6=$(mkpipe '{"name":"plain","enabled":true,"steps":[{"id":"p","transform_sql":"SELECT * FROM scores WHERE score < 50","target_table":"low"}]}')
run_pipe "$P6"
check "regression: inline transform_sql without {} (Bob only)" \
  "[[ \"\$(query_val 'SELECT count(*) FROM low')\" == '1' ]]"

echo ""
echo "sql templates test: $PASS passed, $FAIL failed"
[[ $FAIL -eq 0 ]]
