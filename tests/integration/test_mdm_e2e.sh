#!/usr/bin/env bash
# End-to-end MDM chain: source slice → SQL normalize → match → SCD2 → REST query.
# Runs the pipeline twice (a delta between runs) and asserts SCD2 versioning +
# that every stage executes through the REST/pipeline path.
#
# Scope notes (see docs/MDM_E2E_REPORT.md):
#   - "source(cursor)" is represented here by a re-ingested slice; the incremental
#     high-watermark pull itself is connector-level and covered by test_pg_cdc.sh.
#   - the match stage uses jaro_winkler(); see the report for the engine gap that
#     currently makes it a no-op (the chain RUNS, but match emits no similarity).

set -u
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
GW="$ROOT/build/release/bin/napastak_gateway"
PORT=19299
DATA="$(mktemp -d -t dfo_mdm_XXXX)"
PASS=0; FAIL=0
cleanup() { [[ -n "${GW_PID:-}" ]] && kill "$GW_PID" 2>/dev/null; rm -rf "$DATA"; }
trap cleanup EXIT
check() { if eval "$2"; then echo "PASS: $1"; PASS=$((PASS+1)); else echo "FAIL: $1   ($2)"; FAIL=$((FAIL+1)); fi; }

[[ -x "$GW" ]] || { echo "missing $GW (run: make release)"; exit 1; }
python3 -c 'import json' 2>/dev/null || { echo "SKIP: python3 unavailable"; exit 0; }

cat > "$DATA/cfg.json" <<EOF
{"port":$PORT,"data_dir":"$DATA","auth_enabled":true,"jwt_secret":"mdm","admin_password":"admin"}
EOF
"$GW" -c "$DATA/cfg.json" >"$DATA/gw.log" 2>&1 & GW_PID=$!
for i in {1..30}; do curl -sf "http://localhost:$PORT/health" >/dev/null 2>&1 && break; sleep 0.2; done
TOKEN=$(curl -sf -X POST "http://localhost:$PORT/api/auth/token" -H 'Content-Type: application/json' \
  -d '{"username":"admin","password":"admin"}' | python3 -c "import sys,json;print(json.load(sys.stdin)['token'])")
[[ -n "$TOKEN" ]] || { echo "FAIL: no token"; exit 1; }
A=(-H "Authorization: Bearer $TOKEN")
echo "Gateway ready on :$PORT"

cat > "$DATA/m.py" <<'PYEOF'
import sys, json
from collections import Counter
d = json.load(sys.stdin)
ci = {c: i for i, c in enumerate(d["columns"])}; rows = d["rows"]
vt, bk = ci["valid_to"], ci["id"]
op = sum(1 for r in rows if r[vt] == "")
cnt = Counter(r[bk] for r in rows); oc = Counter(r[bk] for r in rows if r[vt] == "")
print(f"total={len(rows)}"); print(f"open={op}"); print(f"closed={len(rows)-op}")
for k in cnt: print(f"cnt_{k}={cnt[k]}"); print(f"open_{k}={oc.get(k,0)}")
PYEOF
ingest() { curl -sf -X DELETE "http://localhost:$PORT/api/tables/$1" "${A[@]}" >/dev/null 2>&1 || true
           curl -sf -X POST "http://localhost:$PORT/api/ingest/csv?table=$1" "${A[@]}" \
                 -H 'Content-Type: text/csv' --data-binary "$2" >/dev/null; }
q() { curl -sf -X POST "http://localhost:$PORT/api/tables/query" "${A[@]}" -H 'Content-Type: application/json' -d "{\"sql\":\"$1\"}"; }
run() { curl -sf -X POST "http://localhost:$PORT/api/pipelines/$PID/run" "${A[@]}" >/dev/null; }

# ── Build the 3-stage MDM pipeline: normalize → match → scd2 ────────────────
PIPE=$(cat <<'JSON'
{"name":"mdm_e2e","enabled":true,"steps":[
  {"id":"normalize","name":"normalize",
   "transform_sql":"SELECT id, trim(lower(name)) AS name, lower(email) AS email, updated_at, is_deleted FROM src_customers",
   "target_table":"clean_customers"},
  {"id":"match","name":"match",
   "transform_sql":"SELECT a.id AS a_id, b.id AS b_id, jaro_winkler(a.name, b.name) AS sim FROM clean_customers a, clean_customers b WHERE a.id < b.id",
   "target_table":"customer_matches"},
  {"id":"scd2","name":"scd2",
   "transform_sql":"SELECT id, name, email, updated_at, is_deleted FROM clean_customers",
   "target_table":"dim_customer",
   "scd2_business_key":"id","scd2_compare_columns":"name,email",
   "scd2_transaction_time":"updated_at","scd2_effective_from_col":"valid_from",
   "scd2_effective_to_col":"valid_to","scd2_deleted_flag":"is_deleted"}
]}
JSON
)
PID=$(curl -sf -X POST "http://localhost:$PORT/api/pipelines" "${A[@]}" -H 'Content-Type: application/json' -d "$PIPE" \
      | python3 -c "import sys,json;print(json.load(sys.stdin).get('id',''))")
check "MDM pipeline created (3 stages)" "[[ -n '$PID' ]]"

# ── Run 1: initial slice (4 customers, C1/C4 near-dup names) ─────────────────
ingest src_customers $'id,name,email,updated_at,is_deleted\nC1,  Alice  ,A@X.COM,100,0\nC2,Bob,b@x.com,100,0\nC3,Carol,c@x.com,100,0\nC4,Alicia,a2@x.com,100,0'
run
check "normalize ran: clean_customers has 4 rows" \
  "q 'SELECT count(*) AS n FROM clean_customers' | grep -q '\"4\"'"
check "normalize trims/lowercases (alice, a@x.com)" \
  "q \"SELECT name,email FROM clean_customers WHERE id='C1'\" | grep -q '\"alice\"' && q \"SELECT name,email FROM clean_customers WHERE id='C1'\" | grep -q '\"a@x.com\"'"
check "match stage produced a table (6 candidate pairs)" \
  "q 'SELECT count(*) AS n FROM customer_matches' | grep -q '\"6\"'"

D=$(q 'SELECT id, valid_from, valid_to FROM dim_customer')
eval "$(echo "$D" | python3 "$DATA/m.py")"
check "scd2 run1: 4 open versions"  "[[ '$total' == 4 && '$open' == 4 && '$closed' == 0 ]]"

# Match now computes real similarity (jaro_winkler wired into api.c eval_func).
# C1 "alice" vs C4 "alicia" are near-duplicates → high score.
SIM=$(q "SELECT sim FROM customer_matches WHERE a_id='C1' AND b_id='C4'" | python3 -c "import sys,json;d=json.load(sys.stdin);print(d['rows'][0][0] if d['rows'] else 'NONE')")
check "match computes jaro_winkler similarity (C1~C4 > 0.8)" \
  "python3 -c \"import sys; v='${SIM}'; sys.exit(0 if (v not in ('','NONE') and float(v) > 0.8) else 1)\""
echo "      (C1 'alice' ~ C4 'alicia' similarity = ${SIM})"

# ── Run 2: delta — C1 name changed, C3 soft-deleted, C5 new, C2/C4 unchanged ─
ingest src_customers $'id,name,email,updated_at,is_deleted\nC1,Alison,a@x.com,200,0\nC2,Bob,b@x.com,100,0\nC3,Carol,c@x.com,300,1\nC4,Alicia,a2@x.com,100,0\nC5,Dave,d@x.com,200,0'
run
D=$(q 'SELECT id, valid_from, valid_to FROM dim_customer')
eval "$(echo "$D" | python3 "$DATA/m.py")"
check "scd2 run2: C1 changed → 2 versions, 1 open" "[[ '$cnt_C1' == 2 && '$open_C1' == 1 ]]"
check "scd2 run2: C3 deleted → closed, 0 open"     "[[ '$open_C3' == 0 && '$cnt_C3' == 1 ]]"
check "scd2 run2: C5 new → 1 open version"         "[[ '$cnt_C5' == 1 && '$open_C5' == 1 ]]"
check "scd2 run2: C2 unchanged → still 1 open"     "[[ '$cnt_C2' == 1 && '$open_C2' == 1 ]]"
check "scd2 run2: golden = one open row per key"   "[[ '$open' == 4 ]]"   # C1',C2,C4,C5 open; C1,C3 closed
check "scd2 run2: total = 6 versions (4+2 closed)" "[[ '$total' == 6 && '$closed' == 2 ]]"

echo ""
echo "MDM E2E: $PASS passed, $FAIL failed"
[[ $FAIL -eq 0 ]]
