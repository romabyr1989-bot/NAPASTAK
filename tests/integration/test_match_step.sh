#!/usr/bin/env bash
# Integration test for the match step + new QEngine scalar functions
# (word_similarity, normalize_inn, normalize_name).

set -u
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
GW="$ROOT/build/release/bin/napastak_gateway"
PORT=19307
DATA="$(mktemp -d -t dfo_match_XXXX)"
SECRET="match-test-$$"
PASS=0; FAIL=0; SKIP=0

cleanup() { [[ -n "${GW_PID:-}" ]] && kill "$GW_PID" 2>/dev/null; rm -rf "$DATA"; }
trap cleanup EXIT
check() { if eval "$2"; then echo "PASS: $1"; PASS=$((PASS+1)); else echo "FAIL: $1   ($2)"; FAIL=$((FAIL+1)); fi; }

[[ -x "$GW" ]] || { echo "missing $GW"; exit 1; }
python3 -c 'import json' 2>/dev/null || { echo "SKIP: python3 not available"; exit 0; }

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

# One-row table so scalar-only SELECTs have a row to project over.
curl -sf -X POST "http://localhost:$PORT/api/ingest/csv?table=d1" "${AUTH[@]}" \
  -H 'Content-Type: text/csv' --data-binary $'x\n1' >/dev/null

qval() {  # run SQL, print rows[0][0]
  curl -sf -X POST "http://localhost:$PORT/api/tables/query" "${AUTH[@]}" \
    -H 'Content-Type: application/json' -d "{\"sql\":$(python3 -c "import json,sys;print(json.dumps(sys.argv[1]))" "$1")}" 2>/dev/null \
    | python3 -c "import sys,json
try:
  d=json.load(sys.stdin); print(d['rows'][0][0] if d.get('rows') else '')
except: print('')"
}
flt_lt_1() { python3 -c "import sys;v='$1';sys.exit(0 if v and float(v)<1.0 else 1)"; }

# ── word_similarity ──────────────────────────────────────────────────────────
WS_SAME=$(qval "SELECT word_similarity('abc','abc') FROM d1")
WS_DIFF=$(qval "SELECT word_similarity('abc','xyz') FROM d1")
WS_NEAR=$(qval "SELECT word_similarity('Ivanov','Ivano') FROM d1")   # 'nov' trigram missing → 0.75
check "word_similarity('abc','abc') = 1.0"   "python3 -c \"import sys;sys.exit(0 if float('$WS_SAME')==1.0 else 1)\""
check "word_similarity('abc','xyz') = 0.0"   "python3 -c \"import sys;sys.exit(0 if float('$WS_DIFF')==0.0 else 1)\""
check "word_similarity near-match in (0,1)"  "flt_lt_1 '$WS_NEAR' && python3 -c \"import sys;sys.exit(0 if float('$WS_NEAR')>0 else 1)\""

# ── normalize_inn ────────────────────────────────────────────────────────────
check "normalize_inn('12-34 56') = '123456'" "[[ \"$(qval "SELECT normalize_inn('12-34 56') FROM d1")\" == '123456' ]]"
check "normalize_inn('') = ''"                "[[ -z \"$(qval "SELECT normalize_inn('') FROM d1")\" ]]"

# ── normalize_name (ASCII case-fold; trims + collapses spaces) ───────────────
check "normalize_name('  IVAN  ') = 'ivan'"            "[[ \"$(qval "SELECT normalize_name('  IVAN  ') FROM d1")\" == 'ivan' ]]"
check "normalize_name('Ivan  Ivanov') collapses spaces" "[[ \"$(qval "SELECT normalize_name('Ivan  Ivanov') FROM d1")\" == 'ivan ivanov' ]]"

# ── Seed persons (some potential duplicates) ─────────────────────────────────
PCSV='party_id,last_name,first_name,birth_date,inn,dul_number
P1,Ivanov,Ivan,1980-01-01,123456789012,4500123456
P2,Ivanov,Ivan,1980-01-01,123456789012,4500123456
P3,Petrov,Petr,1990-05-15,987654321098,6312345678
P4,Ivanov,Ivane,1980-01-01,,
P5,Sidorov,Sidor,1975-11-20,111111111111,7700000001'
curl -sf -X POST "http://localhost:$PORT/api/ingest/csv?table=persons" "${AUTH[@]}" \
  -H 'Content-Type: text/csv' --data-binary "$PCSV" >/dev/null

mk_run() {  # $1=pipeline json → create+run, echo nothing
  local pid
  pid=$(curl -sf -X POST "http://localhost:$PORT/api/pipelines" "${AUTH[@]}" \
    -H 'Content-Type: application/json' -d "$1" | python3 -c "import sys,json;print(json.load(sys.stdin).get('id',''))")
  curl -sf -X POST "http://localhost:$PORT/api/pipelines/$pid/run" "${AUTH[@]}" >/dev/null
}

# ── Rule 1: exact DUL group, test FIO, word_similarity >= 0.90 ───────────────
RULES1='[{"rule_name":"DUL exact","id_column":"party_id","scan_columns":["dul_number"],"test_columns":["last_name","first_name"],"metric_function":"word_similarity","operator":">=","threshold":0.90}]'
P1=$(python3 -c "import json,sys;print(json.dumps({'name':'pm1','enabled':True,'steps':[{'id':'m','transform_sql':'SELECT * FROM persons','target_table':'matches','match_rules':sys.argv[1]}]}))" "$RULES1")
mk_run "$P1"
check "match_step: output has id_a/id_b/rule_name/metric_value" \
  "curl -sf -X POST 'http://localhost:$PORT/api/tables/query' \"\${AUTH[@]}\" -H 'Content-Type: application/json' -d '{\"sql\":\"SELECT * FROM matches\"}' | grep -q id_a && curl -sf -X POST 'http://localhost:$PORT/api/tables/query' \"\${AUTH[@]}\" -H 'Content-Type: application/json' -d '{\"sql\":\"SELECT * FROM matches\"}' | grep -q metric_value"
check "match_step: P1-P2 matched (same DUL)" \
  "[[ \"$(qval "SELECT count(*) FROM matches WHERE (id_a='P1' AND id_b='P2') OR (id_a='P2' AND id_b='P1')")\" -ge 1 ]]"
# count(*) over a 0-row filter yields no row (engine filters before aggregating)
# → an empty result means "no such matched pair".
check "match_step: P3 not matched with P1" \
  "[[ -z \"$(qval "SELECT count(*) FROM matches WHERE (id_a='P1' AND id_b='P3') OR (id_a='P3' AND id_b='P1')")\" ]]"
check "match_step: P4 (empty DUL) not matched by DUL rule" \
  "[[ -z \"$(qval "SELECT count(*) FROM matches WHERE id_a='P4' OR id_b='P4'")\" ]]"

# ── Rule 2 chain: DUL + FIO+BD (birth_date group, threshold 0.66) ────────────
RULES2='[{"rule_name":"DUL","id_column":"party_id","scan_columns":["dul_number"],"test_columns":["last_name"],"metric_function":"word_similarity","operator":">=","threshold":0.90},{"rule_name":"FIO+BD","id_column":"party_id","scan_columns":["birth_date"],"test_columns":["last_name","first_name"],"metric_function":"word_similarity","operator":">=","threshold":0.66,"normalize":"name"}]'
P2J=$(python3 -c "import json,sys;print(json.dumps({'name':'pm2','enabled':True,'steps':[{'id':'m','transform_sql':'SELECT * FROM persons','target_table':'matches2','match_rules':sys.argv[1]}]}))" "$RULES2")
mk_run "$P2J"
check "match_step: P4 matched with P1 by FIO+BD (threshold 0.66)" \
  "[[ \"$(qval "SELECT count(*) FROM matches2 WHERE rule_name='FIO+BD' AND ((id_a='P1' AND id_b='P4') OR (id_a='P4' AND id_b='P1'))")\" -ge 1 ]]"
check "match_step: P3 (Petrov) not matched by any rule" \
  "[[ -z \"$(qval "SELECT count(*) FROM matches2 WHERE id_a='P3' OR id_b='P3'")\" ]]"

echo ""
echo "Match-step test: $PASS passed, $FAIL failed, $SKIP skipped"
[[ $FAIL -eq 0 ]]
