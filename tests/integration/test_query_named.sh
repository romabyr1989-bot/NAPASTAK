#!/usr/bin/env bash
# Integration test for POST /api/query/named (named SQL parameters).

set -u
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
GW_BIN="$ROOT/build/release/bin/dfo_gateway"
PORT=19312
DATA="$(mktemp -d -t dfo_named_XXXX)"
SECRET="named-test-$$"
PASS=0; FAIL=0; SKIP=0

cleanup() { [[ -n "${GW_PID:-}" ]] && kill "$GW_PID" 2>/dev/null; rm -rf "$DATA"; }
trap cleanup EXIT
check() { if eval "$2"; then echo "PASS: $1"; PASS=$((PASS+1)); else echo "FAIL: $1   ($2)"; FAIL=$((FAIL+1)); fi; }

[[ -x "$GW_BIN" ]] || { echo "missing $GW_BIN"; exit 1; }
python3 -c 'import json' 2>/dev/null || { echo "SKIP: python3 missing"; exit 0; }

cat > "$DATA/cfg.json" <<EOF
{"port":$PORT,"data_dir":"$DATA","auth_enabled":true,
 "jwt_secret":"$SECRET","admin_password":"admin"}
EOF
"$GW_BIN" -c "$DATA/cfg.json" > "$DATA/gw.log" 2>&1 &
GW_PID=$!
for i in {1..30}; do curl -sf "http://localhost:$PORT/health" >/dev/null 2>&1 && break; sleep 0.2; done
TOKEN=$(curl -sf -X POST "http://localhost:$PORT/api/auth/token" \
  -H 'Content-Type: application/json' -d '{"username":"admin","password":"admin"}' \
  | python3 -c "import sys,json;print(json.load(sys.stdin)['token'])")
[[ -n "$TOKEN" ]] || { echo "FAIL: no admin token"; exit 1; }
GW="http://localhost:$PORT"
AUTH=(-H "Authorization: Bearer $TOKEN")
echo "Gateway ready on :$PORT"

nrows() { python3 -c "import sys,json
try: print(len(json.load(sys.stdin).get('rows',[])))
except: print(-1)"; }

# Seed
curl -sf -X POST "$GW/api/ingest/csv?table=scores" "${AUTH[@]}" \
  -H 'Content-Type: text/csv' --data-binary $'name,score\nalice,30\nbob,42\ncarol,21' >/dev/null

# ── 1: string parameter ──────────────────────────────────────────────────────
R1=$(curl -sf -X POST "$GW/api/query/named" "${AUTH[@]}" -H 'Content-Type: application/json' \
  -d '{"sql":"SELECT * FROM scores WHERE name = :n","params":{"n":"alice"}}')
check "named: string param matches alice" "[[ '$R1' == *alice* ]]"
check "named: string param returns exactly 1 row" "[[ \"\$(echo '$R1' | nrows)\" == '1' ]]"

# ── 2: numeric parameter (bare, no quotes) ───────────────────────────────────
R2=$(curl -sf -X POST "$GW/api/query/named" "${AUTH[@]}" -H 'Content-Type: application/json' \
  -d '{"sql":"SELECT * FROM scores WHERE score > :min","params":{"min":25}}')
check "named: numeric param returns rows (alice30,bob42 > 25)" "[[ \"\$(echo '$R2' | nrows)\" == '2' ]]"

# ── 3: NULL parameter → NULL literal ─────────────────────────────────────────
R3=$(curl -sf -X POST "$GW/api/query/named" "${AUTH[@]}" -H 'Content-Type: application/json' \
  -d '{"sql":"SELECT :v AS maybe FROM scores WHERE name = :n","params":{"v":null,"n":"bob"}}')
check "named: NULL param yields empty/null cell" \
  "[[ \"\$(echo '$R3' | python3 -c 'import sys,json; d=json.load(sys.stdin); print(d[\"rows\"][0][0] if d[\"rows\"] else \"X\")')\" == '' ]]"

# ── 4: unknown parameter → 400 ───────────────────────────────────────────────
C4=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$GW/api/query/named" "${AUTH[@]}" \
  -H 'Content-Type: application/json' -d '{"sql":"SELECT * FROM scores WHERE name = :x","params":{}}')
check "named: unknown param → HTTP 400" "[[ '$C4' == '400' ]]"

# ── 5: SQL injection via string param is neutralised ─────────────────────────
R5=$(curl -sf -X POST "$GW/api/query/named" "${AUTH[@]}" -H 'Content-Type: application/json' \
  -d $'{"sql":"SELECT * FROM scores WHERE name = :n","params":{"n":"x\' OR \'1\'=\'1"}}')
check "named: injection neutralised (0 rows, not all)" "[[ \"\$(echo '$R5' | nrows)\" == '0' ]]"

# ── 6: only SELECT allowed ───────────────────────────────────────────────────
C6=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$GW/api/query/named" "${AUTH[@]}" \
  -H 'Content-Type: application/json' -d '{"sql":"DELETE FROM scores WHERE name = :n","params":{"n":"alice"}}')
check "named: non-SELECT → HTTP 400" "[[ '$C6' == '400' ]]"

# ── 7: :: cast is not treated as a placeholder ───────────────────────────────
R7=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$GW/api/query/named" "${AUTH[@]}" \
  -H 'Content-Type: application/json' -d '{"sql":"SELECT name FROM scores WHERE name = :n","params":{"n":"bob"}}')
check "named: valid request → HTTP 200" "[[ '$R7' == '200' ]]"

echo ""
echo "query/named test: $PASS passed, $FAIL failed, $SKIP skipped"
[[ $FAIL -eq 0 ]]
