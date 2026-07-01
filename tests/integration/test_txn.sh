#!/usr/bin/env bash
# test_txn.sh — SQL DML transaction test (INSERT / SELECT / DELETE / UPDATE)
# Port 19203
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
GW="$PROJECT_ROOT/build/release/bin/dfo_gateway"
PORT=19203
BASE="http://127.0.0.1:$PORT"
DATA_DIR="/tmp/dfo_test_txn_$$"
CFG_FILE="/tmp/dfo_test_txn_$$.json"
PASS=0
FAIL=0

check() {
    local desc="$1"
    local cond="$2"
    if [ "$cond" = "1" ]; then
        echo "PASS: $desc"
        PASS=$((PASS + 1))
    else
        echo "FAIL: $desc"
        FAIL=$((FAIL + 1))
    fi
}

cleanup() {
    if [ -n "${GW_PID:-}" ] && kill -0 "$GW_PID" 2>/dev/null; then
        kill "$GW_PID" 2>/dev/null || true
        wait "$GW_PID" 2>/dev/null || true
    fi
    rm -rf "$DATA_DIR" "$CFG_FILE"
}
trap cleanup EXIT

mkdir -p "$DATA_DIR"
cat > "$CFG_FILE" <<EOF
{
  "port": $PORT,
  "data_dir": "$DATA_DIR",
  "auth_enabled": true,
  "rbac_enabled": false,
  "cluster_mode": false,
  "jwt_secret": "test-secret-txn-integration"
}
EOF

# Start gateway
"$GW" -c "$CFG_FILE" >"$DATA_DIR/gw.log" 2>&1 &
GW_PID=$!

# Wait for ready
for i in $(seq 1 30); do
    STATUS=$(curl -s -o /dev/null -w "%{http_code}" "$BASE/health" 2>/dev/null || true)
    [ "$STATUS" = "200" ] && break
    sleep 0.3
done
STATUS=$(curl -s -o /dev/null -w "%{http_code}" "$BASE/health" 2>/dev/null || true)
check "gateway started and healthy" "$([ "$STATUS" = "200" ] && echo 1 || echo 0)"

# Auth
RESP=$(curl -s -w "\n%{http_code}" -X POST "$BASE/api/auth/token" \
    -H "Content-Type: application/json" \
    -d '{"username":"admin","password":"admin"}')
HTTP_CODE=$(echo "$RESP" | tail -1)
BODY=$(echo "$RESP" | sed '$d')
TOKEN=$(echo "$BODY" | grep -o '"token":"[^"]*"' | cut -d'"' -f4)
check "admin auth returns 200" "$([ "$HTTP_CODE" = "200" ] && echo 1 || echo 0)"
AUTH="Authorization: Bearer $TOKEN"

query() {
    local sql="$1"
    # Escape backslashes and double-quotes for JSON embedding
    local escaped
    escaped=$(printf '%s' "$sql" | sed 's/\\/\\\\/g; s/"/\\"/g')
    curl -s -w "\n%{http_code}" -X POST "$BASE/api/tables/query" \
        -H "Content-Type: application/json" \
        -H "$AUTH" \
        -d "{\"sql\":\"$escaped\"}"
}

# NAPASTAK doesn't support `CREATE TABLE` / `INSERT INTO ... VALUES` —
# tables are created via the CSV ingest endpoint instead. Bulk-load 3
# rows in one shot. The table comes alive with the inferred schema.
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X POST \
    "$BASE/api/ingest/csv?table=txn_test" \
    -H "$AUTH" -H 'Content-Type: text/csv' \
    --data-binary $'id,name,score\n1,alice,100\n2,bob,200\n3,carol,300\n')
check "ingest 3 rows into txn_test" "$([ "$HTTP_CODE" = "200" ] && echo 1 || echo 0)"

# SELECT all — should see 3 rows
RESP=$(query "SELECT * FROM txn_test")
HTTP_CODE=$(echo "$RESP" | tail -1)
BODY=$(echo "$RESP" | sed '$d')
check "SELECT after INSERT returns 200" "$([ "$HTTP_CODE" = "200" ] && echo 1 || echo 0)"
check "SELECT sees alice" "$(echo "$BODY" | grep -q 'alice' && echo 1 || echo 0)"
check "SELECT sees bob" "$(echo "$BODY" | grep -q 'bob' && echo 1 || echo 0)"
check "SELECT sees carol" "$(echo "$BODY" | grep -q 'carol' && echo 1 || echo 0)"

# DELETE row with id='2' (bob)
RESP=$(query "DELETE FROM txn_test WHERE id = '2'")
HTTP_CODE=$(echo "$RESP" | tail -1)
BODY=$(echo "$RESP" | sed '$d')
check "DELETE WHERE id='2' returns 200" "$([ "$HTTP_CODE" = "200" ] && echo 1 || echo 0)"
check "DELETE response contains deleted count" \
    "$(echo "$BODY" | grep -qE '"deleted"|"rows":\[\["[1-9]' && echo 1 || echo 0)"

# SELECT again — bob should be gone
RESP=$(query "SELECT * FROM txn_test")
HTTP_CODE=$(echo "$RESP" | tail -1)
BODY=$(echo "$RESP" | sed '$d')
check "SELECT after DELETE returns 200" "$([ "$HTTP_CODE" = "200" ] && echo 1 || echo 0)"
check "bob is no longer visible after DELETE" "$(echo "$BODY" | grep -q 'bob' && echo 0 || echo 1)"
check "alice still visible after DELETE" "$(echo "$BODY" | grep -q 'alice' && echo 1 || echo 0)"
check "carol still visible after DELETE" "$(echo "$BODY" | grep -q 'carol' && echo 1 || echo 0)"

# UPDATE alice's score
RESP=$(query "UPDATE txn_test SET score = '999' WHERE id = '1'")
HTTP_CODE=$(echo "$RESP" | tail -1)
check "UPDATE WHERE id='1' returns 200" "$([ "$HTTP_CODE" = "200" ] && echo 1 || echo 0)"

# SELECT — verify alice has new score
RESP=$(query "SELECT * FROM txn_test WHERE id = '1'")
HTTP_CODE=$(echo "$RESP" | tail -1)
BODY=$(echo "$RESP" | sed '$d')
check "SELECT after UPDATE returns 200" "$([ "$HTTP_CODE" = "200" ] && echo 1 || echo 0)"
check "alice's score updated to 999" "$(echo "$BODY" | grep -q '999' && echo 1 || echo 0)"
check "alice's old score 100 is gone" "$(echo "$BODY" | grep -q '"100"' && echo 0 || echo 1)"

# CSV ingest — add rows via ingest endpoint and verify via query
RESP=$(curl -s -w "\n%{http_code}" -X POST "$BASE/api/ingest/csv?table=txn_ingest" \
    -H "Content-Type: text/csv" \
    -H "$AUTH" \
    -d "id,value
a1,v1
a2,v2
a3,v3")
HTTP_CODE=$(echo "$RESP" | tail -1)
check "CSV ingest to txn_ingest returns 200" "$([ "$HTTP_CODE" = "200" ] && echo 1 || echo 0)"

RESP=$(query "SELECT * FROM txn_ingest")
HTTP_CODE=$(echo "$RESP" | tail -1)
BODY=$(echo "$RESP" | sed '$d')
check "SELECT from ingested table returns 200" "$([ "$HTTP_CODE" = "200" ] && echo 1 || echo 0)"
check "ingested rows visible (v1)" "$(echo "$BODY" | grep -q 'v1' && echo 1 || echo 0)"
check "ingested rows visible (v3)" "$(echo "$BODY" | grep -q 'v3' && echo 1 || echo 0)"

# Summary
echo ""
echo "Results: PASS=$PASS FAIL=$FAIL"
[ "$FAIL" -eq 0 ] && exit 0 || exit 1
