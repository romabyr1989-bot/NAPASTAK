#!/usr/bin/env bash
# test_rbac.sh — RBAC policy enforcement integration test
# Port 19201, rbac_enabled=true
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
GW="$PROJECT_ROOT/build/release/bin/napastak_gateway"
PORT=19201
BASE="http://127.0.0.1:$PORT"
DATA_DIR="/tmp/dfo_test_rbac_$$"
CFG_FILE="/tmp/dfo_test_rbac_$$.json"
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

# Write config
mkdir -p "$DATA_DIR"
cat > "$CFG_FILE" <<EOF
{
  "port": $PORT,
  "data_dir": "$DATA_DIR",
  "auth_enabled": true,
  "rbac_enabled": true,
  "cluster_mode": false,
  "jwt_secret": "test-secret-rbac-integration"
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

# Get admin token
RESP=$(curl -s -w "\n%{http_code}" -X POST "$BASE/api/auth/token" \
    -H "Content-Type: application/json" \
    -d '{"username":"admin","password":"admin"}')
HTTP_CODE=$(echo "$RESP" | tail -1)
BODY=$(echo "$RESP" | sed '$d')
TOKEN=$(echo "$BODY" | grep -o '"token":"[^"]*"' | cut -d'"' -f4)
check "admin auth token returned 200" "$([ "$HTTP_CODE" = "200" ] && echo 1 || echo 0)"
check "token is non-empty" "$([ -n "$TOKEN" ] && echo 1 || echo 0)"

AUTH="Authorization: Bearer $TOKEN"

# NAPASTAK doesn't support CREATE TABLE / INSERT INTO via SQL — tables
# are created by the CSV ingest endpoint. Bulk-load 3 rows in one shot.
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X POST \
    "$BASE/api/ingest/csv?table=events" \
    -H "$AUTH" -H 'Content-Type: text/csv' \
    --data-binary $'id,name,val\nid1,event1,1\nid2,event2,2\nid3,event3,3\n')
check "admin ingests events via CSV" "$([ "$HTTP_CODE" = "200" ] && echo 1 || echo 0)"

# SELECT as admin — should succeed (admin is always superuser)
RESP=$(curl -s -w "\n%{http_code}" -X POST "$BASE/api/tables/query" \
    -H "Content-Type: application/json" \
    -H "$AUTH" \
    -d '{"sql":"SELECT * FROM events"}')
HTTP_CODE=$(echo "$RESP" | tail -1)
BODY=$(echo "$RESP" | sed '$d')
check "admin SELECT from events returns 200" "$([ "$HTTP_CODE" = "200" ] && echo 1 || echo 0)"
check "SELECT result contains event data" "$(echo "$BODY" | grep -q 'event1' && echo 1 || echo 0)"

# List RBAC policies — admin should see them
RESP=$(curl -s -w "\n%{http_code}" -X GET "$BASE/api/rbac/policies" \
    -H "$AUTH")
HTTP_CODE=$(echo "$RESP" | tail -1)
BODY=$(echo "$RESP" | sed '$d')
check "GET /api/rbac/policies returns 200" "$([ "$HTTP_CODE" = "200" ] && echo 1 || echo 0)"

# Create events2 table + seed via CSV ingest (admin = superuser)
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X POST \
    "$BASE/api/ingest/csv?table=events2" \
    -H "$AUTH" -H 'Content-Type: text/csv' \
    --data-binary $'id,payload\nx1,payload_a\n')
check "admin ingests events2 via CSV (superuser)" \
    "$([ "$HTTP_CODE" = "200" ] && echo 1 || echo 0)"

# Add RBAC policy: ROLE_ANALYST(1) WRITE(2) on events2 — deny by omitting it
# Instead: set viewer role (2) to READ-only on events (ACTION_TABLE_READ=1)
# ROLE_VIEWER=2, ACTION_TABLE_READ=1, table_pattern=events
RESP=$(curl -s -w "\n%{http_code}" -X POST "$BASE/api/rbac/policies" \
    -H "Content-Type: application/json" \
    -H "$AUTH" \
    -d '{"role":2,"table_pattern":"events","allowed_actions":1,"row_filter":""}')
HTTP_CODE=$(echo "$RESP" | tail -1)
BODY=$(echo "$RESP" | sed '$d')
check "POST /api/rbac/policies for viewer READ on events returns 200" \
    "$([ "$HTTP_CODE" = "200" ] && echo 1 || echo 0)"
check "policy set response ok=true" "$(echo "$BODY" | grep -q '"ok":true' && echo 1 || echo 0)"

# Get viewer token via API key
# Create API key for a viewer-role user
RESP=$(curl -s -w "\n%{http_code}" -X POST "$BASE/api/auth/apikeys" \
    -H "Content-Type: application/json" \
    -H "$AUTH" \
    -d '{"user_id":"viewer1","role":"viewer"}')
HTTP_CODE=$(echo "$RESP" | tail -1)
BODY=$(echo "$RESP" | sed '$d')
check "create viewer API key returns 200" "$([ "$HTTP_CODE" = "200" ] && echo 1 || echo 0)"
APIKEY=$(echo "$BODY" | grep -o '"key":"[^"]*"' | cut -d'"' -f4)
check "API key is non-empty" "$([ -n "$APIKEY" ] && echo 1 || echo 0)"

# Viewer SELECT on events — should succeed (policy allows READ)
RESP=$(curl -s -w "\n%{http_code}" -X POST "$BASE/api/tables/query" \
    -H "Content-Type: application/json" \
    -H "Authorization: Bearer $APIKEY" \
    -d '{"sql":"SELECT * FROM events"}')
HTTP_CODE=$(echo "$RESP" | tail -1)
check "viewer SELECT on events succeeds (READ allowed)" \
    "$([ "$HTTP_CODE" = "200" ] && echo 1 || echo 0)"

# Viewer DELETE on events2 — must fail (no WRITE policy for viewer on events2).
# Using DELETE rather than INSERT because the SQL parser supports DELETE and
# RBAC fires AFTER sql_parse — INSERT VALUES would 400 before we ever check
# permissions, masking the real RBAC check we want to assert here.
RESP=$(curl -s -w "\n%{http_code}" -X POST "$BASE/api/tables/query" \
    -H "Content-Type: application/json" \
    -H "Authorization: Bearer $APIKEY" \
    -d '{"sql":"DELETE FROM events2 WHERE id = '"'"'x1'"'"'"}')
HTTP_CODE=$(echo "$RESP" | tail -1)
check "viewer DELETE on events2 blocked (no WRITE policy)" \
    "$([ "$HTTP_CODE" = "403" ] && echo 1 || echo 0)"

# Unauthorized request — no token
RESP=$(curl -s -w "\n%{http_code}" -X POST "$BASE/api/tables/query" \
    -H "Content-Type: application/json" \
    -d '{"sql":"SELECT * FROM events"}')
HTTP_CODE=$(echo "$RESP" | tail -1)
check "unauthenticated query returns 401" "$([ "$HTTP_CODE" = "401" ] && echo 1 || echo 0)"

# Summary
echo ""
echo "Results: PASS=$PASS FAIL=$FAIL"
[ "$FAIL" -eq 0 ] && exit 0 || exit 1
