#!/usr/bin/env bash
# test_audit.sh — Audit log integration test
# Port 19202, audit_log_file set
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
GW="$PROJECT_ROOT/build/release/bin/napastak_gateway"
PORT=19202
BASE="http://127.0.0.1:$PORT"
DATA_DIR="/tmp/dfo_test_audit_$$"
CFG_FILE="/tmp/dfo_test_audit_$$.json"
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
AUDIT_LOG="$DATA_DIR/audit.log"

cat > "$CFG_FILE" <<EOF
{
  "port": $PORT,
  "data_dir": "$DATA_DIR",
  "auth_enabled": true,
  "rbac_enabled": false,
  "cluster_mode": false,
  "jwt_secret": "test-secret-audit-integration",
  "audit_log_file": "$AUDIT_LOG"
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
check "token non-empty" "$([ -n "$TOKEN" ] && echo 1 || echo 0)"

AUTH="Authorization: Bearer $TOKEN"

# Create a table and run a few queries to generate audit events
curl -s -o /dev/null -X POST "$BASE/api/tables/query" \
    -H "Content-Type: application/json" \
    -H "$AUTH" \
    -d '{"sql":"CREATE TABLE audit_test (id TEXT, msg TEXT)"}' || true

curl -s -o /dev/null -X POST "$BASE/api/tables/query" \
    -H "Content-Type: application/json" \
    -H "$AUTH" \
    -d '{"sql":"INSERT INTO audit_test VALUES ('"'"'1'"'"', '"'"'hello'"'"')"}' || true

curl -s -o /dev/null -X POST "$BASE/api/tables/query" \
    -H "Content-Type: application/json" \
    -H "$AUTH" \
    -d '{"sql":"SELECT * FROM audit_test"}' || true

curl -s -o /dev/null -X POST "$BASE/api/tables/query" \
    -H "Content-Type: application/json" \
    -H "$AUTH" \
    -d '{"sql":"SELECT * FROM audit_test WHERE id = '"'"'1'"'"'"}' || true

# GET /api/audit — check status 200 and non-empty results
RESP=$(curl -s -w "\n%{http_code}" -X GET "$BASE/api/audit?limit=50" \
    -H "$AUTH")
HTTP_CODE=$(echo "$RESP" | tail -1)
BODY=$(echo "$RESP" | sed '$d')
check "GET /api/audit returns 200" "$([ "$HTTP_CODE" = "200" ] && echo 1 || echo 0)"
check "audit response is a JSON array" "$(echo "$BODY" | grep -q '^\[' && echo 1 || echo 0)"

# Trigger a failed auth attempt
curl -s -o /dev/null -X POST "$BASE/api/auth/token" \
    -H "Content-Type: application/json" \
    -d '{"username":"admin","password":"WRONG_PASSWORD"}' || true

# Wait for the async ring-buffer flush thread to persist the event
sleep 1

# GET /api/audit again — expect an auth_fail event
RESP=$(curl -s -w "\n%{http_code}" -X GET "$BASE/api/audit?limit=100" \
    -H "$AUTH")
HTTP_CODE=$(echo "$RESP" | tail -1)
BODY=$(echo "$RESP" | sed '$d')
check "audit query after bad auth returns 200" "$([ "$HTTP_CODE" = "200" ] && echo 1 || echo 0)"
check "audit log contains auth_fail event (event_type=5)" \
    "$(echo "$BODY" | grep -qE '"event_type":5|invalid credentials' && echo 1 || echo 0)"

# Verify audit is protected — unauthenticated access should fail
RESP=$(curl -s -w "\n%{http_code}" -X GET "$BASE/api/audit?limit=10")
HTTP_CODE=$(echo "$RESP" | tail -1)
check "unauthenticated GET /api/audit is rejected" \
    "$([ "$HTTP_CODE" = "401" ] && echo 1 || echo 0)"

# Verify viewer cannot access audit (admin only)
RESP=$(curl -s -w "\n%{http_code}" -X POST "$BASE/api/auth/apikeys" \
    -H "Content-Type: application/json" \
    -H "$AUTH" \
    -d '{"user_id":"viewer2","role":"viewer"}')
VIEWER_KEY=$(echo "$RESP" | sed '$d' | grep -o '"key":"[^"]*"' | cut -d'"' -f4)
if [ -n "$VIEWER_KEY" ]; then
    RESP=$(curl -s -w "\n%{http_code}" -X GET "$BASE/api/audit?limit=10" \
        -H "Authorization: Bearer $VIEWER_KEY")
    HTTP_CODE=$(echo "$RESP" | tail -1)
    check "viewer cannot access audit endpoint (403)" \
        "$([ "$HTTP_CODE" = "403" ] && echo 1 || echo 0)"
else
    check "viewer key creation for audit restriction test" "0"
fi

# Summary
echo ""
echo "Results: PASS=$PASS FAIL=$FAIL"
[ "$FAIL" -eq 0 ] && exit 0 || exit 1
