#!/usr/bin/env bash
# test_replication.sh — Cluster / replication smoke test
# storage_node on port 19290, gateway on port 19204 with cluster_mode=true
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
GW="$PROJECT_ROOT/build/release/bin/dfo_gateway"
STORAGE="$PROJECT_ROOT/build/release/bin/dfo_storage"
GW_PORT=19204
ST_PORT=19290
BASE="http://127.0.0.1:$GW_PORT"
DATA_DIR="/tmp/dfo_test_repl_$$"
ST_DATA_DIR="/tmp/dfo_test_repl_st_$$"
CFG_FILE="/tmp/dfo_test_repl_$$.json"
PASS=0
FAIL=0
GW_PID=""
ST_PID=""

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
    if [ -n "$GW_PID" ] && kill -0 "$GW_PID" 2>/dev/null; then
        kill "$GW_PID" 2>/dev/null || true
        wait "$GW_PID" 2>/dev/null || true
    fi
    if [ -n "$ST_PID" ] && kill -0 "$ST_PID" 2>/dev/null; then
        kill "$ST_PID" 2>/dev/null || true
        wait "$ST_PID" 2>/dev/null || true
    fi
    rm -rf "$DATA_DIR" "$ST_DATA_DIR" "$CFG_FILE"
}
trap cleanup EXIT

mkdir -p "$DATA_DIR" "$ST_DATA_DIR"

# Start storage node (if binary exists)
STORAGE_RUNNING=0
if [ -x "$STORAGE" ]; then
    "$STORAGE" -p "$ST_PORT" -d "$ST_DATA_DIR" >"$ST_DATA_DIR/st.log" 2>&1 &
    ST_PID=$!
    # Wait up to 3s for storage node. dfo_storage speaks a binary cluster
    # protocol (proto_recv/proto_send), NOT HTTP — there is no /health
    # endpoint, so liveness == the listen socket accepting a TCP connect.
    for i in $(seq 1 15); do
        if (exec 3<>"/dev/tcp/127.0.0.1/$ST_PORT") 2>/dev/null; then
            exec 3>&- 3<&-
            STORAGE_RUNNING=1
            break
        fi
        sleep 0.2
    done
    check "storage node started" "$([ "$STORAGE_RUNNING" = "1" ] && echo 1 || echo 0)"
else
    echo "SKIP: storage binary not found at $STORAGE — skipping storage node start"
    check "storage binary exists" "0"
fi

# Gateway config with cluster_mode=true
cat > "$CFG_FILE" <<EOF
{
  "port": $GW_PORT,
  "data_dir": "$DATA_DIR",
  "auth_enabled": true,
  "rbac_enabled": false,
  "cluster_mode": true,
  "jwt_secret": "test-secret-repl-integration"
}
EOF

# Start gateway
"$GW" -c "$CFG_FILE" >"$DATA_DIR/gw.log" 2>&1 &
GW_PID=$!

# Wait for gateway ready
for i in $(seq 1 30); do
    STATUS=$(curl -s -o /dev/null -w "%{http_code}" "$BASE/health" 2>/dev/null || true)
    [ "$STATUS" = "200" ] && break
    sleep 0.3
done
STATUS=$(curl -s -o /dev/null -w "%{http_code}" "$BASE/health" 2>/dev/null || true)
check "gateway started in cluster_mode and healthy" "$([ "$STATUS" = "200" ] && echo 1 || echo 0)"

# Auth
RESP=$(curl -s -w "\n%{http_code}" -X POST "$BASE/api/auth/token" \
    -H "Content-Type: application/json" \
    -d '{"username":"admin","password":"admin"}')
HTTP_CODE=$(echo "$RESP" | tail -1)
BODY=$(echo "$RESP" | sed '$d')
TOKEN=$(echo "$BODY" | grep -o '"token":"[^"]*"' | cut -d'"' -f4)
check "admin auth in cluster mode returns 200" "$([ "$HTTP_CODE" = "200" ] && echo 1 || echo 0)"
AUTH="Authorization: Bearer $TOKEN"

# Create the table via CSV ingest. NAPASTAK does not support SQL CREATE TABLE /
# INSERT ... VALUES via /api/tables/query — tables come alive through the CSV
# ingest endpoint (same convention as test_rbac.sh / test_txn.sh). Seed one row
# so the (id,payload) schema is inferred.
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X POST \
    "$BASE/api/ingest/csv?table=repl_test" \
    -H "$AUTH" -H 'Content-Type: text/csv' \
    --data-binary $'id,payload\nr0,data_0\n')
check "CREATE TABLE repl_test via gateway (cluster mode)" \
    "$([ "$HTTP_CODE" = "200" ] && echo 1 || echo 0)"

# Append rows via CSV ingest (INSERT ... VALUES is likewise unsupported by the
# gateway SQL engine). Each ingest re-sends the header plus one data row; the
# endpoint appends to the existing table.
for i in 1 2 3; do
    HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X POST \
        "$BASE/api/ingest/csv?table=repl_test" \
        -H "$AUTH" -H 'Content-Type: text/csv' \
        --data-binary "$(printf 'id,payload\nr%d,data_%d\n' "$i" "$i")")
    check "INSERT row $i into repl_test" "$([ "$HTTP_CODE" = "200" ] && echo 1 || echo 0)"
done

# Wait for potential WAL replication flush
sleep 1

# Verify gateway still responds (no crash after replication activity)
STATUS=$(curl -s -o /dev/null -w "%{http_code}" "$BASE/health" 2>/dev/null || true)
check "gateway still healthy after INSERT + 1s wait" \
    "$([ "$STATUS" = "200" ] && echo 1 || echo 0)"

# SELECT the rows via gateway — they should still be readable
RESP=$(curl -s -w "\n%{http_code}" -X POST "$BASE/api/tables/query" \
    -H "Content-Type: application/json" \
    -H "$AUTH" \
    -d '{"sql":"SELECT * FROM repl_test"}')
HTTP_CODE=$(echo "$RESP" | tail -1)
BODY=$(echo "$RESP" | sed '$d')
check "SELECT from repl_test returns 200" "$([ "$HTTP_CODE" = "200" ] && echo 1 || echo 0)"
check "data_1 visible in repl_test" "$(echo "$BODY" | grep -q 'data_1' && echo 1 || echo 0)"
check "data_3 visible in repl_test" "$(echo "$BODY" | grep -q 'data_3' && echo 1 || echo 0)"

# Check cluster status endpoint
RESP=$(curl -s -w "\n%{http_code}" -X GET "$BASE/api/cluster/status" \
    -H "$AUTH")
HTTP_CODE=$(echo "$RESP" | tail -1)
check "GET /api/cluster/status returns 200" "$([ "$HTTP_CODE" = "200" ] && echo 1 || echo 0)"

# Verify process is still alive
check "gateway process still running after replication test" \
    "$(kill -0 "$GW_PID" 2>/dev/null && echo 1 || echo 0)"

# Summary
echo ""
echo "Results: PASS=$PASS FAIL=$FAIL"
[ "$FAIL" -eq 0 ] && exit 0 || exit 1
