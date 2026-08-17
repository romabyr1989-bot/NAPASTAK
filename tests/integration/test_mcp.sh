#!/usr/bin/env bash
# Smoke test for napastak_mcp_server.
# Spawns gateway on a fresh data dir, gets a JWT, then drives the MCP server
# over stdio: initialize → tools/list → tools/call (query, list_tables, metrics).

set -u
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
GW_BIN="$ROOT/build/release/bin/napastak_gateway"
MCP_BIN="$ROOT/build/release/bin/napastak_mcp_server"
PORT=19250
DATA="$(mktemp -d -t dfo_mcp_test_XXXX)"
SECRET="mcp-test-secret-$$"
PASS=0; FAIL=0

cleanup() {
  [[ -n "${GW_PID:-}" ]] && kill "$GW_PID" 2>/dev/null
  rm -rf "$DATA"
}
trap cleanup EXIT

check() {
  local name="$1" cmd="$2"
  if eval "$cmd"; then
    echo "PASS: $name"; PASS=$((PASS+1))
  else
    echo "FAIL: $name"; FAIL=$((FAIL+1))
  fi
}

# 0. Pre-flight
[[ -x "$GW_BIN"  ]] || { echo "missing $GW_BIN. Run: make BUILD=release"; exit 1; }
[[ -x "$MCP_BIN" ]] || { echo "missing $MCP_BIN. Run: make BUILD=release"; exit 1; }

# 1. Start gateway
cat > "$DATA/cfg.json" <<EOF
{"port":$PORT,"data_dir":"$DATA","auth_enabled":true,
 "jwt_secret":"$SECRET","admin_password":"admin"}
EOF
"$GW_BIN" -c "$DATA/cfg.json" > "$DATA/gw.log" 2>&1 &
GW_PID=$!
for i in {1..20}; do
  curl -sf "http://localhost:$PORT/health" >/dev/null 2>&1 && break
  sleep 0.2
done
TOKEN=$(curl -sf -X POST "http://localhost:$PORT/api/auth/token" \
  -H 'Content-Type: application/json' \
  -d '{"username":"admin","password":"admin"}' | python3 -c "import sys,json;print(json.load(sys.stdin)['token'])")
[[ -n "$TOKEN" ]] || { echo "FAIL: no token"; exit 1; }
echo "Gateway ready on :$PORT"

run_mcp() {
  echo "$1" | "$MCP_BIN" --gateway "http://localhost:$PORT" --api-key "$TOKEN" 2>/dev/null
}

# 2. initialize handshake
RESP=$(run_mcp '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"test","version":"1.0"}}}')
check "initialize returns protocolVersion" \
  '[[ "$RESP" == *"protocolVersion"*"2024-11-05"* ]]'
check "initialize returns serverInfo" \
  '[[ "$RESP" == *"napastak"* ]]'

# 3. tools/list returns all 10 tools
RESP=$(run_mcp '{"jsonrpc":"2.0","id":2,"method":"tools/list"}')
for tool in query list_tables describe_table ingest_csv ingest_url \
            list_pipelines run_pipeline create_pipeline get_metrics analyze; do
  check "tools/list contains $tool" '[[ "$RESP" == *"\"name\":\"'"$tool"'\""* ]]'
done

# 4. tools/call query → SELECT 1
RESP=$(run_mcp '{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"query","arguments":{"sql":"SELECT 1 AS n"}}}')
check "query returns 'isError':false" '[[ "$RESP" == *"\"isError\":false"* ]]'
check "query returns markdown table"   '[[ "$RESP" == *"| n |"* || "$RESP" == *"| 1 |"* ]]'

# 5. tools/call get_metrics
RESP=$(run_mcp '{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"get_metrics","arguments":{}}}')
check "get_metrics returns Uptime"     '[[ "$RESP" == *"Uptime"* ]]'

# 6. Unknown tool → isError:true
RESP=$(run_mcp '{"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"no_such_tool"}}')
check "unknown tool returns isError:true" '[[ "$RESP" == *"\"isError\":true"* ]]'

# 7. ping
RESP=$(run_mcp '{"jsonrpc":"2.0","id":6,"method":"ping"}')
check "ping returns empty result"      '[[ "$RESP" == *"\"result\":{}"* ]]'

# 8. Bad JSON → -32700 Parse error
RESP=$(run_mcp 'not json {{{')
check "bad JSON → -32700 Parse error"  '[[ "$RESP" == *"-32700"* && "$RESP" == *"Parse error"* ]]'

echo ""
echo "MCP smoke test: $PASS passed, $FAIL failed"
[[ $FAIL -eq 0 ]]
