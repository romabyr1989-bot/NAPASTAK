#!/usr/bin/env bash
# Integration test for the PostgreSQL wire-protocol server (Step 3 Week 1).
#
# Uses the system `psql` client. Skips if it isn't installed.
# Covers: handshake, cleartext auth (good/bad/missing), Week 1 query subset.

set -u
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
GW="$ROOT/build/release/bin/dfo_gateway"
HTTP_PORT=18180
PG_PORT=15532
DATA="$(mktemp -d -t dfo_pgw_test_XXXX)"
SECRET="pgw-test-$$"
PASS=0; FAIL=0; SKIP=0

cleanup() { [[ -n "${GW_PID:-}" ]] && kill "$GW_PID" 2>/dev/null; rm -rf "$DATA"; }
trap cleanup EXIT
check() { if eval "$2"; then echo "PASS: $1"; PASS=$((PASS+1)); else echo "FAIL: $1   ($2)"; FAIL=$((FAIL+1)); fi; }
skip()  { echo "SKIP: $1 — $2"; SKIP=$((SKIP+1)); }

[[ -x "$GW" ]] || { echo "missing $GW; run: make BUILD=release"; exit 1; }
command -v psql >/dev/null 2>&1 || { skip "all checks" "psql not installed"; echo "Pgwire test: $PASS passed, $FAIL failed, $SKIP skipped"; exit 0; }

# Spin up gateway with pgwire enabled
cat > "$DATA/cfg.json" <<EOF
{"port":$HTTP_PORT,"data_dir":"$DATA","auth_enabled":true,
 "jwt_secret":"$SECRET","admin_password":"admin",
 "pgwire_enabled":1,"pgwire_port":$PG_PORT}
EOF
"$GW" -c "$DATA/cfg.json" > "$DATA/gw.log" 2>&1 &
GW_PID=$!
for i in {1..30}; do
  curl -sf "http://localhost:$HTTP_PORT/health" >/dev/null 2>&1 && break
  sleep 0.2
done

PSQL="psql -h localhost -p $PG_PORT -U admin -d dataflow -tAc"

# 1. Handshake + auth + SELECT 1
RESP=$(PGPASSWORD=admin $PSQL "SELECT 1" 2>&1)
check "SELECT 1 returns 1"                 "[[ '$RESP' == '1' ]]"

# 2. version()
RESP=$(PGPASSWORD=admin $PSQL "SELECT version()" 2>&1)
check "version() mentions NAPASTAK"     "[[ '$RESP' == *NAPASTAK* ]]"

# 3. current_database / current_user
RESP=$(PGPASSWORD=admin $PSQL "SELECT current_database()" 2>&1)
check "current_database() returns dataflow" "[[ '$RESP' == 'dataflow' ]]"
RESP=$(PGPASSWORD=admin $PSQL "SELECT current_user" 2>&1)
check "current_user returns admin"         "[[ '$RESP' == 'admin' ]]"

# 4. SHOW server_version
RESP=$(PGPASSWORD=admin $PSQL "SHOW server_version" 2>&1)
check "SHOW server_version works"          "[[ '$RESP' == *NAPASTAK* ]]"

# 5. Multiple constants
RESP=$(PGPASSWORD=admin $PSQL "SELECT 42" 2>&1)
check "SELECT 42 returns 42"               "[[ '$RESP' == '42' ]]"

# 6. BEGIN/SET/COMMIT no-op
RESP=$(PGPASSWORD=admin psql -h localhost -p $PG_PORT -U admin -d dataflow -tA <<'EOF' 2>&1
BEGIN;
SET TimeZone='UTC';
SELECT 7;
COMMIT;
EOF
)
check "BEGIN/SET/COMMIT around SELECT works" "echo '$RESP' | grep -q '^7\$'"

# 7. Bad password rejected with 28P01
RESP=$(PGPASSWORD=wrong $PSQL "SELECT 1" 2>&1)
check "bad password rejected (28P01-style err)" "[[ '$RESP' == *password* ]]"

# 8. Week 2: real SQL via qengine — table operations
PGPASSWORD=admin curl -sf -X POST "http://localhost:$HTTP_PORT/api/ingest/csv?table=pgw_users" \
  -H 'Content-Type: text/csv' \
  -H "Authorization: Bearer $(curl -sf -X POST "http://localhost:$HTTP_PORT/api/auth/token" \
        -H 'Content-Type: application/json' \
        -d '{"username":"admin","password":"admin"}' \
      | python3 -c 'import sys,json;print(json.load(sys.stdin)["token"])')" \
  --data-binary $'id,name,age\n1,alice,30\n2,bob,25\n3,carol,42\n' > /dev/null

RESP=$(PGPASSWORD=admin $PSQL "SELECT * FROM pgw_users ORDER BY id" 2>&1)
check "Week 2: SELECT returns 3 rows"      "[[ \$(echo '$RESP' | wc -l) -eq 3 ]]"

RESP=$(PGPASSWORD=admin $PSQL "SELECT name FROM pgw_users WHERE age > 28 ORDER BY name" 2>&1)
check "Week 2: WHERE filter works"         "[[ '$RESP' == *alice*carol* ]]"

RESP=$(PGPASSWORD=admin $PSQL "SELECT COUNT(*) FROM pgw_users" 2>&1)
check "Week 2: aggregate COUNT(*)"         "[[ '$RESP' == '3' ]]"

RESP=$(PGPASSWORD=admin $PSQL "SELEKT 1" 2>&1)
check "Week 2: parser error → ERROR response" "[[ '$RESP' == *ERROR* ]]"

# Week 3 — pg_catalog / information_schema emulation
RESP=$(PGPASSWORD=admin $PSQL "SELECT table_schema, table_name FROM information_schema.tables" 2>&1)
check "Week 3: information_schema.tables shows pgw_users" "[[ '$RESP' == *pgw_users* ]]"
check "Week 3: information_schema.tables uses public schema" "[[ '$RESP' == *public* ]]"

RESP=$(PGPASSWORD=admin $PSQL "SELECT column_name, data_type FROM information_schema.columns" 2>&1)
check "Week 3: information_schema.columns includes 'name'" "[[ '$RESP' == *name* ]]"
check "Week 3: information_schema.columns includes 'age'"  "[[ '$RESP' == *age* ]]"

RESP=$(PGPASSWORD=admin $PSQL "SELECT * FROM pg_catalog.pg_namespace" 2>&1)
check "Week 3: pg_namespace exposes 'public'"     "[[ '$RESP' == *public* ]]"
check "Week 3: pg_namespace exposes 'pg_catalog'" "[[ '$RESP' == *pg_catalog* ]]"

RESP=$(PGPASSWORD=admin $PSQL "SELECT relname FROM pg_class" 2>&1)
check "Week 3: pg_class lists user tables"        "[[ '$RESP' == *pgw_users* ]]"

# Week 3 — type inference: int column should round-trip cleanly through
# psql with right-alignment / no quoting (psql aligns numerics right).
RESP=$(PGPASSWORD=admin $PSQL "SELECT id FROM pgw_users ORDER BY id LIMIT 1" 2>&1)
check "Week 3: typed int column returns scalar 1" "[[ '$RESP' == '1' ]]"

# 9. SSL request handled (psql probes SSL on connect by default — already
#    exercised above but verify by forcing sslmode=disable still works)
RESP=$(PGPASSWORD=admin psql "host=localhost port=$PG_PORT user=admin dbname=dataflow sslmode=disable" -tAc "SELECT 1" 2>&1)
check "sslmode=disable still works"        "[[ '$RESP' == '1' ]]"

# 10. Gateway log shows the listener and the connection event
check "gateway log: pgwire listening line"     "grep -q 'pgwire: listening on' '$DATA/gw.log'"
check "gateway log: at least one auth ok"      "grep -q 'pgwire: auth ok' '$DATA/gw.log'"

echo ""
echo "Pgwire test: $PASS passed, $FAIL failed, $SKIP skipped"
[[ $FAIL -eq 0 ]]
