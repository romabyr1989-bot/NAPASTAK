#!/usr/bin/env bash
# Integration test for the PostgreSQL connector's incremental ("light CDC")
# read mode — DfoReadReq.cursor used as a high-watermark on cursor_column.
#
# Spins up a throwaway local PostgreSQL, compiles a tiny C harness that loads
# the connector and performs ONE read_batch for a given input cursor, then
# drives two sequential reads:
#   read #1 (empty cursor)  → all rows, cursor advances to max(cursor_column)
#   read #2 (cursor from #1) → ONLY rows newer than the bookmark
#
# SKIPs cleanly when the PostgreSQL tools or libpq aren't available.

set -u
# Force the C locale: PostgreSQL 16 on macOS aborts with "postmaster became
# multithreaded during startup" when locale resolution pulls in CoreFoundation
# (which spawns a thread). The C locale avoids that machinery.
export LC_ALL=C LANG=C
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
DATA="$(mktemp -d -t dfo_pgcdc_XXXX)"
PGDATA="$DATA/pgdata"
PGPORT=54329
PASS=0; FAIL=0
USER_NAME="$(whoami)"

cleanup() {
  [[ -d "$PGDATA" ]] && pg_ctl -D "$PGDATA" -m immediate stop >/dev/null 2>&1
  rm -rf "$DATA"
}
trap cleanup EXIT
check() { if eval "$2"; then echo "PASS: $1"; PASS=$((PASS+1)); else echo "FAIL: $1   ($2)"; FAIL=$((FAIL+1)); fi; }

# ── Prerequisites ────────────────────────────────────────────────────────────
for bin in initdb pg_ctl psql; do
  command -v "$bin" >/dev/null 2>&1 || { echo "SKIP: $bin not found — PostgreSQL not installed"; exit 0; }
done
if command -v pg_config >/dev/null 2>&1; then
  PQINC="$(pg_config --includedir)"; PQLIB="$(pg_config --libdir)"
else
  PQINC="/usr/local/opt/libpq/include"; PQLIB="/usr/local/opt/libpq/lib"
fi
[[ -f "$PQINC/libpq-fe.h" ]] || { echo "SKIP: libpq-fe.h not found — cannot build harness"; exit 0; }

# ── Harness: load connector, do one read_batch, print ROWS / CURSOR ──────────
cat > "$DATA/harness.c" <<'CEOF'
#include "connector.h"
#include "log.h"
#include "arena.h"
#include <stdio.h>
extern const DfoConnector dfo_connector_entry;
int main(int argc, char **argv) {           /* argv: cfg_json cursor entity */
    if (argc < 4) { fprintf(stderr, "usage: cfg cursor entity\n"); return 2; }
    log_init(&g_log, stderr, LOG_WARN, 0);
    Arena *a = arena_create(1 << 20);
    void *ctx = dfo_connector_entry.create(argv[1], a);
    if (!ctx || dfo_connector_entry.ping(ctx) != 0) { fprintf(stderr, "no PG\n"); return 3; }
    DfoReadReq req = { .cursor = argv[2], .limit = 100, .filter = NULL };
    ColBatch *batch = NULL;
    if (dfo_connector_entry.read_batch(ctx, a, &req, argv[3], &batch) != 0 || !batch) {
        fprintf(stderr, "read_batch failed\n"); return 4;
    }
    printf("ROWS=%d\n", batch->nrows);
    printf("CURSOR=%s\n", req.cursor ? req.cursor : "");
    dfo_connector_entry.destroy(ctx);
    return 0;
}
CEOF

if ! gcc -std=c11 -D_GNU_SOURCE \
     -I"$ROOT/lib" -I"$ROOT/lib/core" -I"$ROOT/lib/storage" -I"$ROOT/lib/connector" \
     -I"$PQINC" \
     "$DATA/harness.c" \
     "$ROOT/lib/connector/plugins/pg/pg_connector.c" \
     "$ROOT/lib/core/arena.c" "$ROOT/lib/core/log.c" \
     -L"$PQLIB" -lpq -lpthread -o "$DATA/harness" 2>"$DATA/cc.log"; then
  echo "SKIP: harness build failed:"; cat "$DATA/cc.log"; exit 0
fi

# ── Throwaway PostgreSQL instance ────────────────────────────────────────────
initdb -D "$PGDATA" -A trust -U "$USER_NAME" --locale=C --encoding=UTF8 >/dev/null 2>&1 \
  || { echo "SKIP: initdb failed"; exit 0; }
pg_ctl -D "$PGDATA" -l "$DATA/pg.log" -w \
  -o "-p $PGPORT -c listen_addresses=127.0.0.1 -k $DATA" start >/dev/null 2>&1 \
  || { echo "SKIP: postgres failed to start"; cat "$DATA/pg.log" 2>/dev/null; exit 0; }

PSQL=(psql -h 127.0.0.1 -p "$PGPORT" -U "$USER_NAME" -d postgres -v ON_ERROR_STOP=1 -q)
"${PSQL[@]}" -c "CREATE TABLE cdc_t(seq int PRIMARY KEY, name text);" >/dev/null
"${PSQL[@]}" -c "INSERT INTO cdc_t VALUES (1,'a'),(2,'b'),(3,'c');" >/dev/null

CFG="{\"host\":\"127.0.0.1\",\"port\":\"$PGPORT\",\"dbname\":\"postgres\",\"user\":\"$USER_NAME\",\"cursor_column\":\"seq\"}"

# ── Read #1: empty cursor → all 3 rows, cursor advances to 3 ─────────────────
OUT1="$("$DATA/harness" "$CFG" "" "cdc_t")"
ROWS1=$(sed -n 's/^ROWS=//p'   <<<"$OUT1")
CUR1=$(sed  -n 's/^CURSOR=//p' <<<"$OUT1")
check "read #1 returns all 3 initial rows"      "[[ '$ROWS1' == 3 ]]"
check "read #1 advances cursor to max seq (3)"  "[[ '$CUR1'  == 3 ]]"

# ── Insert 2 new rows, then Read #2 with the bookmark from #1 ────────────────
"${PSQL[@]}" -c "INSERT INTO cdc_t VALUES (4,'d'),(5,'e');" >/dev/null
OUT2="$("$DATA/harness" "$CFG" "$CUR1" "cdc_t")"
ROWS2=$(sed -n 's/^ROWS=//p'   <<<"$OUT2")
CUR2=$(sed  -n 's/^CURSOR=//p' <<<"$OUT2")
check "read #2 returns ONLY the 2 new rows"     "[[ '$ROWS2' == 2 ]]"
check "read #2 advances cursor to max seq (5)"  "[[ '$CUR2'  == 5 ]]"

# ── Read #3 with the latest cursor → nothing new, cursor unchanged ──────────
OUT3="$("$DATA/harness" "$CFG" "$CUR2" "cdc_t")"
ROWS3=$(sed -n 's/^ROWS=//p' <<<"$OUT3")
check "read #3 with up-to-date cursor returns 0 rows" "[[ '$ROWS3' == 0 ]]"

echo ""
echo "pg-cdc test: $PASS passed, $FAIL failed"
[[ $FAIL -eq 0 ]]
