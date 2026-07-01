/* pgwire.h — minimal PostgreSQL wire-protocol v3 server.
 *
 * Implements just enough of https://www.postgresql.org/docs/current/protocol.html
 * to let standard PostgreSQL clients (psql, psycopg2, JDBC, ODBC, dbt, BI
 * tools) connect to NAPASTAK. The gateway runs the server in a
 * dedicated thread; per-connection sockets get their own threads.
 *
 * What's implemented in Week 1:
 *   • TCP listener on configurable port
 *   • SSLRequest gracefully rejected (clients fall back to cleartext)
 *   • StartupMessage parsed (user/database picked up)
 *   • AuthenticationCleartextPassword + ErrorResponse on bad creds
 *   • Simple Query protocol: Query (Q) → RowDescription (T) →
 *     DataRow (D)* → CommandComplete (C) → ReadyForQuery (Z)
 *   • Termination (X) and clean shutdown
 *
 * Week 4 added: Extended Query (Parse/Bind/Describe/Execute/Close/Sync/
 * Flush). Internally it lowers to Simple Query: $1..$N placeholders are
 * substituted with bound parameter values (text format only) and the
 * resulting SQL goes to the same `query` callback as Simple Query.
 *
 * What's still NOT implemented (tracked in docs/POSTGRES_WIRE.md):
 *   • SCRAM-SHA-256 / md5 auth (cleartext is fine over loopback / VPN)
 *   • SSL/TLS termination
 *   • COPY, NOTIFY/LISTEN, two-phase commit
 *   • Binary parameter format codes (text format only)
 *
 * Design: the wire-protocol layer here is decoupled from SQL execution.
 * Callers register a `query` callback that does whatever they want with
 * the SQL text and emits results via the helpers exported here. */
#pragma once
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

/* PostgreSQL OID constants for the column types we map onto */
#define PG_OID_BOOL      16
#define PG_OID_INT8      20
#define PG_OID_INT4      23
#define PG_OID_FLOAT8    701
#define PG_OID_TEXT      25
#define PG_OID_VARCHAR   1043
#define PG_OID_TIMESTAMP 1114

/* Непрозрачные дескрипторы: соединение и сам сервер.
 * Внутреннее устройство скрыто от вызывающего кода. */
typedef struct PgConn       PgConn;
typedef struct PgWireServer PgWireServer;

/* Описание одной колонки результата (для RowDescription). */
typedef struct {
    const char *name;
    int32_t     type_oid;
    int16_t     type_size;       /* -1 for variable-length */
} PgColumn;

typedef struct {
    /* Verify (user, password). Return 0 on success, -1 on reject.
     * Cleartext is used over the wire; future work upgrades to SCRAM. */
    int  (*authenticate)(const char *user, const char *password,
                          const char *database, void *ud);

    /* Execute SQL on behalf of an authenticated client.
     * Implementation MUST emit results via:
     *   pgwire_send_row_description(conn, …)   — once
     *   pgwire_send_data_row(conn, …)          — per row
     *   pgwire_send_command_complete(conn, …)  — when done
     * OR call pgwire_send_error(conn, …) on failure.
     * The user / database from startup are accessible via
     * pgwire_user(conn) / pgwire_database(conn). */
    void (*query)(PgConn *conn, const char *sql, void *ud);
} PgWireCallbacks;
/* Колбэки реализации SQL: связывают wire-протокол с исполнением запросов;
 * `ud` — пользовательский контекст, переданный в pgwire_create. */

/* ── Server lifecycle ───────────────────────────────────────────── */
PgWireServer *pgwire_create(int port, PgWireCallbacks cbs, void *ud);
int           pgwire_start (PgWireServer *s);   /* spawns accept thread; non-blocking */
void          pgwire_stop  (PgWireServer *s);
void          pgwire_destroy(PgWireServer *s);

/* ── Per-connection helpers (called from the query callback) ──── */
const char *pgwire_user    (PgConn *c);
const char *pgwire_database(PgConn *c);

/* Send a column description for the result of the current statement.
 * Must be called BEFORE any pgwire_send_data_row(). */
void pgwire_send_row_description(PgConn *c, int ncols, const PgColumn *cols);

/* Send one row of values. Each `values[i]` is either a NUL-terminated UTF-8
 * string (for non-NULL) or NULL (to send a SQL NULL). All values are sent
 * in text format per protocol. */
void pgwire_send_data_row(PgConn *c, int ncols, const char *const *values);

/* Tag examples: "SELECT 5", "INSERT 0 3", "UPDATE 12", "DELETE 1". */
void pgwire_send_command_complete(PgConn *c, const char *tag);

/* sqlstate is a 5-char SQLSTATE code, e.g. "42601" (syntax_error),
 * "42501" (insufficient_privilege), "28P01" (invalid_password),
 * "XX000" (internal_error). */
void pgwire_send_error(PgConn *c, const char *sqlstate, const char *msg);
