/* pgwire.c — see pgwire.h.
 *
 * Per-connection threading: the accept loop spawns a detached pthread for
 * each socket. Connection state lives on its stack frame, and the wire
 * protocol is fully synchronous, so no locking is needed within a
 * connection. The server-level state (running flag) is protected by an
 * atomic. */
#include "pgwire.h"
#include "../core/log.h"

#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#ifndef MSG_NOSIGNAL
#define MSG_NOSIGNAL 0
#endif

#define PG_PROTOCOL_V3   196608        /* 0x00030000 */
#define PG_SSL_REQUEST   80877103      /* 0x04D2162F */
#define PG_CANCEL_REQUEST 80877102

#define PG_BUF_MAX (1024 * 1024)       /* per-message ceiling */

/* ── Server state ─────────────────────────────────────────────── */
struct PgWireServer {
    int               port;
    int               listen_fd;
    PgWireCallbacks   cbs;
    void             *ud;
    pthread_t         accept_thread;
    volatile int      running;
};

/* ── Per-connection state ─────────────────────────────────────── */

/* Extended Query: prepared statements + portals.
 * `name` is "" for unnamed (the unnamed slots are at index 0). */
#define PG_MAX_PREPARED 32
#define PG_MAX_PORTALS  32

typedef struct {
    char  name[64];
    char *sql;          /* heap-owned (free on Close / overwrite) */
    int   in_use;
} PgPrepared;

typedef struct {
    char         name[64];
    PgPrepared  *prep;          /* points into PgConn::prepared */
    int          n_params;
    char       **values;        /* heap; values[i]=NULL → SQL NULL */
    int          in_use;
} PgPortal;

struct PgConn {
    int               fd;
    PgWireServer     *srv;
    char              user[64];
    char              database[64];
    int               authenticated;
    /* Cancel keys (we just emit zeroes; we don't yet support cancel) */
    int32_t           proc_id;
    int32_t           secret_key;

    /* Extended Query state — Week 4 */
    PgPrepared        prepared[PG_MAX_PREPARED];
    PgPortal          portals [PG_MAX_PORTALS];
    /* When something fails inside an extended-query batch, we set this
     * and skip subsequent P/B/D/E messages until Sync arrives. */
    int               batch_error;
};

/* ── I/O helpers ──────────────────────────────────────────────── */
static int read_full(int fd, void *buf, size_t n) {
    char *p = buf;
    size_t got = 0;
    while (got < n) {
        ssize_t r = read(fd, p + got, n - got);
        if (r < 0 && errno == EINTR) continue;
        if (r <= 0) return -1;
        got += (size_t)r;
    }
    return 0;
}

static int write_full(int fd, const void *buf, size_t n) {
    const char *p = buf;
    size_t sent = 0;
    while (sent < n) {
        ssize_t w = send(fd, p + sent, n - sent, MSG_NOSIGNAL);
        if (w < 0 && errno == EINTR) continue;
        if (w <= 0) return -1;
        sent += (size_t)w;
    }
    return 0;
}

static uint32_t be32(const uint8_t *p) {
    return ((uint32_t)p[0] << 24) | ((uint32_t)p[1] << 16) |
           ((uint32_t)p[2] <<  8) |  (uint32_t)p[3];
}

static void put_be32(uint8_t *p, uint32_t v) {
    p[0] = (uint8_t)(v >> 24); p[1] = (uint8_t)(v >> 16);
    p[2] = (uint8_t)(v >>  8); p[3] = (uint8_t) v;
}

static void put_be16(uint8_t *p, uint16_t v) {
    p[0] = (uint8_t)(v >> 8); p[1] = (uint8_t)v;
}

/* ── Message construction ─────────────────────────────────────── */
typedef struct { uint8_t *buf; size_t len, cap; } MsgBuf;

static void mb_init(MsgBuf *m) {
    m->cap = 256;
    m->len = 5;                /* reserve type + length */
    m->buf = malloc(m->cap);
}

static void mb_reserve(MsgBuf *m, size_t add) {
    if (m->len + add <= m->cap) return;
    while (m->len + add > m->cap) m->cap *= 2;
    m->buf = realloc(m->buf, m->cap);
}

static void mb_u8 (MsgBuf *m, uint8_t v)  { mb_reserve(m, 1); m->buf[m->len++] = v; }
static void mb_u16(MsgBuf *m, uint16_t v) { mb_reserve(m, 2); put_be16(m->buf + m->len, v); m->len += 2; }
static void mb_u32(MsgBuf *m, uint32_t v) { mb_reserve(m, 4); put_be32(m->buf + m->len, v); m->len += 4; }
static void mb_bytes(MsgBuf *m, const void *p, size_t n) {
    mb_reserve(m, n); memcpy(m->buf + m->len, p, n); m->len += n;
}
static void mb_cstr(MsgBuf *m, const char *s) {
    size_t n = strlen(s) + 1; mb_bytes(m, s, n);
}

/* Finalize and write to fd. type==0 means typeless (StartupMessage style). */
static int mb_send(int fd, MsgBuf *m, uint8_t type) {
    int rc;
    if (type == 0) {
        /* No type byte; length is over the entire body including length field */
        uint32_t L = (uint32_t)(m->len - 1);  /* slot 0 unused, body in [1..len) */
        put_be32(m->buf + 1, L);
        rc = write_full(fd, m->buf + 1, m->len - 1);
    } else {
        m->buf[0] = type;
        uint32_t L = (uint32_t)(m->len - 1);  /* length excludes the type byte */
        put_be32(m->buf + 1, L);
        rc = write_full(fd, m->buf, m->len);
    }
    free(m->buf);
    return rc;
}

/* ── Outgoing protocol messages ───────────────────────────────── */
static int send_auth_cleartext(int fd) {
    MsgBuf m; mb_init(&m); mb_u32(&m, 3); /* AuthenticationCleartextPassword */
    return mb_send(fd, &m, 'R');
}

static int send_auth_ok(int fd) {
    MsgBuf m; mb_init(&m); mb_u32(&m, 0);
    return mb_send(fd, &m, 'R');
}

static int send_param_status(int fd, const char *k, const char *v) {
    MsgBuf m; mb_init(&m); mb_cstr(&m, k); mb_cstr(&m, v);
    return mb_send(fd, &m, 'S');
}

static int send_backend_key_data(int fd, int32_t pid, int32_t key) {
    MsgBuf m; mb_init(&m);
    mb_u32(&m, (uint32_t)pid);
    mb_u32(&m, (uint32_t)key);
    return mb_send(fd, &m, 'K');
}

static int send_ready_for_query(int fd, char status) {
    MsgBuf m; mb_init(&m); mb_u8(&m, (uint8_t)status);
    return mb_send(fd, &m, 'Z');
}

/* ── Public helpers — emit query results ──────────────────────── */
void pgwire_send_row_description(PgConn *c, int ncols, const PgColumn *cols) {
    MsgBuf m; mb_init(&m);
    mb_u16(&m, (uint16_t)ncols);
    for (int i = 0; i < ncols; i++) {
        mb_cstr(&m, cols[i].name);
        mb_u32(&m, 0);                       /* table OID */
        mb_u16(&m, 0);                       /* column attr number */
        mb_u32(&m, (uint32_t)cols[i].type_oid);
        mb_u16(&m, (uint16_t)cols[i].type_size);
        mb_u32(&m, 0xFFFFFFFFu);             /* type modifier (-1) */
        mb_u16(&m, 0);                       /* format code = text */
    }
    mb_send(c->fd, &m, 'T');
}

void pgwire_send_data_row(PgConn *c, int ncols, const char *const *values) {
    MsgBuf m; mb_init(&m);
    mb_u16(&m, (uint16_t)ncols);
    for (int i = 0; i < ncols; i++) {
        if (!values[i]) {
            mb_u32(&m, 0xFFFFFFFFu);          /* NULL marker (-1) */
        } else {
            uint32_t L = (uint32_t)strlen(values[i]);
            mb_u32(&m, L);
            mb_bytes(&m, values[i], L);
        }
    }
    mb_send(c->fd, &m, 'D');
}

void pgwire_send_command_complete(PgConn *c, const char *tag) {
    MsgBuf m; mb_init(&m); mb_cstr(&m, tag);
    mb_send(c->fd, &m, 'C');
}

void pgwire_send_error(PgConn *c, const char *sqlstate, const char *msg) {
    MsgBuf m; mb_init(&m);
    mb_u8(&m, 'S'); mb_cstr(&m, "ERROR");
    mb_u8(&m, 'C'); mb_cstr(&m, sqlstate ? sqlstate : "XX000");
    mb_u8(&m, 'M'); mb_cstr(&m, msg ? msg : "internal error");
    mb_u8(&m, 0);                              /* terminator */
    mb_send(c->fd, &m, 'E');
}

const char *pgwire_user    (PgConn *c) { return c->user; }
const char *pgwire_database(PgConn *c) { return c->database; }

/* ── Startup handshake ────────────────────────────────────────── */
/* Returns 0 to continue, 1 if the client just probed SSL and should
 * be re-read, -1 on fatal error. */
static int read_startup(PgConn *c, uint8_t *body, uint32_t *body_len_out) {
    uint8_t hdr[4];
    if (read_full(c->fd, hdr, 4) != 0) return -1;
    uint32_t total = be32(hdr);
    if (total < 8 || total > PG_BUF_MAX) return -1;
    uint32_t body_len = total - 4;
    if (read_full(c->fd, body, body_len) != 0) return -1;
    *body_len_out = body_len;

    uint32_t code = be32(body);
    if (code == PG_SSL_REQUEST) {
        /* Reply 'N' — we don't speak SSL yet; client falls back to plaintext. */
        char n = 'N';
        if (write_full(c->fd, &n, 1) != 0) return -1;
        return 1;
    }
    if (code == PG_CANCEL_REQUEST) {
        /* No-op for now; clients connect, send cancel, close. */
        return -1;
    }
    if (code != PG_PROTOCOL_V3) {
        LOG_WARN("pgwire: unsupported protocol version %u", code);
        return -1;
    }
    return 0;
}

static void parse_startup_kv(PgConn *c, const uint8_t *body, uint32_t body_len) {
    /* body = 4-byte version (already validated) + key/value cstrings + null */
    const char *p   = (const char *)body + 4;
    const char *end = (const char *)body + body_len;
    while (p < end && *p) {
        const char *key = p;
        while (p < end && *p) p++;
        if (p >= end) break;
        p++;                                /* skip key NUL */
        const char *val = p;
        while (p < end && *p) p++;
        if (p >= end) break;
        if      (strcmp(key, "user")     == 0) strncpy(c->user,     val, sizeof(c->user)     - 1);
        else if (strcmp(key, "database") == 0) strncpy(c->database, val, sizeof(c->database) - 1);
        p++;                                /* skip value NUL */
    }
    if (!c->database[0]) strncpy(c->database, c->user, sizeof(c->database) - 1);
}

/* ── Main per-connection loop ─────────────────────────────────── */
static int handle_password(PgConn *c) {
    uint8_t  hdr[5];
    if (read_full(c->fd, hdr, 5) != 0) return -1;
    if (hdr[0] != 'p') return -1;
    uint32_t L = be32(hdr + 1);
    if (L < 5 || L - 4 > PG_BUF_MAX) return -1;
    uint32_t body_len = L - 4;
    char *pw = malloc(body_len + 1);
    if (!pw) return -1;
    if (read_full(c->fd, pw, body_len) != 0) { free(pw); return -1; }
    pw[body_len] = '\0';                    /* defensive */
    /* PasswordMessage body: cstring */
    int rc = -1;
    if (c->srv->cbs.authenticate) {
        rc = c->srv->cbs.authenticate(c->user, pw, c->database, c->srv->ud);
    }
    /* zero-out the password buffer ASAP */
    memset(pw, 0, body_len);
    free(pw);
    return rc;
}

/* ── Extended Query — wire emitters ──────────────────────────── */
static int send_simple_tag(int fd, uint8_t type) {
    /* Empty-body messages: ParseComplete (1), BindComplete (2),
     * CloseComplete (3), NoData (n), EmptyQueryResponse (I). */
    MsgBuf m; mb_init(&m);
    return mb_send(fd, &m, type);
}

/* ── Extended Query — body parsing helpers ───────────────────── */

/* Read a NUL-terminated cstring from `buf`. *off advanced past the NUL.
 * Returns NULL on overflow. */
static const char *xq_cstr(const uint8_t *buf, size_t len, size_t *off) {
    if (*off >= len) return NULL;
    const char *s = (const char *)(buf + *off);
    while (*off < len && buf[*off] != 0) (*off)++;
    if (*off >= len) return NULL;
    (*off)++;            /* consume NUL */
    return s;
}

static int xq_int16(const uint8_t *buf, size_t len, size_t *off, int16_t *out) {
    if (*off + 2 > len) return -1;
    *out = (int16_t)((buf[*off] << 8) | buf[*off + 1]);
    *off += 2;
    return 0;
}

static int xq_int32(const uint8_t *buf, size_t len, size_t *off, int32_t *out) {
    if (*off + 4 > len) return -1;
    *out = (int32_t)be32(buf + *off);
    *off += 4;
    return 0;
}

/* ── Extended Query — slot helpers ───────────────────────────── */

static PgPrepared *xq_find_or_alloc_prepared(PgConn *c, const char *name) {
    /* Replace existing slot with same name OR find first empty one. */
    for (int i = 0; i < PG_MAX_PREPARED; i++) {
        if (c->prepared[i].in_use && strcmp(c->prepared[i].name, name) == 0)
            return &c->prepared[i];
    }
    for (int i = 0; i < PG_MAX_PREPARED; i++) {
        if (!c->prepared[i].in_use) return &c->prepared[i];
    }
    return NULL;
}

static PgPrepared *xq_find_prepared(PgConn *c, const char *name) {
    for (int i = 0; i < PG_MAX_PREPARED; i++) {
        if (c->prepared[i].in_use && strcmp(c->prepared[i].name, name) == 0)
            return &c->prepared[i];
    }
    return NULL;
}

static void xq_prepared_release(PgPrepared *p) {
    if (!p->in_use) return;
    free(p->sql); p->sql = NULL;
    p->name[0] = 0;
    p->in_use = 0;
}

static PgPortal *xq_find_or_alloc_portal(PgConn *c, const char *name) {
    for (int i = 0; i < PG_MAX_PORTALS; i++) {
        if (c->portals[i].in_use && strcmp(c->portals[i].name, name) == 0)
            return &c->portals[i];
    }
    for (int i = 0; i < PG_MAX_PORTALS; i++) {
        if (!c->portals[i].in_use) return &c->portals[i];
    }
    return NULL;
}

static PgPortal *xq_find_portal(PgConn *c, const char *name) {
    for (int i = 0; i < PG_MAX_PORTALS; i++) {
        if (c->portals[i].in_use && strcmp(c->portals[i].name, name) == 0)
            return &c->portals[i];
    }
    return NULL;
}

static void xq_portal_release(PgPortal *p) {
    if (!p->in_use) return;
    if (p->values) {
        for (int i = 0; i < p->n_params; i++) free(p->values[i]);
        free(p->values);
        p->values = NULL;
    }
    p->n_params = 0;
    p->prep = NULL;
    p->name[0] = 0;
    p->in_use = 0;
}

/* ── Extended Query — parameter substitution ─────────────────── */

/* Decide whether a string-typed value should be emitted as raw SQL
 * (numeric / NULL / booleans) or single-quoted text. */
static int xq_value_is_numeric(const char *v) {
    if (!v || !*v) return 0;
    const char *p = v;
    if (*p == '-' || *p == '+') p++;
    int has_digit = 0;
    while (*p >= '0' && *p <= '9') { p++; has_digit = 1; }
    if (has_digit && *p == '\0') return 1;
    if (has_digit && *p == '.') {
        p++;
        while (*p >= '0' && *p <= '9') p++;
        if (*p == 'e' || *p == 'E') {
            p++; if (*p == '-' || *p == '+') p++;
            while (*p >= '0' && *p <= '9') p++;
        }
        if (*p == '\0') return 1;
    }
    return 0;
}

/* Append a single bound value to `out` as SQL syntax, advancing *off.
 * Output buffer is sized at the call site to be safely larger than
 * sql_len + sum_of_value_lens * 2 + small overhead. */
static void xq_append_value(char *out, size_t cap, size_t *off,
                            const char *v) {
    if (!v) {                                        /* SQL NULL */
        const char *n = "NULL";
        size_t nl = 4;
        if (*off + nl < cap) { memcpy(out + *off, n, nl); *off += nl; }
        return;
    }
    if (xq_value_is_numeric(v) ||
        !strcasecmp(v, "true") || !strcasecmp(v, "false")) {
        size_t vl = strlen(v);
        if (*off + vl < cap) { memcpy(out + *off, v, vl); *off += vl; }
        return;
    }
    /* Quote as text literal, doubling embedded ' per SQL standard. */
    if (*off + 1 < cap) out[(*off)++] = '\'';
    for (const char *p = v; *p && *off + 2 < cap; p++) {
        if (*p == '\'') { out[(*off)++] = '\''; out[(*off)++] = '\''; }
        else            { out[(*off)++] = *p; }
    }
    if (*off + 1 < cap) out[(*off)++] = '\'';
}

/* Substitute $1..$N in `sql` with the bound values. Numbers are
 * inlined raw, strings get single-quoted with '' escape, NULLs are
 * emitted as the keyword NULL. Heap-allocates the result. */
static char *xq_substitute_params(const char *sql, char **values, int n_params) {
    size_t sql_len = strlen(sql);
    size_t cap = sql_len + 64;
    for (int i = 0; i < n_params; i++) {
        if (values[i]) cap += strlen(values[i]) * 2 + 4;
        else           cap += 6;                     /* "NULL" */
    }
    char  *out = malloc(cap);
    if (!out) return NULL;
    size_t off = 0;
    for (size_t i = 0; i < sql_len; ) {
        if (sql[i] == '$' && sql[i+1] >= '0' && sql[i+1] <= '9') {
            i++;
            int idx = 0;
            while (sql[i] >= '0' && sql[i] <= '9') { idx = idx*10 + (sql[i] - '0'); i++; }
            if (idx >= 1 && idx <= n_params) {
                xq_append_value(out, cap, &off, values[idx - 1]);
            } else {
                /* Out of range: emit NULL — defensive. */
                xq_append_value(out, cap, &off, NULL);
            }
            continue;
        }
        if (off + 1 < cap) out[off++] = sql[i];
        i++;
    }
    out[off] = '\0';
    return out;
}

/* ── Extended Query — message handlers ───────────────────────── */

static void handle_parse(PgConn *c, const uint8_t *body, size_t len) {
    size_t off = 0;
    const char *name = xq_cstr(body, len, &off);
    const char *sql  = xq_cstr(body, len, &off);
    if (!name || !sql) {
        pgwire_send_error(c, "08P01", "Parse: malformed body");
        c->batch_error = 1; return;
    }
    /* Skip parameter type OIDs (we don't use them; substitution is text-mode) */
    int16_t n_param_types = 0;
    if (xq_int16(body, len, &off, &n_param_types) < 0) {
        pgwire_send_error(c, "08P01", "Parse: missing param-type count");
        c->batch_error = 1; return;
    }
    off += (size_t)n_param_types * 4;                /* ignore type oids */

    PgPrepared *p = xq_find_or_alloc_prepared(c, name);
    if (!p) {
        pgwire_send_error(c, "53000", "too many prepared statements");
        c->batch_error = 1; return;
    }
    if (p->in_use) xq_prepared_release(p);
    strncpy(p->name, name, sizeof(p->name) - 1);
    p->name[sizeof(p->name) - 1] = '\0';
    p->sql = strdup(sql);
    p->in_use = 1;
    send_simple_tag(c->fd, '1');                     /* ParseComplete */
}

static void handle_bind(PgConn *c, const uint8_t *body, size_t len) {
    size_t off = 0;
    const char *portal_name = xq_cstr(body, len, &off);
    const char *stmt_name   = xq_cstr(body, len, &off);
    if (!portal_name || !stmt_name) {
        pgwire_send_error(c, "08P01", "Bind: malformed cstrings");
        c->batch_error = 1; return;
    }

    int16_t n_fmt = 0;
    if (xq_int16(body, len, &off, &n_fmt) < 0) {
        pgwire_send_error(c, "08P01", "Bind: missing format-code count");
        c->batch_error = 1; return;
    }
    /* Reject any non-zero (binary) format code — Week 4 supports text only.
     * `n_fmt`==0 means "all text"; ==1 means "all values use this code";
     * == n_params means per-value. */
    int all_text = (n_fmt == 0);
    int16_t default_fmt = 0;
    int    *param_fmts = NULL;
    if (n_fmt == 1) {
        if (xq_int16(body, len, &off, &default_fmt) < 0) goto bad;
    } else if (n_fmt > 1) {
        param_fmts = malloc((size_t)n_fmt * sizeof(int));
        if (!param_fmts) goto bad;
        for (int i = 0; i < n_fmt; i++) {
            int16_t v = 0; if (xq_int16(body, len, &off, &v) < 0) { free(param_fmts); goto bad; }
            param_fmts[i] = v;
        }
    }

    int16_t n_params = 0;
    if (xq_int16(body, len, &off, &n_params) < 0) { free(param_fmts); goto bad; }

    char **values = NULL;
    if (n_params > 0) {
        values = calloc((size_t)n_params, sizeof(char *));
        if (!values) { free(param_fmts); goto bad; }
        for (int i = 0; i < n_params; i++) {
            int32_t L = 0;
            if (xq_int32(body, len, &off, &L) < 0) { goto values_bad; }
            if (L < 0) { values[i] = NULL; continue; }
            if ((size_t)off + (size_t)L > len) goto values_bad;
            int fmt = all_text ? 0
                    : (n_fmt == 1 ? default_fmt
                                  : (i < n_fmt ? param_fmts[i] : 0));
            if (fmt != 0) {
                pgwire_send_error(c, "0A000",
                    "binary parameter format not supported (use text format)");
                /* free what we already alloc'd */
                for (int j = 0; j < i; j++) free(values[j]);
                free(values); free(param_fmts);
                c->batch_error = 1;
                return;
            }
            values[i] = malloc((size_t)L + 1);
            if (!values[i]) goto values_bad;
            memcpy(values[i], body + off, (size_t)L);
            values[i][L] = '\0';
            off += (size_t)L;
        }
    }
    free(param_fmts);

    /* Skip result format codes — we always emit text. */
    int16_t n_res_fmt = 0;
    xq_int16(body, len, &off, &n_res_fmt);
    off += (size_t)n_res_fmt * 2;

    PgPrepared *prep = xq_find_prepared(c, stmt_name);
    if (!prep) {
        pgwire_send_error(c, "26000", "Bind: prepared statement not found");
        for (int i = 0; i < n_params; i++) if (values) free(values[i]);
        free(values);
        c->batch_error = 1; return;
    }
    PgPortal *po = xq_find_or_alloc_portal(c, portal_name);
    if (!po) {
        pgwire_send_error(c, "53000", "too many portals");
        for (int i = 0; i < n_params; i++) if (values) free(values[i]);
        free(values);
        c->batch_error = 1; return;
    }
    if (po->in_use) xq_portal_release(po);
    strncpy(po->name, portal_name, sizeof(po->name) - 1);
    po->name[sizeof(po->name) - 1] = '\0';
    po->prep = prep;
    po->n_params = n_params;
    po->values = values;
    po->in_use = 1;
    send_simple_tag(c->fd, '2');                     /* BindComplete */
    return;

values_bad:
    if (values) {
        for (int i = 0; i < n_params; i++) free(values[i]);
        free(values);
    }
    free(param_fmts);
bad:
    pgwire_send_error(c, "08P01", "Bind: malformed body");
    c->batch_error = 1;
}

static void handle_describe(PgConn *c, const uint8_t *body, size_t len) {
    /* We don't know the result schema until we run the query — emit
     * NoData and let Execute send the actual RowDescription. Most Postgres
     * clients tolerate this; it costs them one extra round-trip if they
     * relied on Describe to plan. */
    if (len < 1) { pgwire_send_error(c, "08P01", "Describe: empty body"); c->batch_error = 1; return; }
    /* body[0] = 'S' (statement) or 'P' (portal); name follows; we don't read it */
    send_simple_tag(c->fd, 'n');                     /* NoData */
}

static void handle_execute(PgConn *c, const uint8_t *body, size_t len) {
    size_t off = 0;
    const char *portal_name = xq_cstr(body, len, &off);
    int32_t max_rows = 0;
    if (!portal_name || xq_int32(body, len, &off, &max_rows) < 0) {
        pgwire_send_error(c, "08P01", "Execute: malformed body");
        c->batch_error = 1; return;
    }
    (void)max_rows;                                   /* we don't paginate yet */

    PgPortal *po = xq_find_portal(c, portal_name);
    if (!po) {
        pgwire_send_error(c, "26000", "Execute: portal not found");
        c->batch_error = 1; return;
    }
    if (!po->prep || !po->prep->sql) {
        pgwire_send_error(c, "26000", "Execute: portal has no statement");
        c->batch_error = 1; return;
    }

    char *final_sql = (po->n_params > 0)
        ? xq_substitute_params(po->prep->sql, po->values, po->n_params)
        : strdup(po->prep->sql);
    if (!final_sql) {
        pgwire_send_error(c, "53200", "out of memory");
        c->batch_error = 1; return;
    }
    if (c->srv->cbs.query) c->srv->cbs.query(c, final_sql, c->srv->ud);
    free(final_sql);
}

static void handle_close(PgConn *c, const uint8_t *body, size_t len) {
    if (len < 1) { send_simple_tag(c->fd, '3'); return; }
    char kind = (char)body[0];
    size_t off = 1;
    const char *name = xq_cstr(body, len, &off);
    if (!name) name = "";
    if (kind == 'S') {
        PgPrepared *p = xq_find_prepared(c, name);
        if (p) xq_prepared_release(p);
    } else if (kind == 'P') {
        PgPortal *p = xq_find_portal(c, name);
        if (p) xq_portal_release(p);
    }
    send_simple_tag(c->fd, '3');                     /* CloseComplete */
}

/* Главный цикл обработки сообщений после успешной аутентификации:
 * читает заголовок (тип + длина), затем тело и диспетчеризует по типу
 * сообщения — Simple Query ('Q') и Extended Query ('P'/'B'/'D'/'E'/'C'/'S').
 * Возвращает 0 при штатном Terminate, -1 при ошибке/обрыве соединения. */
static int run_query_loop(PgConn *c) {
    while (c->srv->running) {
        uint8_t hdr[5];
        if (read_full(c->fd, hdr, 5) != 0) return -1;
        uint8_t  type = hdr[0];
        uint32_t L    = be32(hdr + 1);
        if (L < 4 || L - 4 > PG_BUF_MAX) return -1;
        uint32_t body_len = L - 4;
        uint8_t *body = body_len ? malloc(body_len) : NULL;
        if (body_len && !body) return -1;
        if (body_len && read_full(c->fd, body, body_len) != 0) { free(body); return -1; }

        switch (type) {
            case 'Q': {
                /* Simple Query: cstring */
                const char *sql = body_len ? (const char *)body : "";
                if (c->srv->cbs.query) {
                    c->srv->cbs.query(c, sql, c->srv->ud);
                } else {
                    pgwire_send_error(c, "0A000", "no query handler installed");
                }
                send_ready_for_query(c->fd, 'I');
                break;
            }
            /* ── Extended Query batch (Week 4) ── */
            case 'P': if (!c->batch_error) handle_parse   (c, body, body_len); break;
            case 'B': if (!c->batch_error) handle_bind    (c, body, body_len); break;
            case 'D': if (!c->batch_error) handle_describe(c, body, body_len); break;
            case 'E': if (!c->batch_error) handle_execute (c, body, body_len); break;
            case 'C': if (!c->batch_error) handle_close   (c, body, body_len); break;
            case 'S':                                        /* Sync */
                send_ready_for_query(c->fd, c->batch_error ? 'E' : 'I');
                c->batch_error = 0;
                break;
            case 'H':                                        /* Flush — we already flush */
                break;
            case 'X':                                        /* Terminate */
                free(body);
                return 0;
            case 'F': case 'd': case 'f':
                /* FunctionCall / CopyData / CopyFail — not supported */
                pgwire_send_error(c, "0A000", "FunctionCall / COPY not supported");
                send_ready_for_query(c->fd, 'I');
                break;
            default:
                LOG_WARN("pgwire: unknown message type 0x%02X (%c)", type,
                         (type >= 32 && type < 127) ? type : '?');
                pgwire_send_error(c, "08P01", "unknown protocol message");
                send_ready_for_query(c->fd, 'I');
                break;
        }
        free(body);
    }
    return 0;
}

/* Точка входа потока соединения: startup-хендшейк (с возможным SSL-пробом),
 * cleartext-аутентификация, отправка стартовых ParameterStatus и переход в
 * цикл запросов. На выходе освобождает состояние Extended Query и закрывает fd. */
static void *connection_thread(void *arg) {
    PgConn *c = arg;
    LOG_INFO("pgwire: client connected fd=%d", c->fd);

    uint8_t body[8192];
    uint32_t body_len = 0;
    int sr = read_startup(c, body, &body_len);
    if (sr == 1) {
        /* SSLRequest: we replied 'N', read the real StartupMessage now */
        sr = read_startup(c, body, &body_len);
    }
    if (sr != 0) goto done;

    parse_startup_kv(c, body, body_len);
    if (!c->user[0]) {
        pgwire_send_error(c, "28000", "missing user in startup");
        goto done;
    }

    /* Cleartext auth round-trip */
    if (send_auth_cleartext(c->fd) != 0) goto done;
    if (handle_password(c) != 0) {
        pgwire_send_error(c, "28P01", "password authentication failed");
        goto done;
    }
    c->authenticated = 1;
    c->proc_id    = (int32_t)(intptr_t)c & 0x7FFFFFFF;
    c->secret_key = 0;

    if (send_auth_ok(c->fd)                                      != 0 ||
        send_param_status(c->fd, "client_encoding", "UTF8")      != 0 ||
        send_param_status(c->fd, "DateStyle",       "ISO, MDY")  != 0 ||
        send_param_status(c->fd, "TimeZone",        "UTC")       != 0 ||
        send_param_status(c->fd, "server_version",  "16.0 (NAPASTAK)") != 0 ||
        send_backend_key_data(c->fd, c->proc_id, c->secret_key)  != 0 ||
        send_ready_for_query(c->fd, 'I')                         != 0)
        goto done;

    LOG_INFO("pgwire: auth ok user=%s db=%s", c->user, c->database);
    run_query_loop(c);

done:
    /* Free Extended Query state */
    for (int i = 0; i < PG_MAX_PREPARED; i++) xq_prepared_release(&c->prepared[i]);
    for (int i = 0; i < PG_MAX_PORTALS;  i++) xq_portal_release  (&c->portals [i]);
    close(c->fd);
    LOG_INFO("pgwire: client disconnected");
    free(c);
    return NULL;
}

/* ── Accept loop ──────────────────────────────────────────────── */
/* Цикл accept: на каждое входящее соединение выделяет PgConn и порождает
 * отдельный detached-поток connection_thread с увеличенным стеком. */
static void *accept_thread_fn(void *arg) {
    PgWireServer *s = arg;
    LOG_INFO("pgwire: listening on :%d", s->port);
    while (s->running) {
        struct sockaddr_in peer;
        socklen_t plen = sizeof(peer);
        int cfd = accept(s->listen_fd, (struct sockaddr *)&peer, &plen);
        if (cfd < 0) {
            if (errno == EINTR || errno == EAGAIN) continue;
            if (!s->running) break;
            LOG_WARN("pgwire: accept failed: %s", strerror(errno));
            continue;
        }
        PgConn *c = calloc(1, sizeof(*c));
        if (!c) { close(cfd); continue; }
        c->fd  = cfd;
        c->srv = s;
        pthread_t t;
        /* macOS pthread default stack is 512 KB — too small for the
         * recursive-descent SQL parser + qengine. Use 4 MB. */
        pthread_attr_t attr;
        pthread_attr_init(&attr);
        pthread_attr_setstacksize(&attr, 4 * 1024 * 1024);
        int prc = pthread_create(&t, &attr, connection_thread, c);
        pthread_attr_destroy(&attr);
        if (prc != 0) { close(cfd); free(c); continue; }
        pthread_detach(t);
    }
    LOG_INFO("pgwire: accept loop stopped");
    return NULL;
}

/* ── Public lifecycle ─────────────────────────────────────────── */
PgWireServer *pgwire_create(int port, PgWireCallbacks cbs, void *ud) {
    PgWireServer *s = calloc(1, sizeof(*s));
    if (!s) return NULL;
    s->port      = port;
    s->cbs       = cbs;
    s->ud        = ud;
    s->listen_fd = -1;
    return s;
}

int pgwire_start(PgWireServer *s) {
    if (!s) return -1;
    s->listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (s->listen_fd < 0) { LOG_ERROR("pgwire: socket failed"); return -1; }
    int opt = 1;
    setsockopt(s->listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port        = htons((uint16_t)s->port);
    if (bind(s->listen_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        LOG_ERROR("pgwire: bind :%d failed: %s", s->port, strerror(errno));
        close(s->listen_fd); s->listen_fd = -1; return -1;
    }
    if (listen(s->listen_fd, 32) < 0) {
        LOG_ERROR("pgwire: listen failed");
        close(s->listen_fd); s->listen_fd = -1; return -1;
    }
    s->running = 1;
    if (pthread_create(&s->accept_thread, NULL, accept_thread_fn, s) != 0) {
        LOG_ERROR("pgwire: pthread_create failed");
        s->running = 0;
        close(s->listen_fd); s->listen_fd = -1; return -1;
    }
    return 0;
}

void pgwire_stop(PgWireServer *s) {
    if (!s || !s->running) return;
    s->running = 0;
    if (s->listen_fd >= 0) {
        shutdown(s->listen_fd, SHUT_RDWR);
        close(s->listen_fd);
        s->listen_fd = -1;
    }
    pthread_join(s->accept_thread, NULL);
}

void pgwire_destroy(PgWireServer *s) {
    if (!s) return;
    pgwire_stop(s);
    free(s);
}
