#include "app.h"
#include "../../lib/core/log.h"
#include "../../lib/auth/auth.h"
#include "../../lib/core/json.h"
#include "../../lib/net/tls.h"
#include "../../lib/yaml/yaml_loader.h"
#include "../../lib/yaml/yaml_template.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <signal.h>
#include <unistd.h>
#include <errno.h>
#include <dirent.h>
#include <sys/stat.h>
#include <sys/socket.h>
#ifndef MSG_NOSIGNAL
#define MSG_NOSIGNAL 0
#endif

static char *expand_env_vars(const char *input) {
    if (!input) return NULL;
    size_t len = strlen(input);
    size_t cap = len + 256;
    char *out = malloc(cap + 1);
    if (!out) return NULL;

    const char *src = input;
    char *dst = out;
    while (*src) {
        if (src[0] == '$' && src[1] == '{') {
            const char *start = src + 2;
            const char *end = strchr(start, '}');
            if (end) {
                size_t name_len = (size_t)(end - start);
                char name[256];
                if (name_len >= sizeof(name)) name_len = sizeof(name) - 1;
                memcpy(name, start, name_len);
                name[name_len] = '\0';
                const char *val = getenv(name);
                if (val) {
                    size_t vlen = strlen(val);
                    if ((size_t)(dst - out) + vlen + 1 > cap) {
                        cap = (size_t)(dst - out) + vlen + 1 + 256;
                        char *tmp = realloc(out, cap);
                        if (!tmp) { free(out); return NULL; }
                        dst = tmp + (dst - out);
                        out = tmp;
                    }
                    memcpy(dst, val, vlen);
                    dst += vlen;
                }
                src = end + 1;
                continue;
            }
        }
        if ((size_t)(dst - out) + 2 > cap) {
            cap *= 2;
            char *tmp = realloc(out, cap);
            if (!tmp) { free(out); return NULL; }
            dst = tmp + (dst - out);
            out = tmp;
        }
        *dst++ = *src++;
    }
    *dst = '\0';
    return out;
}


App g_app;

/* WebSocket broadcast to all connected clients */
void app_ws_broadcast(App *app, const char *json_msg) {
    if (!json_msg) return;
    pthread_mutex_lock(&app->ws_mu);
    size_t len = strlen(json_msg);
    if (len > 65535) { pthread_mutex_unlock(&app->ws_mu); return; }
    /* Frame: FIN=1, opcode=1 (text), mask=0 */
    uint8_t frame[16+65536]; int flen=0;
    frame[flen++]=0x81; /* FIN + text */
    if(len<126) frame[flen++]=(uint8_t)len;
    else{ frame[flen++]=126; frame[flen++]=(uint8_t)(len>>8); frame[flen++]=(uint8_t)len; }
    memcpy(frame+flen,json_msg,len); flen+=(int)len;
    int dead[256]; int ndead=0;
    for(int i=0;i<app->nws_clients;i++){
        WsClient *wc = &app->ws_clients[i];
        ssize_t r = wc->tls
            ? tls_write(wc->tls, frame, (size_t)flen)
            : send(wc->fd, frame, (size_t)flen, MSG_NOSIGNAL);
        if(r<0) dead[ndead++]=i;
    }
    for(int i=ndead-1;i>=0;i--){
        int di = dead[i];
        if (di < 0 || di >= app->nws_clients) continue;
        WsClient *dc = &app->ws_clients[di];
        if (dc->tls) { tls_conn_destroy(dc->tls); }
        app->ws_clients[di]=app->ws_clients[--app->nws_clients];
    }
    pthread_mutex_unlock(&app->ws_mu);
}

/* ── Step 3 Week 1: PostgreSQL wire-protocol callbacks ─────────────
 *
 * Auth: accept either (admin, admin_password) or (anything, "dfo_xxx" API key).
 *       Cleartext over TCP — fine for loopback/VPN; SCRAM + TLS land later.
 * Query: this Week 1 build understands a small set of compatibility probes
 *        that psql / DBeaver / Tableau emit on connect:
 *          SELECT 1 / SELECT 1+1 / etc — constant-folded scalar
 *          SELECT version() / SHOW server_version
 *          SELECT current_database() / current_user / current_schema()
 *          BEGIN / COMMIT / ROLLBACK / SET ... — accepted as no-ops
 *        Anything else returns a clear error pointing at the JSON API.
 *        Real qengine integration is Week 2.                                */
static int pg_authenticate_cb(const char *user, const char *password,
                              const char *database, void *ud) {
    App *app = (App *)ud;
    (void)database;
    if (!user || !password) return -1;
    /* admin → admin_password (configured) */
    if (strcmp(user, "admin") == 0 && app->admin_password[0] &&
        strcmp(password, app->admin_password) == 0)
        return 0;
    /* API-key auth: password is a "dfo_<hex>" token */
    if (app->auth_store && strncmp(password, "dfo_", 4) == 0) {
        AuthClaims c;
        if (auth_apikey_verify(app->auth_store, password, &c) == 0) return 0;
    }
    LOG_WARN("pgwire: auth rejected for user=%s", user);
    return -1;
}

/* Lower-case + strip leading whitespace + drop trailing ';' for matching. */
static void normalize_sql(const char *sql, char *out, size_t cap) {
    while (*sql == ' ' || *sql == '\t' || *sql == '\n') sql++;
    size_t n = 0;
    while (*sql && n + 1 < cap) {
        char c = *sql++;
        out[n++] = (c >= 'A' && c <= 'Z') ? (char)(c - 'A' + 'a') : c;
    }
    out[n] = '\0';
    /* trim trailing ';' or whitespace */
    while (n > 0 && (out[n-1] == ';' || out[n-1] == ' ' ||
                     out[n-1] == '\t' || out[n-1] == '\n')) out[--n] = '\0';
}

static int starts_with(const char *s, const char *prefix) {
    return strncmp(s, prefix, strlen(prefix)) == 0;
}

static void pg_query_cb(PgConn *conn, const char *sql, void *ud) {
    (void)ud;
    char norm[1024]; normalize_sql(sql, norm, sizeof(norm));

    /* Empty statement — psql sends "" between commands */
    if (norm[0] == '\0') {
        pgwire_send_command_complete(conn, "");
        return;
    }

    /* Transaction control: accept as no-ops (no real txn yet over pgwire) */
    if (strcmp(norm, "begin") == 0 || strcmp(norm, "begin transaction") == 0) {
        pgwire_send_command_complete(conn, "BEGIN");          return;
    }
    if (strcmp(norm, "commit") == 0 || strcmp(norm, "commit transaction") == 0) {
        pgwire_send_command_complete(conn, "COMMIT");         return;
    }
    if (strcmp(norm, "rollback") == 0) {
        pgwire_send_command_complete(conn, "ROLLBACK");       return;
    }
    if (starts_with(norm, "set ")) {
        pgwire_send_command_complete(conn, "SET");            return;
    }
    if (starts_with(norm, "discard ")) {
        pgwire_send_command_complete(conn, "DISCARD ALL");    return;
    }

    /* SHOW server_version → single text row */
    if (strcmp(norm, "show server_version") == 0) {
        PgColumn c[] = {{"server_version", PG_OID_TEXT, -1}};
        pgwire_send_row_description(conn, 1, c);
        const char *row[] = {"16.0 (DataFlow OS)"};
        pgwire_send_data_row(conn, 1, row);
        pgwire_send_command_complete(conn, "SHOW");
        return;
    }

    /* SELECT version() */
    if (strcmp(norm, "select version()") == 0) {
        PgColumn c[] = {{"version", PG_OID_TEXT, -1}};
        pgwire_send_row_description(conn, 1, c);
        const char *row[] = {"DataFlow OS 0.1 (PostgreSQL-compatible wire protocol)"};
        pgwire_send_data_row(conn, 1, row);
        pgwire_send_command_complete(conn, "SELECT 1");
        return;
    }

    /* SELECT current_database() / current_user / current_schema() */
    if (strcmp(norm, "select current_database()") == 0) {
        PgColumn c[] = {{"current_database", PG_OID_TEXT, -1}};
        pgwire_send_row_description(conn, 1, c);
        const char *row[] = { pgwire_database(conn) };
        pgwire_send_data_row(conn, 1, row);
        pgwire_send_command_complete(conn, "SELECT 1");
        return;
    }
    if (strcmp(norm, "select current_user") == 0 ||
        strcmp(norm, "select current_user()") == 0 ||
        strcmp(norm, "select user") == 0) {
        PgColumn c[] = {{"current_user", PG_OID_TEXT, -1}};
        pgwire_send_row_description(conn, 1, c);
        const char *row[] = { pgwire_user(conn) };
        pgwire_send_data_row(conn, 1, row);
        pgwire_send_command_complete(conn, "SELECT 1");
        return;
    }
    if (strcmp(norm, "select current_schema()") == 0 ||
        strcmp(norm, "select current_schema") == 0) {
        PgColumn c[] = {{"current_schema", PG_OID_TEXT, -1}};
        pgwire_send_row_description(conn, 1, c);
        const char *row[] = { "public" };
        pgwire_send_data_row(conn, 1, row);
        pgwire_send_command_complete(conn, "SELECT 1");
        return;
    }

    /* Everything else — including SELECT 1, SELECT * FROM users, INSERT,
     * UPDATE, DELETE, GROUP BY, JOINs — goes through the real engine via
     * api_pg_execute. Result rows are streamed as text-format DataRows. */
    api_pg_execute(conn, sql);
}

static void on_pipeline_run(Pipeline *p, void *ud) {
    App *app=(App*)ud;
    LOG_INFO("running pipeline %s (%s)", p->name, p->id);
    app->metrics->total_pipelines_run++;
    Arena *a=arena_create(256);
    app_ws_broadcast(app,arena_sprintf(a,"{\"event\":\"pipeline_triggered\",\"id\":\"%s\"}",p->id));
    arena_destroy(a);
    pipeline_execute_steps(p, app);
}

void app_init(App *app, const char *config_json) {
    memset(app, 0, sizeof(App));
    strncpy(app->data_dir,    DATA_DIR_DEFAULT,    sizeof(app->data_dir)-1);
    strncpy(app->db_path,     DB_PATH_DEFAULT,     sizeof(app->db_path)-1);
    strncpy(app->plugins_dir, "./build/release/lib", sizeof(app->plugins_dir)-1);
    strncpy(app->sql_templates_dir, "./sql", sizeof(app->sql_templates_dir)-1);
    app->port = DEFAULT_PORT;

    /* parse config */
    if (config_json && *config_json) {
        Arena *a=arena_create(4096);
        JVal *cfg=json_parse(a,config_json,strlen(config_json));
        if(cfg&&cfg->type==JV_OBJECT){
            const char *dp=json_str(json_get(cfg,"data_dir"),NULL);
            if(dp) {
                char *expanded = expand_env_vars(dp);
                if (expanded) { strncpy(app->data_dir,expanded,sizeof(app->data_dir)-1); free(expanded); }
            }
            const char *pd=json_str(json_get(cfg,"plugins_dir"),NULL);
            if(pd) {
                char *expanded = expand_env_vars(pd);
                if (expanded) { strncpy(app->plugins_dir,expanded,sizeof(app->plugins_dir)-1); free(expanded); }
            }
            int port=(int)json_int(json_get(cfg,"port"),0);
            if(port>0) app->port=port;
            app->auth_enabled = json_int(json_get(cfg,"auth_enabled"),1);  // default true
            const char *js=json_str(json_get(cfg,"jwt_secret"),NULL);
            if(js) {
                char *expanded = expand_env_vars(js);
                if (expanded) { strncpy(app->jwt_secret,expanded,sizeof(app->jwt_secret)-1); free(expanded); }
            }
            const char *ap=json_str(json_get(cfg,"admin_password"),NULL);
            if(ap) {
                char *expanded = expand_env_vars(ap);
                if (expanded) { strncpy(app->admin_password,expanded,sizeof(app->admin_password)-1); free(expanded); }
            }
            const char *tc=json_str(json_get(cfg,"tls_cert"),NULL);
            if(tc) {
                char *expanded = expand_env_vars(tc);
                if (expanded) { strncpy(app->tls_cert_path,expanded,sizeof(app->tls_cert_path)-1); free(expanded); }
            }
            const char *tk=json_str(json_get(cfg,"tls_key"),NULL);
            if(tk) {
                char *expanded = expand_env_vars(tk);
                if (expanded) { strncpy(app->tls_key_path,expanded,sizeof(app->tls_key_path)-1); free(expanded); }
            }
            const char *cd=json_str(json_get(cfg,"connector_dir"),NULL);
            if(cd) {
                char *expanded = expand_env_vars(cd);
                if (expanded) { strncpy(app->plugins_dir,expanded,sizeof(app->plugins_dir)-1); free(expanded); }
            }
            /* RBAC */
            app->rbac_enabled = (bool)json_int(json_get(cfg,"rbac_enabled"), 0);
            /* audit */
            const char *alf = json_str(json_get(cfg,"audit_log_file"), NULL);
            if (alf) strncpy(app->audit_log_file, alf, sizeof(app->audit_log_file)-1);
            /* cluster */
            app->cluster_mode = (bool)json_int(json_get(cfg,"cluster_mode"), 0);
            /* Step 5: GitOps YAML pipelines */
            const char *pld = json_str(json_get(cfg,"pipelines_dir"), NULL);
            if (pld) {
                char *expanded = expand_env_vars(pld);
                if (expanded) { strncpy(app->pipelines_dir, expanded, sizeof(app->pipelines_dir)-1); free(expanded); }
            }
            /* SQL templates base directory */
            const char *std_ = json_str(json_get(cfg,"sql_templates_dir"), NULL);
            if (std_) {
                char *expanded = expand_env_vars(std_);
                if (expanded) { strncpy(app->sql_templates_dir, expanded, sizeof(app->sql_templates_dir)-1); free(expanded); }
            }
            /* Step 3 Week 1: PostgreSQL wire-protocol server */
            app->pgwire_port    = (int)json_int(json_get(cfg, "pgwire_port"), 0);
            app->pgwire_enabled = (bool)json_int(json_get(cfg, "pgwire_enabled"), 0);
            /* If port is set without enabled flag, infer enabled */
            if (app->pgwire_port > 0 && !app->pgwire_enabled) app->pgwire_enabled = true;
        }
        arena_destroy(a);
    }
    if (app->tls_cert_path[0] && app->tls_key_path[0]) {
        app->tls_enabled = true;
    }
    snprintf(app->db_path,sizeof(app->db_path),"%s/catalog.db",app->data_dir);

    /* create data dir */
    mkdir(app->data_dir, 0755);

    /* log_format: "json" or "text" (default text) */
    int json_mode = 0;
    if (config_json && *config_json) {
        Arena *lfa = arena_create(512);
        JVal *lcfg = json_parse(lfa, config_json, strlen(config_json));
        if (lcfg && lcfg->type == JV_OBJECT) {
            const char *lf = json_str(json_get(lcfg, "log_format"), NULL);
            if (lf && strcmp(lf, "json") == 0) json_mode = 1;
        }
        arena_destroy(lfa);
    }
    log_init(&g_log, stderr, LOG_INFO, json_mode);
    LOG_INFO("DataFlow OS starting — data_dir=%s port=%d", app->data_dir, app->port);

    /* subsystems */
    app->catalog  = catalog_open(app->db_path);
    app->auth_store = auth_store_create(app->db_path);
    if (!app->auth_store) {
        LOG_ERROR("failed to create auth store");
        exit(1);
    }
    if (strlen(app->jwt_secret) == 0) {
        /* Generate random secret as 64 hex chars (no null bytes in HMAC key) */
        uint8_t raw[32];
        FILE *f = fopen("/dev/urandom", "rb");
        if (!f || fread(raw, 1, sizeof(raw), f) != sizeof(raw)) {
            LOG_ERROR("failed to generate JWT secret");
            if (f) fclose(f);
            exit(1);
        }
        fclose(f);
        for (int i = 0; i < 32; i++)
            snprintf(app->jwt_secret + i*2, 3, "%02x", raw[i]);
        app->jwt_secret[64] = '\0';
        LOG_WARN("jwt_secret not set in config — generated random (tokens won't survive restart)");
    }
    if (strlen(app->admin_password) == 0) {
        strncpy(app->admin_password, "admin", sizeof(app->admin_password)-1);
        LOG_WARN("admin_password not set in config — using default 'admin'");
    }
    app->metrics  = calloc(1, sizeof(Metrics)); metrics_init(app->metrics);
    app->workers  = tp_create(WORKER_THREADS, 256);
    app->scheduler= scheduler_create(on_pipeline_run, app);
    app->txn_mgr  = txn_manager_create();

    /* RBAC */
    char rbac_db[512];
    snprintf(rbac_db, sizeof(rbac_db), "%s/rbac.db", app->data_dir);
    app->rbac = rbac_store_create(rbac_db, app->rbac_enabled);
    if (app->rbac) rbac_init_defaults(app->rbac);

    /* audit log */
    char audit_db[512];
    snprintf(audit_db, sizeof(audit_db), "%s/audit.db", app->data_dir);
    app->audit = audit_log_create(audit_db,
                     app->audit_log_file[0] ? app->audit_log_file : NULL);

    /* materialized views */
    app->matviews = mvs_create(app->catalog, app->data_dir);

    /* cluster / replication */
    if (app->cluster_mode) {
        char node_id[37] = "leader-0";
        app->replicator = replicator_create(true, node_id);
        LOG_INFO("cluster mode enabled, node_id=%s", node_id);
    }

    pthread_mutex_init(&app->tables_mu, NULL);
    pthread_mutex_init(&app->ws_mu, NULL);

    /* load tables from catalog */
    Arena *la=arena_create(16384);
    char **tnames; int tn;
    catalog_list_tables(app->catalog,&tnames,&tn,la);
    hm_init(&app->tables,NULL,64);
    for(int i=0;i<tn;i++){
        Table *t=table_open(tnames[i],app->data_dir);
        hm_set(&app->tables,tnames[i],t);
    }
    LOG_INFO("loaded %d tables from catalog", tn);

    /* hook WAL callback for replication on every loaded table */
    if (app->cluster_mode && app->replicator) {
        int idx = 0; const char *k; void *v;
        while ((idx = hm_next(&app->tables, idx, &k, &v)) >= 0)
            table_set_wal_callback((Table *)v, replicator_wal_cb, app->replicator);
        LOG_INFO("cluster: WAL callbacks registered on %d tables", tn);
    }

    /* load pipelines */
    char **pids; int pn;
    catalog_list_pipelines(app->catalog,&pids,&pn,la);
    for(int i=0;i<pn;i++){
        char *pjson=NULL;
        if(catalog_load_pipeline(app->catalog,pids[i],&pjson,la)==0){
            Pipeline p; memset(&p,0,sizeof(p));
            if(pipeline_from_json(&p,pjson)==0)
                scheduler_add(app->scheduler,&p);
        }
    }
    LOG_INFO("loaded %d pipelines", pn);

    /* Step 5: auto-load YAML pipelines from pipelines_dir.
     * If a YAML pipeline has the same `name` as one already loaded from the
     * catalog, the YAML version replaces it — files are the source of truth
     * in GitOps mode. */
    if (app->pipelines_dir[0]) {
        DIR *d = opendir(app->pipelines_dir);
        if (!d) {
            LOG_WARN("pipelines_dir '%s' cannot be opened: %s",
                     app->pipelines_dir, strerror(errno));
        } else {
            int yaml_loaded = 0, yaml_failed = 0;
            struct dirent *de;
            while ((de = readdir(d))) {
                size_t nlen = strlen(de->d_name);
                if (nlen < 5) continue;
                int is_yaml = (strcmp(de->d_name + nlen - 5, ".yaml") == 0) ||
                              (strcmp(de->d_name + nlen - 4, ".yml")  == 0);
                if (!is_yaml) continue;
                char path[1024];
                snprintf(path, sizeof(path), "%s/%s", app->pipelines_dir, de->d_name);
                FILE *f = fopen(path, "r");
                if (!f) { yaml_failed++; continue; }
                fseek(f, 0, SEEK_END); long sz = ftell(f); rewind(f);
                if (sz <= 0 || sz > 1024 * 1024) { fclose(f); yaml_failed++; continue; }
                char *src = malloc((size_t)sz + 1);
                if (!src) { fclose(f); yaml_failed++; continue; }
                fread(src, 1, (size_t)sz, f); src[sz] = '\0';
                fclose(f);

                Arena *ya = arena_create(64 * 1024);
                /* Expand {{ var }} placeholders + strip the vars: section so
                 * the YAML parser never sees it. NULL keys → only the file's
                 * own vars: block resolves. */
                char *expanded = yaml_expand_vars(ya, src, (size_t)sz, NULL, NULL, 0);
                if (!expanded) {
                    LOG_WARN("pipelines_dir: %s template expansion failed", de->d_name);
                    arena_destroy(ya); free(src); yaml_failed++; continue;
                }
                YamlError yerr = {0};
                char *json = NULL;
                if (yaml_to_json(expanded, strlen(expanded), ya, &json, &yerr) < 0) {
                    LOG_WARN("pipelines_dir: %s parse error line %d: %s",
                             de->d_name, yerr.line, yerr.buf);
                    arena_destroy(ya); free(src); yaml_failed++; continue;
                }
                Pipeline p; memset(&p, 0, sizeof(p));
                if (pipeline_from_json(&p, json) < 0) {
                    LOG_WARN("pipelines_dir: %s schema invalid", de->d_name);
                    arena_destroy(ya); free(src); yaml_failed++; continue;
                }
                /* Generate a stable id from the file name if not present */
                if (!p.id[0])
                    snprintf(p.id, sizeof(p.id), "yaml_%s", de->d_name);
                /* Replace any existing pipeline with the same id */
                scheduler_remove(app->scheduler, p.id);
                scheduler_add(app->scheduler, &p);
                yaml_loaded++;
                arena_destroy(ya);
                free(src);
            }
            closedir(d);
            LOG_INFO("pipelines_dir: loaded %d YAML pipeline(s) (%d failed) from %s",
                     yaml_loaded, yaml_failed, app->pipelines_dir);
        }
    }

    arena_destroy(la);

    /* register routes */
    api_register_routes(&app->router);
    app->router.userdata = app;

    /* create TLS context if enabled */
    TlsCtx *tls_ctx = NULL;
    if (app->tls_enabled && app->tls_cert_path[0] && app->tls_key_path[0]) {
        tls_ctx = tls_server_ctx_create(app->tls_cert_path, app->tls_key_path);
        if (!tls_ctx) {
            LOG_ERROR("failed to create TLS context");
            exit(1);
        }
    }

    /* create HTTP server (http.c handles HTTPS binding on :8443 + redirect on :port) */
    app->server = http_server_create(&app->router, app->port, 128, tls_ctx);

    /* start scheduler */
    scheduler_start(app->scheduler);

    /* Step 4: file_arrival trigger watcher.
     * Returns NULL if no file_arrival triggers exist or platform unsupported. */
    app->file_watcher = file_watcher_create(app->scheduler);

    /* Step 3 Week 1: PostgreSQL wire-protocol server (opt-in via config) */
    if (app->pgwire_enabled && app->pgwire_port > 0) {
        PgWireCallbacks cbs = {
            .authenticate = pg_authenticate_cb,
            .query        = pg_query_cb,
        };
        app->pgwire = pgwire_create(app->pgwire_port, cbs, app);
        if (app->pgwire && pgwire_start(app->pgwire) == 0) {
            LOG_INFO("PostgreSQL wire-protocol enabled on :%d (Week 1: handshake + simple queries only)",
                     app->pgwire_port);
        } else {
            LOG_WARN("pgwire: failed to start on :%d", app->pgwire_port);
            if (app->pgwire) { pgwire_destroy(app->pgwire); app->pgwire = NULL; }
        }
    }
}

void app_run(App *app) {
    LOG_INFO("DataFlow OS ready — http://localhost:%d", app->port);
    http_server_run(app->server);
}

void app_stop(App *app) {
    if (app->pgwire) { pgwire_destroy(app->pgwire); app->pgwire = NULL; }
    if (app->file_watcher) { file_watcher_destroy(app->file_watcher); app->file_watcher = NULL; }
    scheduler_stop(app->scheduler);
    http_server_stop(app->server);
    tp_destroy(app->workers);
    txn_manager_destroy(app->txn_mgr);
    if (app->replicator) replicator_destroy(app->replicator);
    mvs_destroy(app->matviews);
    audit_log_destroy(app->audit);
    rbac_store_destroy(app->rbac);
    catalog_close(app->catalog);
    LOG_INFO("DataFlow OS stopped");
}

static volatile sig_atomic_t g_shutdown = 0;

static void sig_handler(int sig) {
    (void)sig;
    g_shutdown = 1;
    http_server_stop(g_app.server); /* safe: sets volatile int */
}

int main(int argc, char **argv) {
    char *config_json = NULL;

    /* Separate passes for -c (file) and inline overrides (-p, -d) */
    int    cli_port = 0;
    char   cli_data_dir[512] = {0};
    char  *file_config = NULL;

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "-c") == 0 && i+1 < argc) {
            FILE *f = fopen(argv[++i], "r");
            if (!f) { fprintf(stderr, "can't open config '%s'\n", argv[i]); return 1; }
            fseek(f, 0, SEEK_END); long sz = ftell(f); rewind(f);
            file_config = malloc((size_t)sz + 1);
            fread(file_config, 1, (size_t)sz, f);
            file_config[sz] = '\0';
            fclose(f);
        } else if (strcmp(argv[i], "-p") == 0 && i+1 < argc) {
            cli_port = atoi(argv[++i]);
        } else if (strcmp(argv[i], "-d") == 0 && i+1 < argc) {
            strncpy(cli_data_dir, argv[++i], sizeof(cli_data_dir) - 1);
        }
    }

    if (file_config) {
        config_json = file_config;
    } else if (cli_port > 0 || cli_data_dir[0]) {
        /* Build inline JSON from CLI flags */
        char buf[800]; int off = 0;
        off += snprintf(buf, sizeof(buf), "{");
        bool first = true;
        if (cli_port > 0) {
            off += snprintf(buf + off, sizeof(buf) - (size_t)off,
                            "\"port\":%d", cli_port);
            first = false;
        }
        if (cli_data_dir[0]) {
            off += snprintf(buf + off, sizeof(buf) - (size_t)off,
                            "%s\"data_dir\":\"%s\"", first ? "" : ",", cli_data_dir);
        }
        snprintf(buf + off, sizeof(buf) - (size_t)off, "}");
        config_json = strdup(buf);
    }

    signal(SIGINT,  sig_handler);
    signal(SIGTERM, sig_handler);
    signal(SIGUSR1, SIG_IGN); /* graceful reload placeholder */

    app_init(&g_app, config_json);
    app_run(&g_app);  /* blocks until http_server_stop() */
    if (g_shutdown) {
        LOG_INFO("received signal, shutting down...");
        app_stop(&g_app);
    }
    return 0;
}
