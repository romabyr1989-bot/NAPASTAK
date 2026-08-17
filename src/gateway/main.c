/* gateway/main.c — точка входа NAPASTAK.
 * Поднимает все подсистемы (catalog, auth, scheduler, matviews, кластер),
 * HTTP/HTTPS-сервер и опциональный PostgreSQL wire-протокол, обрабатывает
 * CLI-аргументы, конфиг и graceful-shutdown по сигналам. */
#include "app.h"
#include "../../lib/core/log.h"
#include "../../lib/auth/auth.h"
#include "../../lib/core/json.h"
#include "../../lib/net/tls.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <signal.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/socket.h>
#ifndef MSG_NOSIGNAL
#define MSG_NOSIGNAL 0
#endif

/* Подставляет значения переменных окружения вида ${VAR} в строке.
 * Возвращает новую malloc'нутую строку (нужно free); буфер растёт по мере
 * необходимости. Неизвестные ${VAR} раскрываются в пустую строку. */
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


/* Булев ключ конфига. json_int понимает только числа и на JSON-булевом молча
 * отдаёт значение по умолчанию — из-за этого "auth_enabled": false
 * игнорировалось, и аутентификация оставалась включённой вопреки конфигу.
 * Принимаем обе записи: true/false и 1/0. */
static bool cfg_flag(JVal *v, bool def) {
    if (!v) return def;
    if (v->type == JV_BOOL)   return v->b;
    if (v->type == JV_NUMBER) return v->n != 0;
    return def;
}

/* Глобальный экземпляр приложения — нужен обработчику сигналов. */
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
    /* Шлём кадр всем; индексы клиентов с ошибкой записи копим для удаления */
    int dead[256]; int ndead=0;
    for(int i=0;i<app->nws_clients;i++){
        WsClient *wc = &app->ws_clients[i];
        ssize_t r = wc->tls
            ? tls_write(wc->tls, frame, (size_t)flen)
            : send(wc->fd, frame, (size_t)flen, MSG_NOSIGNAL);
        if(r<0) dead[ndead++]=i;
    }
    /* Удаляем мёртвых с конца: swap-remove не сдвигает ещё не обработанные */
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

/* Истина, если s начинается с prefix. */
static int starts_with(const char *s, const char *prefix) {
    return strncmp(s, prefix, strlen(prefix)) == 0;
}

/* Обработчик одиночного SQL-запроса по pgwire: сначала перехватываем
 * совместимостные пробы клиентов и txn-команды как no-op, остальное
 * отдаём реальному движку через api_pg_execute (см. блок выше). */
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
        const char *row[] = {"16.0 (NAPASTAK)"};
        pgwire_send_data_row(conn, 1, row);
        pgwire_send_command_complete(conn, "SHOW");
        return;
    }

    /* SELECT version() */
    if (strcmp(norm, "select version()") == 0) {
        PgColumn c[] = {{"version", PG_OID_TEXT, -1}};
        pgwire_send_row_description(conn, 1, c);
        const char *row[] = {"NAPASTAK 0.1 (PostgreSQL-compatible wire protocol)"};
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

/* Колбэк планировщика: запуск пайплайна по расписанию/триггеру.
 * Инкрементит метрику, шлёт WS-событие и выполняет шаги пайплайна. */
static void on_pipeline_run(Pipeline *p, void *ud) {
    App *app=(App*)ud;
    LOG_INFO("running pipeline %s (%s)", p->name, p->id);
    app->metrics->total_pipelines_run++;
    Arena *a=arena_create(256);
    app_ws_broadcast(app,arena_sprintf(a,"{\"event\":\"pipeline_triggered\",\"id\":\"%s\"}",p->id));
    arena_destroy(a);
    pipeline_execute_steps(p, app);
}

/* Datamart auto-refresh ticker — drives scheduled + on-write refresh by calling
 * mvs_tick() every ~15s. Kept as a tiny dedicated thread to avoid coupling the
 * generic scheduler to the matview store. */
#include <pthread.h>
#include <unistd.h>
static pthread_t   g_mv_ticker;
static volatile int g_mv_ticker_run = 0;
static void *mv_ticker_loop(void *arg) {
    App *app = (App *)arg;
    while (g_mv_ticker_run) {
        for (int i = 0; i < 15 && g_mv_ticker_run; i++) sleep(1);  /* responsive to stop */
        if (g_mv_ticker_run && app->matviews) mvs_tick(app->matviews, app);
    }
    return NULL;
}

/* Поток обслуживания постоянных подключений: поднимает сессии к системам из
 * справочника и держит их живыми, чтобы соединение существовало постоянно, а не
 * только на время запуска конвейера. Отдельный поток, потому что открытие
 * сессии к недоступной системе упирается в таймаут — старт гейтвея и работа
 * конвейеров из-за этого тормозить не должны. */
static pthread_t    g_conn_ticker;
static volatile int g_conn_ticker_run = 0;

static void *conn_ticker_loop(void *arg) {
    App *app = (App *)arg;
    (void)app;
    /* Небольшая пауза на старте: даём подсистемам подняться. */
    for (int i = 0; i < 3 && g_conn_ticker_run; i++) sleep(1);
    while (g_conn_ticker_run) {
        api_conn_pool_maintain();
        for (int i = 0; i < 20 && g_conn_ticker_run; i++) sleep(1);
    }
    return NULL;
}

/* Остановка потока прогрева с пределом ожидания. Обход может сидеть в коннекте
 * к недоступной системе (таймаут драйвера — секунды), и безусловный join
 * подвешивал бы завершение гейтвея. Не дождавшись, поток бросаем: процесс всё
 * равно завершается, а пул при остановке занятые сессии не трогает. */
static void conn_ticker_stop(void) {
    if (!g_conn_ticker_run) return;
    g_conn_ticker_run = 0;
    /* Ждём настоящим join с крайним сроком. Прежняя проба живости через
     * pthread_kill(tid, 0) не работала: у незаджойненного потока TID остаётся
     * валидным и после выхода из функции, поэтому она всегда возвращала 0,
     * цикл каждый раз выкручивал все 6 с и печатал предупреждение даже когда
     * поток завершился мгновенно. Заодно поток теперь пожинается, а не течёт. */
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    ts.tv_sec += 6;
    if (pthread_timedjoin_np(g_conn_ticker, NULL, &ts) != 0)
        LOG_WARN("поток обслуживания подключений не завершился за 6 с — не ждём дальше");
}

/* Инициализация приложения: дефолты, разбор JSON-конфига, создание всех
 * подсистем (catalog/auth/scheduler/matviews/RBAC/audit/cluster), загрузка
 * таблиц и пайплайнов из каталога, запуск HTTP-сервера,
 * matview-тикера, file-watcher и опционального pgwire. */
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
            app->auth_enabled = cfg_flag(json_get(cfg,"auth_enabled"), true);   /* принимает и true/false, и 1/0 */
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
            app->rbac_enabled = cfg_flag(json_get(cfg,"rbac_enabled"), false);
            /* audit */
            const char *alf = json_str(json_get(cfg,"audit_log_file"), NULL);
            if (alf) strncpy(app->audit_log_file, alf, sizeof(app->audit_log_file)-1);
            /* cluster */
            app->cluster_mode = cfg_flag(json_get(cfg,"cluster_mode"), false);
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
    /* db_path всегда выводится из data_dir, перекрывая дефолт/конфиг */
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
    LOG_INFO("NAPASTAK starting — data_dir=%s port=%d", app->data_dir, app->port);

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
    /* Пул постоянных сессий к системам-источникам/приёмникам. До 4 одновременных
     * экземпляров на подключение (контексты плагинов не потокобезопасны, а
     * конвейеры идут параллельно на воркерах); простаивающие пингуются раз в 60 с. */
    app->conn_pool = conn_pool_create(4, 60);
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
    api_connections_migrate();   /* привести справочник подключений к текущей модели */

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

    /* start datamart auto-refresh ticker.
     * Big stack (16 MB): the SQL engine (exec_stmt) uses deep/large frames; the
     * default 512 KB pthread stack overflows it (SIGBUS in __chkstk_darwin). */
    g_mv_ticker_run = 1;
    pthread_attr_t mv_attr;
    pthread_attr_init(&mv_attr);
    pthread_attr_setstacksize(&mv_attr, 16 * 1024 * 1024);
    pthread_create(&g_mv_ticker, &mv_attr, mv_ticker_loop, app);
    pthread_attr_destroy(&mv_attr);

    /* Поток постоянных подключений: прогрев сессий + keepalive.
     * Стек как у matview-тикера: внутри идут те же арены и JSON-разбор. */
    g_conn_ticker_run = 1;
    pthread_attr_t cp_attr;
    pthread_attr_init(&cp_attr);
    pthread_attr_setstacksize(&cp_attr, 8 * 1024 * 1024);
    pthread_create(&g_conn_ticker, &cp_attr, conn_ticker_loop, app);
    pthread_attr_destroy(&cp_attr);

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

/* Блокирующий запуск: отдаёт управление циклу HTTP-сервера. */
void app_run(App *app) {
    LOG_INFO("NAPASTAK ready — http://localhost:%d", app->port);
    http_server_run(app->server);
}

/* Корректное завершение: останавливает фоновые потоки/сервисы и закрывает
 * подсистемы в порядке, обратном их зависимостям. */
void app_stop(App *app) {
    if (app->pgwire) { pgwire_destroy(app->pgwire); app->pgwire = NULL; }
    if (app->file_watcher) { file_watcher_destroy(app->file_watcher); app->file_watcher = NULL; }
    if (g_mv_ticker_run) { g_mv_ticker_run = 0; pthread_join(g_mv_ticker, NULL); }
    conn_ticker_stop();
    scheduler_stop(app->scheduler);
    http_server_stop(app->server);
    tp_destroy(app->workers);
    txn_manager_destroy(app->txn_mgr);
    /* После остановки воркеров и тикеров: арендованных сессий уже нет. */
    if (app->conn_pool) { conn_pool_destroy(app->conn_pool); app->conn_pool = NULL; }
    if (app->replicator) replicator_destroy(app->replicator);
    mvs_destroy(app->matviews);
    audit_log_destroy(app->audit);
    rbac_store_destroy(app->rbac);
    catalog_close(app->catalog);
    LOG_INFO("NAPASTAK stopped");
}

static volatile sig_atomic_t g_shutdown = 0;

/* Обработчик SIGINT/SIGTERM: помечает shutdown и будит цикл сервера.
 * Делает только async-signal-safe операции. */
static void sig_handler(int sig) {
    (void)sig;
    g_shutdown = 1;
    http_server_stop(g_app.server); /* safe: sets volatile int */
}

/* Точка входа: разбор CLI (-c конфиг-файл, -p порт, -d data_dir),
 * установка обработчиков сигналов, init → run → (по сигналу) stop. */
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

    /* Файл конфига приоритетнее; иначе собираем JSON из CLI-флагов */
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
