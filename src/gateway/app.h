#pragma once
/* app.h — публичный интерфейс шлюза NAPASTAK.
 * Описывает агрегирующую структуру App (все подсистемы сервера: каталог,
 * планировщик, хранилище, auth/RBAC/audit, кластер, PG-wire, HTTP/TLS)
 * и объявляет жизненный цикл приложения + точки регистрации маршрутов. */
#include "../../lib/net/http.h"
#include "../../lib/storage/storage.h"
#include "../../lib/storage/txn.h"
#include "../../lib/scheduler/scheduler.h"
#include "../../lib/scheduler/file_watcher.h"
#include "../../lib/pgwire/pgwire.h"
#include "../../lib/observ/observ.h"
#include "../../lib/core/hashmap.h"
#include "../../lib/core/threadpool.h"
#include "../../lib/auth/auth.h"
#include "../../lib/auth/rbac.h"
#include "../../lib/auth/audit.h"
#include "../../lib/matview/matview.h"
#include "../../lib/cluster/replicator.h"
#include <pthread.h>

#define DATA_DIR_DEFAULT "./data"
#define DB_PATH_DEFAULT  "./data/catalog.db"
#define DEFAULT_PORT     8080
#define WORKER_THREADS   4

/* WebSocket client: plain WS (tls=NULL) or WSS */
typedef struct { int fd; TlsConn *tls; } WsClient;

/* App — единый контейнер состояния сервера: владеет всеми подсистемами,
 * конфигурацией и сетевыми объектами. Глобальный экземпляр — g_app. */
typedef struct {
    /* subsystems */
    Catalog    *catalog;
    Scheduler  *scheduler;
    FileWatcher *file_watcher;     /* Step 4: TRIGGER_FILE_ARRIVAL backend */
    Metrics    *metrics;
    ThreadPool *workers;

    /* in-memory table store: name → Table* */
    HashMap     tables;
    pthread_mutex_t tables_mu;

    /* WebSocket clients (plain WS + WSS) */
    WsClient    ws_clients[256];
    int         nws_clients;
    pthread_mutex_t ws_mu;

    /* config */
    char        data_dir[512];
    char        db_path[512];
    char        plugins_dir[512];  /* directory containing *_connector.so files */
    int         port;

    /* auth */
    AuthStore  *auth_store;
    char        jwt_secret[AUTH_JWT_SECRET_LEN + 1];
    bool        auth_enabled;
    char        admin_password[256];

    /* RBAC */
    RbacStore  *rbac;
    bool        rbac_enabled;

    /* audit log */
    AuditLog   *audit;
    char        audit_log_file[512];

    /* materialized views */
    MatViewStore *matviews;

    /* cluster / replication */
    Replicator  *replicator;
    bool         cluster_mode;

    /* Step 5: YAML pipelines auto-load (GitOps).
     * If non-empty, app_init scans this directory for *.yaml / *.yml at
     * startup and registers each as a pipeline. Empty = feature off. */
    char         pipelines_dir[512];

    /* Base directory for `sql_template` step paths (see docs/SQL_TEMPLATES.md).
     * Default "./sql". Templates are read at step execution time. */
    char         sql_templates_dir[512];

    /* Step 3 Week 1: PostgreSQL wire-protocol server (opt-in).
     * Enabled when pgwire_port > 0 in the config. Auth is cleartext —
     * fine for loopback or VPN; future weeks add SCRAM + TLS. */
    PgWireServer *pgwire;
    int           pgwire_port;
    bool          pgwire_enabled;

    /* TLS/HTTPS */
    char        tls_cert_path[512];
    char        tls_key_path[512];
    bool        tls_enabled;

    /* transactions */
    TxnManager *txn_mgr;

    /* server */
    Router      router;
    HttpServer *server;
} App;

/* Глобальный экземпляр приложения. */
extern App g_app;

/* Жизненный цикл сервера: разбор конфига и инициализация подсистем,
 * запуск цикла обслуживания, корректная остановка. */
void app_init(App *app, const char *config_json);
void app_run(App *app);
void app_stop(App *app);
/* Рассылка JSON-сообщения всем подключённым WebSocket-клиентам. */
void app_ws_broadcast(App *app, const char *json_msg);

/* route registration */
void api_register_routes(Router *r);

/* pipeline step execution (runs transform_sql → target_table for each step) */
void pipeline_execute_steps(Pipeline *p, App *app);

/* Step 3 Week 2: bridge a PG-wire query into the SQL engine.
 * Parses SQL via sql_parse, executes via exec_stmt, walks the RS,
 * and emits RowDescription / DataRow* / CommandComplete back to the
 * client via the pgwire helpers. ErrorResponse is sent on parse or
 * execution failure. Implementation lives in src/gateway/api.c. */
void api_pg_execute(PgConn *conn, const char *sql);
