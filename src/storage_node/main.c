#include "../../lib/cluster/proto.h"
#include "../../lib/storage/storage.h"
#include "../../lib/core/log.h"
#include "../../lib/core/arena.h"
#include "../../lib/core/hashmap.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <pthread.h>

static volatile sig_atomic_t g_shutdown = 0;
static volatile sig_atomic_t g_srv_fd   = -1;
static void sig_handler(int sig) {
    (void)sig;
    g_shutdown = 1;
    /* Belt-and-suspenders for Linux: shutdown() (async-signal-safe) wakes a
       blocked accept(). On macOS/BSD this is a no-op on a listening socket
       (ENOTCONN); there the real mechanism is blocking these signals in the
       client threads (see client_thread_fn) so main always gets the EINTR. */
    if (g_srv_fd >= 0) shutdown(g_srv_fd, SHUT_RDWR);
}

static char     g_data_dir[512] = "./data_node";
static Catalog *g_catalog       = NULL;

static HashMap          g_tables;
static pthread_mutex_t  g_tables_mu = PTHREAD_MUTEX_INITIALIZER;
static uint64_t         g_last_lsn  = 0;

typedef struct { int fd; } ClientCtx;

/* Get or open a table on the standby (create stub if needed) */
static Table *get_or_open_table(const char *name) {
    pthread_mutex_lock(&g_tables_mu);
    Table *t = (Table *)hm_get(&g_tables, name);
    if (!t) {
        t = table_open(name, g_data_dir);
        if (!t) {
            /* table doesn't exist yet — create with empty schema, will be rebuilt */
            t = table_create(name, NULL, g_data_dir);
        }
        if (t) hm_set(&g_tables, name, t);
    }
    pthread_mutex_unlock(&g_tables_mu);
    return t;
}

static void handle_replicate(int fd, ProtoHeader *hdr, void *body, size_t blen) {
    ProtoReplAckBody ack = { .lsn = 0, .result_code = -1 };

    if (!body || blen < sizeof(ProtoReplicateHdr)) {
        proto_send(fd, MSG_REPL_ACK, hdr->request_id, &ack, sizeof(ack));
        return;
    }

    ProtoReplicateHdr *rhdr = (ProtoReplicateHdr *)body;
    ack.lsn = rhdr->lsn;

    void   *wal_payload = (char *)body + sizeof(ProtoReplicateHdr);
    size_t  wal_len     = blen - sizeof(ProtoReplicateHdr);
    if (wal_len != rhdr->wal_payload_len) {
        LOG_WARN("storage_node: payload len mismatch: got=%zu expected=%u",
                 wal_len, rhdr->wal_payload_len);
        proto_send(fd, MSG_REPL_ACK, hdr->request_id, &ack, sizeof(ack));
        return;
    }

    Table *t = get_or_open_table(rhdr->table_name);
    if (!t) {
        LOG_ERROR("storage_node: can't open table '%s'", rhdr->table_name);
        proto_send(fd, MSG_REPL_ACK, hdr->request_id, &ack, sizeof(ack));
        return;
    }

    /* Apply WAL bytes — table_wal_append wraps them in TLV again, matching primary layout */
    int rc = table_wal_append(t, wal_payload, wal_len);
    if (rc == 0) {
        g_last_lsn = rhdr->lsn;
        ack.result_code = 0;
        LOG_INFO("storage_node: applied lsn=%llu table=%s bytes=%zu",
                 (unsigned long long)rhdr->lsn, rhdr->table_name, wal_len);
    } else {
        LOG_ERROR("storage_node: wal_append failed for table '%s'", rhdr->table_name);
    }

    proto_send(fd, MSG_REPL_ACK, hdr->request_id, &ack, sizeof(ack));
}

static void *client_thread_fn(void *arg) {
    /* Block SIGINT/SIGTERM here so they are always delivered to the main
       thread, whose accept() (installed without SA_RESTART) then returns
       EINTR and lets the shutdown loop exit. Without this a client thread
       blocked in recv() can swallow the signal and the node ignores it. */
    sigset_t block;
    sigemptyset(&block);
    sigaddset(&block, SIGINT);
    sigaddset(&block, SIGTERM);
    pthread_sigmask(SIG_BLOCK, &block, NULL);

    ClientCtx *ctx = (ClientCtx *)arg;
    int fd = ctx->fd;
    free(ctx);
    LOG_INFO("storage_node: client connected fd=%d", fd);

    while (!g_shutdown) {
        ProtoHeader hdr;
        void *body = NULL; size_t blen = 0;
        if (proto_recv(fd, &hdr, &body, &blen) < 0) break;

        switch ((ProtoMsgType)hdr.msg_type) {
        case MSG_PING:
            proto_send(fd, MSG_PONG, hdr.request_id, NULL, 0);
            break;
        case MSG_REPLICATE:
            handle_replicate(fd, &hdr, body, blen);
            break;
        case MSG_STATUS_REQ: {
            ProtoStatusBody sb;
            memset(&sb, 0, sizeof(sb));
            sb.is_leader     = 0;
            sb.wal_offset    = g_last_lsn;
            sb.replica_count = 0;
            proto_send(fd, MSG_STATUS_RESP, hdr.request_id, &sb, sizeof(sb));
            break;
        }
        default:
            LOG_WARN("storage_node: unknown msg_type=%d", hdr.msg_type);
            break;
        }
        proto_free_body(body);
    }
    close(fd);
    return NULL;
}

int main(int argc, char **argv) {
    int port = 9090;
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "-p") == 0 && i + 1 < argc) port = atoi(argv[++i]);
        if (strcmp(argv[i], "-d") == 0 && i + 1 < argc)
            strncpy(g_data_dir, argv[++i], sizeof(g_data_dir) - 1);
    }

    log_init(&g_log, stderr, LOG_INFO, 0);
    LOG_INFO("storage_node starting — port=%d data_dir=%s", port, g_data_dir);

    /* No SA_RESTART: SIGINT/SIGTERM must interrupt the blocking accept()
       (EINTR) so the while(!g_shutdown) loop can re-check and exit. On
       macOS/BSD plain signal() installs handlers with SA_RESTART, which
       auto-restarts accept() and makes the node ignore SIGTERM. */
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = sig_handler;
    sa.sa_flags   = 0;
    sigaction(SIGINT,  &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);
    signal(SIGPIPE, SIG_IGN);

    char db_path[512];
    snprintf(db_path, sizeof(db_path), "%s/catalog.db", g_data_dir);
    mkdir(g_data_dir, 0755);
    g_catalog = catalog_open(db_path);
    hm_init(&g_tables, NULL, 32);

    int srv_fd = socket(AF_INET, SOCK_STREAM, 0);
    g_srv_fd = srv_fd;
    int opt = 1;
    setsockopt(srv_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port        = htons((uint16_t)port);

    if (bind(srv_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        LOG_ERROR("storage_node: bind failed on port %d", port);
        return 1;
    }
    listen(srv_fd, 16);
    LOG_INFO("storage_node listening on :%d", port);

    while (!g_shutdown) {
        struct sockaddr_in peer;
        socklen_t plen = sizeof(peer);
        int cfd = accept(srv_fd, (struct sockaddr *)&peer, &plen);
        if (cfd < 0) continue;
        ClientCtx *ctx = malloc(sizeof(ClientCtx));
        ctx->fd = cfd;
        pthread_t t;
        pthread_create(&t, NULL, client_thread_fn, ctx);
        pthread_detach(t);
    }

    close(srv_fd);
    catalog_close(g_catalog);
    LOG_INFO("storage_node stopped");
    return 0;
}
