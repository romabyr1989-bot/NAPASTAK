/*
 * http.c — встроенный HTTP/1.1 сервер шлюза DataFlow OS.
 *
 * Содержит: построение ответов, роутер с матчингом :path-параметров и
 * auth/audit/metrics-мидлварями, ручной парсер запросов, опциональный TLS,
 * апгрейд до WebSocket (RFC 6455 с собственными SHA-1/base64), HTTP→HTTPS
 * редирект и цикл событий на kqueue (Apple) или epoll (Linux).
 * Также предоставляет исходящий клиент http_post_json() поверх libcurl.
 */
#include "http.h"
#include "../core/log.h"
#include "../observ/observ.h"
#include "../auth/auth.h"
#include "../../src/gateway/app.h"
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netinet/tcp.h>
#include <signal.h>
#include <curl/curl.h>
#include <time.h>

/* Монотонные миллисекунды — для измерения длительности обработки запроса. */
static int64_t clock_monotonic_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}
#ifdef __APPLE__
#  include <sys/event.h>
#  include <sys/time.h>
#else
#  include <sys/epoll.h>
#endif
#include <stdarg.h>

/* ── Response helpers ── */
void http_resp_json(HttpResp *r, int status, const char *json) {
    r->status = status; r->content_type = "application/json";
    r->body = json; r->body_len = json ? strlen(json) : 0;
}
void http_resp_text(HttpResp *r, int status, const char *text) {
    r->status = status; r->content_type = "text/plain";
    r->body = text; r->body_len = text ? strlen(text) : 0;
}
void http_resp_error(HttpResp *r, int status, const char *msg) {
    static _Thread_local char buf[512];
    snprintf(buf, sizeof(buf), "{\"error\":\"%s\"}", msg ? msg : "internal error");
    http_resp_json(r, status, buf);
}

/* Текст статуса для строки ответа; неизвестные коды трактуются как 200 OK. */
static const char *http_status_text(int code) {
    switch (code) {
        case 200: return "OK";
        case 201: return "Created";
        case 204: return "No Content";
        case 400: return "Bad Request";
        case 401: return "Unauthorized";
        case 403: return "Forbidden";
        case 404: return "Not Found";
        case 409: return "Conflict";
        case 500: return "Internal Server Error";
        default:  return "OK";
    }
}

/* ── Router ── */
/* Регистрирует маршрут (метод + шаблон пути) с его обработчиком. */
void router_add(Router *r, const char *method, const char *pattern, HttpHandler h) {
    if (r->nroutes >= HTTP_MAX_ROUTES) { LOG_ERROR("too many routes"); return; }
    Route *rt = &r->routes[r->nroutes++];
    strncpy(rt->method, method, sizeof(rt->method)-1);
    strncpy(rt->pattern, pattern, sizeof(rt->pattern)-1);
    rt->handler = h;
}

/* Сопоставляет шаблон с путём; сегменты вида :name извлекаются в params.
 * Возвращает true только при полном совпадении (с учётом хвостовых '/'). */
static bool route_match(const char *pattern, const char *path, HashMap *params) {
    /* simple segment match: /api/:id/foo */
    const char *p = pattern, *q = path;
    while (*p && *q) {
        if (*p == ':') {
            /* capture param */
            const char *pname_start = ++p;
            while (*p && *p != '/') p++;
            char pname[64]; size_t plen = (size_t)(p - pname_start);
            if (plen >= sizeof(pname)) return false;
            memcpy(pname, pname_start, plen); pname[plen] = '\0';
            const char *vstart = q;
            while (*q && *q != '/') q++;
            if (params) {
                size_t vlen = (size_t)(q - vstart);
                char *val = params->arena
                    ? arena_strndup(params->arena, vstart, vlen)
                    : malloc(vlen + 1);
                if (!params->arena && val) { memcpy(val, vstart, vlen); val[vlen] = '\0'; }
                if (val) hm_set(params, pname, val);
            }
        } else {
            if (*p != *q) return false;
            p++; q++;
        }
    }
    /* skip trailing slashes */
    while (*p == '/') p++;
    while (*q == '/') q++;
    return *p == '\0' && *q == '\0';
}

/* Находит первый подходящий маршрут, прогоняет auth-мидлварь и вызывает
 * обработчик. По умолчанию (нет совпадений) отвечает 404. */
void router_dispatch(Router *r, HttpReq *req, HttpResp *resp) {
    resp->status = 404;
    resp->content_type = "application/json";
    resp->body = "{\"error\":\"not found\"}";
    resp->body_len = strlen(resp->body);

    /* Extract path without query */
    char path[1024]; strncpy(path, req->path, sizeof(path)-1); path[sizeof(path)-1]='\0';
    char *qs = strchr(path, '?');
    if (qs) { req->query = qs + 1; *qs = '\0'; }

    for (int i = 0; i < r->nroutes; i++) {
        Route *rt = &r->routes[i];
        if (strcmp(rt->method, req->method) != 0 &&
            strcmp(rt->method, "*") != 0) continue;
        hm_init(&req->params, req->arena, 8);
        if (route_match(rt->pattern, path, &req->params)) {
            // Middleware: auth check
            App *app = (App *)r->userdata;
            req->auth.role = ROLE_VIEWER;  // default
            req->auth_ok = false;
            static const char *PUBLIC_PATHS[] = {
                "/",           /* UI index.html */
                "/style.css",
                "/app.js",
                "/api/auth/login",
                "/api/auth/token",
                "/metrics",    /* Prometheus scraper не передаёт Authorization */
                "/health",
                "/api/triggers/:token", /* webhook trigger — token IS the auth */
                NULL
            };
            bool is_public = false;
            for (const char **pp = PUBLIC_PATHS; *pp; pp++) {
                if (strcmp(rt->pattern, *pp) == 0) {
                    is_public = true;
                    break;
                }
            }
            if (app && app->auth_enabled && !is_public) {
                int auth_result = auth_check_request(app->auth_store, app->jwt_secret, (void*)req, &req->auth);
                if (auth_result == 0) {
                    req->auth_ok = true;
                } else {
                    LOG_WARN("unauthorized access to %s", path);
                    http_resp_error(resp, 401, "unauthorized");
                    return;
                }
            } else {
                req->auth_ok = true;  // public or auth disabled
            }
            rt->handler(req, resp);
            return;
        }
    }
}

/* ── HTTP/1.1 parser ── */
/* Разбирает сырой буфер в HttpReq: строка запроса, заголовки (ключи в
 * нижнем регистре), тело. Всё выделяется из арены a. Возвращает -1 при
 * повреждённом запросе. */
static int parse_request(Arena *a, char *buf, size_t len, HttpReq *req) {
    char *p = buf, *end = buf + len;
    /* method */
    char *sp = memchr(p, ' ', (size_t)(end-p)); if (!sp) return -1;
    req->method = arena_strndup(a, p, (size_t)(sp-p)); p = sp+1;
    /* path */
    sp = memchr(p, ' ', (size_t)(end-p)); if (!sp) return -1;
    req->path   = arena_strndup(a, p, (size_t)(sp-p)); p = sp+1;
    /* skip HTTP/1.x\r\n */
    char *nl = memchr(p, '\n', (size_t)(end-p)); if (!nl) return -1; p = nl+1;
    /* headers */
    hm_init(&req->headers, a, 16);
    while (p < end) {
        if (*p == '\r' || *p == '\n') { p++; if (p < end && *p == '\n') p++; break; }
        char *colon = memchr(p, ':', (size_t)(end-p)); if (!colon) break;
        char *hname = arena_strndup(a, p, (size_t)(colon-p));
        /* lowercase key */
        for (char *c = hname; *c; c++) if (*c>='A'&&*c<='Z') *c += 32;
        p = colon+1; while (p < end && *p==' ') p++;
        nl = memchr(p, '\n', (size_t)(end-p)); if (!nl) break;
        size_t vlen = (size_t)(nl - p); if (vlen > 0 && p[vlen-1]=='\r') vlen--;
        char *hval = arena_strndup(a, p, vlen);
        hm_set(&req->headers, hname, hval);
        p = nl+1;
    }
    req->body = p; req->body_len = (size_t)(end - p);
    /* check WebSocket upgrade */
    char *upg = hm_get(&req->headers, "upgrade");
    if (upg && strcasecmp(upg, "websocket")==0) req->upgrade_ws = true;
    return 0;
}

/* Сериализует и отправляет ответ по plain-сокету (без TLS):
 * заголовки + тело. MSG_NOSIGNAL подавляет SIGPIPE на разорванном соединении. */
static void send_response(int fd, HttpResp *resp) {
    char header[640];
    const char *ct = resp->content_type ? resp->content_type : "application/octet-stream";
    int hlen;
    if (resp->correlation_id[0]) {
        hlen = snprintf(header, sizeof(header),
            "HTTP/1.1 %d %s\r\n"
            "Content-Type: %s\r\n"
            "Content-Length: %zu\r\n"
            "Access-Control-Allow-Origin: *\r\n"
            "Cache-Control: no-cache, no-store, must-revalidate\r\n"
            "Connection: keep-alive\r\n"
            "X-Correlation-Id: %s\r\n"
            "\r\n",
            resp->status, http_status_text(resp->status), ct,
            resp->body_len, resp->correlation_id);
    } else {
        hlen = snprintf(header, sizeof(header),
            "HTTP/1.1 %d %s\r\n"
            "Content-Type: %s\r\n"
            "Content-Length: %zu\r\n"
            "Access-Control-Allow-Origin: *\r\n"
            "Cache-Control: no-cache, no-store, must-revalidate\r\n"
            "Connection: keep-alive\r\n"
            "\r\n",
            resp->status, http_status_text(resp->status), ct, resp->body_len);
    }
#ifdef MSG_NOSIGNAL
    send(fd, header, (size_t)hlen, MSG_NOSIGNAL);
    if (resp->body && resp->body_len > 0)
        send(fd, resp->body, resp->body_len, MSG_NOSIGNAL);
#else
    send(fd, header, (size_t)hlen, 0);
    if (resp->body && resp->body_len > 0)
        send(fd, resp->body, resp->body_len, 0);
#endif
}

/* Исходящий POST с JSON-телом через libcurl (например, вебхуки/коллбэки).
 * Возвращает 0 при HTTP 2xx, иначе -1. */
int http_post_json(const char *url, const char *body, int timeout_ms) {
    if (!url) return -1;
    CURL *curl = curl_easy_init();
    if (!curl) return -1;

    struct curl_slist *headers = NULL;
    headers = curl_slist_append(headers, "Content-Type: application/json");
    headers = curl_slist_append(headers, "Accept: application/json");

    curl_easy_setopt(curl, CURLOPT_URL, url);
    curl_easy_setopt(curl, CURLOPT_POST, 1L);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, body ? body : "{}");
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, timeout_ms);
    curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 1L);
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 2L);

    long http_code = 0;
    CURLcode res = curl_easy_perform(curl);
    if (res == CURLE_OK) {
        curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
    } else {
        LOG_WARN("http_post_json curl failed: %s", curl_easy_strerror(res));
    }

    curl_slist_free_all(headers);
    curl_easy_cleanup(curl);

    return (res == CURLE_OK && http_code >= 200 && http_code < 300) ? 0 : -1;
}

#include <stdint.h>

/* Helper: compress one 64-byte block into hash state */
static void sha1_compress(uint32_t h[5], const uint8_t blk[64]) {
    uint32_t w[80];
    for (int i = 0; i < 16; i++)
        w[i] = ((uint32_t)blk[i*4]<<24)|((uint32_t)blk[i*4+1]<<16)|
               ((uint32_t)blk[i*4+2]<<8)|(uint32_t)blk[i*4+3];
    for (int i=16;i<80;i++){
        uint32_t x=w[i-3]^w[i-8]^w[i-14]^w[i-16];
        w[i]=(x<<1)|(x>>31);
    }
    uint32_t a=h[0],b=h[1],c=h[2],d=h[3],e=h[4];
    #define ROL(v,n) (((v)<<(n))|((v)>>(32-(n))))
    for (int i=0;i<80;i++){
        uint32_t f,k;
        if(i<20){f=(b&c)|(~b&d);k=0x5A827999;}
        else if(i<40){f=b^c^d;k=0x6ED9EBA1;}
        else if(i<60){f=(b&c)|(b&d)|(c&d);k=0x8F1BBCDC;}
        else{f=b^c^d;k=0xCA62C1D6;}
        uint32_t tmp=ROL(a,5)+f+e+k+w[i]; e=d;d=c;c=ROL(b,30);b=a;a=tmp;
    }
    #undef ROL
    h[0]+=a;h[1]+=b;h[2]+=c;h[3]+=d;h[4]+=e;
}

/* Полный SHA-1 произвольного сообщения (для Sec-WebSocket-Accept).
 * Делает padding по RFC 3174 и выдаёт 20-байтовый дайджест. */
static void sha1(const uint8_t *msg, size_t len, uint8_t out[20]) {
    uint32_t h[5] = {0x67452301,0xEFCDAB89,0x98BADCFE,0x10325476,0xC3D2E1F0};
    uint8_t blk[64];
    uint64_t bits = (uint64_t)len * 8;

    /* process full 64-byte blocks */
    size_t off = 0;
    for (; off + 64 <= len; off += 64) {
        memcpy(blk, msg + off, 64);
        sha1_compress(h, blk);
    }

    /* last (partial) block: copy remaining bytes + 0x80 padding */
    size_t rem = len - off;
    memset(blk, 0, 64);
    memcpy(blk, msg + off, rem);
    blk[rem] = 0x80;

    if (rem >= 56) {
        /* no room for length in this block — need an extra block */
        sha1_compress(h, blk);
        memset(blk, 0, 64);
    }

    /* append 64-bit big-endian bit length */
    for (int i = 0; i < 8; i++) blk[56+i] = (uint8_t)(bits >> (56 - 8*i));
    sha1_compress(h, blk);
    for (int i=0;i<5;i++){out[i*4]=(uint8_t)(h[i]>>24);out[i*4+1]=(uint8_t)(h[i]>>16);
                           out[i*4+2]=(uint8_t)(h[i]>>8);out[i*4+3]=(uint8_t)h[i];}
}

/* Стандартный base64-энкодер (с '=' padding); out должен вмещать терминатор. */
static void base64_enc(const uint8_t *in, size_t n, char *out) {
    static const char t[]="ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    size_t i=0,j=0;
    for(;i+2<n;i+=3){
        out[j++]=t[in[i]>>2]; out[j++]=t[((in[i]&3)<<4)|(in[i+1]>>4)];
        out[j++]=t[((in[i+1]&0xf)<<2)|(in[i+2]>>6)]; out[j++]=t[in[i+2]&0x3f];
    }
    if(i<n){out[j++]=t[in[i]>>2];
        if(i+1<n){out[j++]=t[((in[i]&3)<<4)|(in[i+1]>>4)];out[j++]=t[(in[i+1]&0xf)<<2];}
        else{out[j++]=t[(in[i]&3)<<4];out[j++]='=';}
        out[j++]='=';
    }
    out[j]='\0';
}

/* Отвечает на запрос апгрейда WebSocket: вычисляет Sec-WebSocket-Accept
 * (SHA-1(key+GUID) → base64) и шлёт 101 Switching Protocols. */
static void ws_handshake(int fd, TlsConn *tls, const char *key) {
    /* RFC 6455 magic GUID */
    const char *magic = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
    char accept_src[128]; snprintf(accept_src, sizeof(accept_src), "%s%s", key, magic);
    uint8_t sha[20]; sha1((uint8_t*)accept_src, strlen(accept_src), sha);
    char b64[64]; base64_enc(sha, 20, b64);
    char resp[512];
    int n = snprintf(resp, sizeof(resp),
        "HTTP/1.1 101 Switching Protocols\r\n"
        "Upgrade: websocket\r\n"
        "Connection: Upgrade\r\n"
        "Sec-WebSocket-Accept: %s\r\n\r\n", b64);
    if (tls) {
        tls_write(tls, resp, (size_t)n);
    } else {
#ifdef MSG_NOSIGNAL
        send(fd, resp, (size_t)n, MSG_NOSIGNAL);
#else
        send(fd, resp, (size_t)n, 0);
#endif
    }
}

/* ── HTTP→HTTPS redirect handler ── */
static void handle_redirect(int fd, int https_port) {
    char buf[2048]; ssize_t n = recv(fd, buf, sizeof(buf)-1, 0);
    char path[1024] = "/";
    if (n > 0) { buf[n] = '\0'; sscanf(buf, "%*s %1023s", path); }
    char resp[2048];
    int rlen = snprintf(resp, sizeof(resp),
        "HTTP/1.1 301 Moved Permanently\r\n"
        "Location: https://localhost:%d%s\r\n"
        "Content-Length: 0\r\nConnection: close\r\n\r\n",
        https_port, path);
    send(fd, resp, (size_t)rlen, MSG_NOSIGNAL);
    close(fd);
}

/* ── Connection handler ── */
struct HttpServer {
    Router  *router;
    int      port, backlog, epfd, listenfd, running;
    TlsCtx  *tls_ctx;
    int      redirect_fd;   /* HTTP→HTTPS plain listener (-1 if unused) */
    int      https_port;    /* actual HTTPS port (listenfd) */
    _Atomic int active_conns; /* in-flight connection threads (bounds fan-out) */
};

/* Обрабатывает одно соединение целиком (блокирующе): опциональный TLS,
 * чтение запроса до Content-Length, парсинг, мидлвари и отправка ответа.
 * Для WebSocket-апгрейда fd передаётся в App.ws_clients и не закрывается. */
static void handle_conn(HttpServer *srv, int fd) {
    enum { MAX_REQ_SIZE = 256 * 1024 * 1024 };
    size_t cap = 131072, used = 0;
    char *buf = malloc(cap + 1);
    if (!buf) { close(fd); return; }

    /* TLS handshake if enabled */
    TlsConn *tls = NULL;
    if (srv->tls_ctx) {
        tls = tls_conn_accept(srv->tls_ctx, fd);
        if (!tls) {
            LOG_WARN("TLS handshake failed for fd %d", fd);
            free(buf);
            close(fd);
            return;
        }
    }

    size_t want_total = 0;
    bool header_done = false;
    while (1) {
        if (used == cap) {
            if (cap >= MAX_REQ_SIZE) { 
                if (tls) tls_conn_destroy(tls);
                free(buf); close(fd); return; 
            }
            size_t ncap = cap * 2;
            if (ncap > MAX_REQ_SIZE) ncap = MAX_REQ_SIZE;
            char *nb = realloc(buf, ncap + 1);
            if (!nb) { 
                if (tls) tls_conn_destroy(tls);
                free(buf); close(fd); return; 
            }
            buf = nb; cap = ncap;
        }

        ssize_t n;
        if (tls) {
            n = tls_read(tls, buf + used, cap - used);
        } else {
            n = recv(fd, buf + used, cap - used, 0);
        }
        if (n <= 0) { 
            if (tls) tls_conn_destroy(tls);
            free(buf); close(fd); return; 
        }
        used += (size_t)n;
        buf[used] = '\0';

        if (!header_done) {
            char *he = strstr(buf, "\r\n\r\n");
            if (!he) he = strstr(buf, "\n\n");
            if (he) {
                header_done = true;
                size_t header_len = (size_t)(he - buf) + (he[0] == '\r' ? 4 : 2);
                size_t cl = 0;
                /* case-insensitive search for content-length header */
                char *p = NULL;
                for (char *s = buf; s < buf + used - 16; s++) {
                    if (*s == '\n' && strncasecmp(s + 1, "content-length:", 15) == 0)
                        { p = s; break; }
                }
                /* заголовок в самой первой строке: p+16 ниже укажет на значение */
                if (!p && strncasecmp(buf, "content-length:", 15) == 0) p = buf - 1;
                if (p) cl = (size_t)strtoull(p + 16, NULL, 10);
                want_total = header_len + cl;
                if (want_total > MAX_REQ_SIZE) { 
                    if (tls) tls_conn_destroy(tls);
                    free(buf); close(fd); return; 
                }
            }
        }
        if (header_done && used >= want_total) break;
        if (header_done && want_total == 0) break;
    }

    Arena *a = arena_create(32768);
    HttpReq req = {0}; req.arena = a; req.fd = fd;

    if (parse_request(a, buf, used, &req) < 0) {
        if (tls) tls_conn_destroy(tls);
        free(buf); close(fd); arena_destroy(a); return;
    }

    /* Correlation ID: echo from client or generate fresh UUID4 */
    char *cid_hdr = hm_get(&req.headers, "x-correlation-id");
    if (cid_hdr && *cid_hdr) {
        log_set_correlation_id(cid_hdr);
    } else {
        log_new_correlation_id();
    }
    strncpy(req.correlation_id, g_correlation_id, 36);
    req.correlation_id[36] = '\0';

    /* X-Txn-Id header for future transaction support (Step 2) */
    char *txn_hdr = hm_get(&req.headers, "x-txn-id");
    if (txn_hdr && *txn_hdr) {
        req.txn_id = (uint64_t)strtoull(txn_hdr, NULL, 10);
    }

    /* OPTIONS preflight */
    if (strcmp(req.method,"OPTIONS")==0) {
        char hdrs[256];
        snprintf(hdrs, sizeof(hdrs),
            "HTTP/1.1 204 No Content\r\n"
            "Access-Control-Allow-Origin: *\r\n"
            "Access-Control-Allow-Methods: GET,POST,DELETE,PUT\r\n"
            "Access-Control-Allow-Headers: Content-Type,Authorization,X-Correlation-Id,X-Txn-Id\r\n"
            "X-Correlation-Id: %s\r\n\r\n", req.correlation_id);
        if (tls) {
            tls_write(tls, hdrs, strlen(hdrs));
        } else {
            send(fd, hdrs, strlen(hdrs), MSG_NOSIGNAL);
        }
        if (tls) tls_conn_destroy(tls);
        free(buf); arena_destroy(a); close(fd); return;
    }

    if (req.upgrade_ws) {
        char *key = hm_get(&req.headers, "sec-websocket-key");
        if (key) ws_handshake(fd, tls, key);
        /* Register in App's ws_clients so broadcast can reach this client */
        App *ws_app = (App *)srv->router->userdata;
        if (ws_app) {
            pthread_mutex_lock(&ws_app->ws_mu);
            if (ws_app->nws_clients < 256) {
                ws_app->ws_clients[ws_app->nws_clients].fd  = fd;
                ws_app->ws_clients[ws_app->nws_clients].tls = tls;
                ws_app->nws_clients++;
                tls = NULL;  /* ownership transferred to ws_clients */
            }
            pthread_mutex_unlock(&ws_app->ws_mu);
        }
        if (tls) tls_conn_destroy(tls);
        free(buf);
        arena_destroy(a);
        /* fd stays open — owned by ws_clients until client disconnects */
        return;
    }

    HttpResp resp = {0};
    strncpy(resp.correlation_id, req.correlation_id, 36);

    /* Extract client IP for audit */
    char client_ip[64] = {0};
    {
        struct sockaddr_storage _peer;
        socklen_t _plen = sizeof(_peer);
        if (getpeername(fd, (struct sockaddr *)&_peer, &_plen) == 0) {
            if (_peer.ss_family == AF_INET)
                inet_ntop(AF_INET,
                    &((struct sockaddr_in *)&_peer)->sin_addr, client_ip, sizeof(client_ip));
            else if (_peer.ss_family == AF_INET6)
                inet_ntop(AF_INET6,
                    &((struct sockaddr_in6 *)&_peer)->sin6_addr, client_ip, sizeof(client_ip));
        }
    }

    int64_t t_start = clock_monotonic_ms();
    router_dispatch(srv->router, &req, &resp);
    int64_t elapsed = clock_monotonic_ms() - t_start;

    /* Audit middleware: log significant events */
    App *audit_app = (App *)srv->router->userdata;
    if (audit_app && audit_app->audit) {
        const char *path = req.path ? req.path : "";
        AuditEventType atype = 0;
        if (strncmp(path, "/api/tables/query", 17) == 0)         atype = AUDIT_QUERY;
        else if (strncmp(path, "/api/ingest/", 12) == 0)         atype = AUDIT_INGEST;
        else if (strstr(path, "/run") || strstr(path, "/pipelines/")) atype = AUDIT_PIPELINE_RUN;
        else if (strncmp(path, "/api/auth/token", 15) == 0)      atype = AUDIT_AUTH_LOGIN;
        else if (strncmp(path, "/api/rbac/", 10) == 0)           atype = AUDIT_POLICY_CHANGE;
        else if (strncmp(path, "/api/tables", 11) == 0 &&
                 (strcmp(req.method,"POST")==0||strcmp(req.method,"DELETE")==0))
            atype = AUDIT_SCHEMA_CHANGE;

        if (atype != 0) {
            /* extract resource name: /api/tables/X → X */
            char res_buf[128] = {0};
            const char *rp = strstr(path, "/api/tables/");
            if (rp) {
                rp += strlen("/api/tables/");
                const char *re = strchr(rp, '/');
                size_t rn = re ? (size_t)(re - rp) : strlen(rp);
                if (rn >= sizeof(res_buf)) rn = sizeof(res_buf)-1;
                memcpy(res_buf, rp, rn);
            }
            /* truncate body to 512 bytes for action_detail */
            char detail[513] = {0};
            if (req.body && req.body_len > 0) {
                size_t dn = req.body_len < 512 ? req.body_len : 512;
                memcpy(detail, req.body, dn);
            }
            AuditEvent aev = {
                .type           = atype,
                .user_id        = req.auth.user_id[0] ? req.auth.user_id : "anonymous",
                .role           = req.auth.role,
                .resource       = res_buf,
                .action_detail  = detail,
                .correlation_id = req.correlation_id,
                .client_ip      = client_ip,
                .result_code    = resp.status,
                .duration_ms    = elapsed,
            };
            audit_log_event(audit_app->audit, &aev);
        }
    }

    /* HTTP metrics */
    App *metrics_app = (App *)srv->router->userdata;
    if (metrics_app && metrics_app->metrics) {
        Metrics *m = metrics_app->metrics;
        __atomic_fetch_add(&m->http_requests_total, 1, __ATOMIC_RELAXED);
        if (resp.status >= 500)
            __atomic_fetch_add(&m->http_errors_5xx, 1, __ATOMIC_RELAXED);
        else if (resp.status >= 400)
            __atomic_fetch_add(&m->http_errors_4xx, 1, __ATOMIC_RELAXED);
        metrics_push(&m->http_request_duration_ms, (double)elapsed);
    }

    if (tls) {
        /* Build response and send via TLS */
        char resp_buf[16384];
        if (resp.correlation_id[0]) {
            snprintf(resp_buf, sizeof(resp_buf),
                "HTTP/1.1 %d %s\r\nContent-Type: %s\r\nContent-Length: %zu\r\n"
                "Access-Control-Allow-Origin: *\r\nConnection: keep-alive\r\n"
                "X-Correlation-Id: %s\r\n\r\n",
                resp.status, http_status_text(resp.status),
                resp.content_type, resp.body_len, resp.correlation_id);
        } else {
            snprintf(resp_buf, sizeof(resp_buf),
                "HTTP/1.1 %d %s\r\nContent-Type: %s\r\nContent-Length: %zu\r\n"
                "Access-Control-Allow-Origin: *\r\nConnection: keep-alive\r\n\r\n",
                resp.status, http_status_text(resp.status),
                resp.content_type, resp.body_len);
        }
        tls_write(tls, resp_buf, strlen(resp_buf));
        if (resp.body_len > 0) {
            tls_write(tls, resp.body, resp.body_len);
        }
        tls_conn_destroy(tls);
    } else {
        send_response(fd, &resp);
    }

    free(buf);
    arena_destroy(a);
    close(fd);
}

/* Аллоцирует сервер. При наличии tls_ctx HTTPS слушает на 8443,
 * а исходный port используется под HTTP→HTTPS редирект. */
HttpServer *http_server_create(Router *r, int port, int backlog, TlsCtx *tls_ctx) {
    HttpServer *s = calloc(1, sizeof(HttpServer));
    s->router = r; s->port = port; s->backlog = backlog;
    s->tls_ctx = tls_ctx; s->redirect_fd = -1;
    s->https_port = tls_ctx ? 8443 : port;
    return s;
}

/* Открывает слушающие сокеты и крутит цикл accept на kqueue/epoll,
 * пока s->running. Блокирует вызывающий поток. */
/* Upper bound on concurrent connection threads. Past this we serve inline
 * (backpressure) rather than spawn unboundedly under a connection flood. */
#define HTTP_MAX_CONN_THREADS 96

typedef struct { HttpServer *srv; int fd; } ConnJob;

static void *conn_thread(void *arg) {
    ConnJob *j = (ConnJob *)arg;
    handle_conn(j->srv, j->fd);
    atomic_fetch_sub(&j->srv->active_conns, 1);
    free(j);
    return NULL;
}

/* Dispatch each connection to its own thread so that a slow handler — a heavy
 * /tables/query, or one stalled on the catalog mutex a worker is holding during
 * a big ingest/SCD2 — can NEVER starve /health (which the watchdog polls) on the
 * single kqueue/epoll event loop. The HTTP handlers' shared state is already
 * mutex-guarded (catalog CAT_GUARD, tables_mu, per-metric mutexes, scheduler mu)
 * because the worker pool and scheduler already run concurrently with the event
 * loop. Threads get the same 8 MiB stack as the worker pool (pipeline execution
 * can run inline on a connection via /run preview paths). */
static void dispatch_conn(HttpServer *s, int cfd) {
    if (atomic_load(&s->active_conns) >= HTTP_MAX_CONN_THREADS) {
        handle_conn(s, cfd);   /* backpressure: serve inline */
        return;
    }
    ConnJob *j = (ConnJob *)malloc(sizeof(*j));
    if (!j) { handle_conn(s, cfd); return; }
    j->srv = s; j->fd = cfd;

    pthread_attr_t attr; pthread_attr_t *ap = NULL;
    if (pthread_attr_init(&attr) == 0) {
        pthread_attr_setstacksize(&attr, 8 * 1024 * 1024);
        pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
        ap = &attr;
    }
    atomic_fetch_add(&s->active_conns, 1);
    pthread_t th;
    int rc = pthread_create(&th, ap, conn_thread, j);
    if (ap) pthread_attr_destroy(&attr);
    if (rc != 0) {                          /* spawn failed — fall back inline */
        atomic_fetch_sub(&s->active_conns, 1);
        free(j);
        handle_conn(s, cfd);
    }
}

void http_server_run(HttpServer *s) {
    signal(SIGPIPE, SIG_IGN);
    int yes = 1;

    /* When TLS: HTTPS on https_port (8443), plain redirect on port (8080) */
    int main_port = s->tls_ctx ? s->https_port : s->port;

    int lfd = socket(AF_INET, SOCK_STREAM, 0);
    setsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
#ifdef SO_REUSEPORT
    setsockopt(lfd, SOL_SOCKET, SO_REUSEPORT, &yes, sizeof(yes));
#endif
    struct sockaddr_in addr = {.sin_family=AF_INET,
        .sin_port=htons((uint16_t)main_port), .sin_addr={INADDR_ANY}};
    if (bind(lfd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        LOG_ERROR("bind port %d: %s", main_port, strerror(errno)); return;
    }
    listen(lfd, s->backlog > 0 ? s->backlog : 128);
    s->listenfd = lfd; s->running = 1;

    /* HTTP→HTTPS redirect listener on s->port when TLS is active */
    if (s->tls_ctx && s->port != main_port) {
        int rfd = socket(AF_INET, SOCK_STREAM, 0);
        setsockopt(rfd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
#ifdef SO_REUSEPORT
        setsockopt(rfd, SOL_SOCKET, SO_REUSEPORT, &yes, sizeof(yes));
#endif
        struct sockaddr_in ra = {.sin_family=AF_INET,
            .sin_port=htons((uint16_t)s->port), .sin_addr={INADDR_ANY}};
        if (bind(rfd, (struct sockaddr*)&ra, sizeof(ra)) == 0) {
            listen(rfd, s->backlog > 0 ? s->backlog : 128);
            s->redirect_fd = rfd;
            LOG_INFO("HTTP→HTTPS redirect on :%d → https://localhost:%d",
                     s->port, main_port);
        } else {
            LOG_WARN("redirect bind :%d failed: %s", s->port, strerror(errno));
            close(rfd);
        }
    }

    if (s->tls_ctx)
        LOG_INFO("HTTPS server listening on :%d (TLS 1.2+)", main_port);
    else
        LOG_INFO("HTTP server listening on :%d", main_port);

#ifdef __APPLE__
    int evfd = kqueue(); s->epfd = evfd;
    struct kevent kev;
    EV_SET(&kev, lfd, EVFILT_READ, EV_ADD, 0, 0, NULL);
    kevent(evfd, &kev, 1, NULL, 0, NULL);
    if (s->redirect_fd >= 0) {
        EV_SET(&kev, s->redirect_fd, EVFILT_READ, EV_ADD, 0, 0, NULL);
        kevent(evfd, &kev, 1, NULL, 0, NULL);
    }

    struct kevent events[64];
    while (s->running) {
        struct timespec ts = {0, 100000000}; /* 100ms */
        int nev = kevent(evfd, NULL, 0, events, 64, &ts);
        for (int i = 0; i < nev; i++) {
            int efd = (int)events[i].ident;
            struct sockaddr_in ca; socklen_t cl = sizeof(ca);
            if (efd == lfd) {
                int cfd = accept(lfd, (struct sockaddr*)&ca, &cl);
                if (cfd < 0) continue;
                fcntl(cfd, F_SETFD, FD_CLOEXEC);
                dispatch_conn(s, cfd);
            } else if (s->redirect_fd >= 0 && efd == s->redirect_fd) {
                int cfd = accept(s->redirect_fd, (struct sockaddr*)&ca, &cl);
                if (cfd < 0) continue;
                fcntl(cfd, F_SETFD, FD_CLOEXEC);
                handle_redirect(cfd, main_port);
            }
        }
    }
    close(lfd);
    if (s->redirect_fd >= 0) { close(s->redirect_fd); s->redirect_fd = -1; }
    close(evfd);
#else
    int epfd = epoll_create1(0); s->epfd = epfd;
    struct epoll_event ev = {.events = EPOLLIN, .data.fd = lfd};
    epoll_ctl(epfd, EPOLL_CTL_ADD, lfd, &ev);
    if (s->redirect_fd >= 0) {
        struct epoll_event rev = {.events = EPOLLIN, .data.fd = s->redirect_fd};
        epoll_ctl(epfd, EPOLL_CTL_ADD, s->redirect_fd, &rev);
    }

    struct epoll_event events[64];
    while (s->running) {
        int nev = epoll_wait(epfd, events, 64, 100);
        for (int i = 0; i < nev; i++) {
            struct sockaddr_in ca; socklen_t cl = sizeof(ca);
            if (events[i].data.fd == lfd) {
                int cfd = accept4(lfd, (struct sockaddr*)&ca, &cl, SOCK_CLOEXEC);
                if (cfd < 0) continue;
                dispatch_conn(s, cfd);
            } else if (s->redirect_fd >= 0 && events[i].data.fd == s->redirect_fd) {
                int cfd = accept4(s->redirect_fd, (struct sockaddr*)&ca, &cl, SOCK_CLOEXEC);
                if (cfd < 0) continue;
                handle_redirect(cfd, main_port);
            }
        }
    }
    close(lfd);
    if (s->redirect_fd >= 0) { close(s->redirect_fd); s->redirect_fd = -1; }
    close(epfd);
#endif
}

/* Просит цикл http_server_run() завершиться (флаг проверяется раз в ~100мс). */
void http_server_stop(HttpServer *s) { s->running = 0; }
