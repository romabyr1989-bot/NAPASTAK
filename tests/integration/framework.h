/*
 * Integration test framework for NAPASTAK.
 * Provides: server lifecycle, HTTP client (libcurl), TAP helpers, JSON assertions.
 */
#pragma once
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
/* strncasecmp объявлен в <strings.h> по POSIX. В glibc он подтягивается и через
 * <string.h> при _GNU_SOURCE, поэтому на Linux сборка проходила, а на macOS
 * падала с «call to undeclared function 'strncasecmp'». */
#include <strings.h>
#include <stdint.h>
#include <unistd.h>
#include <signal.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <curl/curl.h>

/* ── TAP state ── */
static int _tap_plan = 0, _tap_pass = 0, _tap_fail = 0;

#define ok(cond, ...) do { \
    _tap_plan++; \
    if (cond) { _tap_pass++; printf("ok %d — " __VA_ARGS__); puts(""); } \
    else       { _tap_fail++; printf("not ok %d — " __VA_ARGS__); puts(""); } \
} while (0)

#define tap_done() do { \
    printf("1..%d\n# pass=%d fail=%d\n", _tap_plan, _tap_pass, _tap_fail); \
    return _tap_fail > 0 ? 1 : 0; \
} while (0)

/* ── Server process ── */
static pid_t _srv_pid = -1;
static int   _srv_port = 0;
static char  _srv_data_dir[256];
static char  _srv_jwt[512];    /* JWT for authenticated requests */

static void _srv_cleanup(void) {
    if (_srv_pid > 0) {
        kill(_srv_pid, SIGTERM);
        int status;
        waitpid(_srv_pid, &status, 0);
        _srv_pid = -1;
    }
    /* remove test data dir */
    if (_srv_data_dir[0]) {
        char cmd[512];
        snprintf(cmd, sizeof(cmd), "rm -rf %s", _srv_data_dir);
        (void)system(cmd);
    }
}

/*
 * Start the gateway binary in a subprocess.
 * binary  — path to napastak_gateway executable
 * port    — TCP port to listen on
 * Returns 0 on success, -1 on failure.
 */
static int test_server_start(const char *binary, int port) {
    _srv_port = port;
    snprintf(_srv_data_dir, sizeof(_srv_data_dir), "/tmp/dfo_test_%d", (int)getpid());
    mkdir(_srv_data_dir, 0755);

    atexit(_srv_cleanup);

    _srv_pid = fork();
    if (_srv_pid < 0) return -1;

    if (_srv_pid == 0) {
        /* child: redirect stdout/stderr to /dev/null to keep TAP output clean */
        int devnull = open("/dev/null", O_WRONLY);
        if (devnull >= 0) { dup2(devnull, STDOUT_FILENO); dup2(devnull, STDERR_FILENO); close(devnull); }
        char port_str[16];
        snprintf(port_str, sizeof(port_str), "%d", port);
        execl(binary, binary, "-p", port_str, "-d", _srv_data_dir, NULL);
        _exit(127);
    }
    return 0;
}

/*
 * Poll GET /health until the server responds 200, or timeout_ms elapses.
 * Returns 0 when ready, -1 on timeout.
 */
static size_t _null_write(void *p, size_t s, size_t n, void *u) {
    (void)p; (void)u; return s * n;
}

static int test_server_wait_ready(int timeout_ms) {
    char url[128];
    snprintf(url, sizeof(url), "http://127.0.0.1:%d/health", _srv_port);
    int elapsed = 0;
    while (elapsed < timeout_ms) {
        CURL *c = curl_easy_init();
        if (!c) { usleep(50000); elapsed += 50; continue; }
        curl_easy_setopt(c, CURLOPT_URL, url);
        curl_easy_setopt(c, CURLOPT_TIMEOUT_MS, 500L);
        curl_easy_setopt(c, CURLOPT_NOSIGNAL, 1L);
        curl_easy_setopt(c, CURLOPT_WRITEFUNCTION, _null_write);
        CURLcode rc = curl_easy_perform(c);
        long http_code = 0;
        curl_easy_getinfo(c, CURLINFO_RESPONSE_CODE, &http_code);
        curl_easy_cleanup(c);
        if (rc == CURLE_OK && http_code == 200) return 0;
        usleep(100000);
        elapsed += 100;
    }
    return -1;
}

/* ── HTTP response buffer ── */
typedef struct {
    char  *data;
    size_t len;
    size_t cap;
    long   status;
    char   correlation_id[64];
} HttpResp;

static size_t _write_cb(void *ptr, size_t size, size_t nmemb, void *ud) {
    HttpResp *r = (HttpResp *)ud;
    size_t n = size * nmemb;
    if (r->len + n + 1 > r->cap) {
        r->cap = r->len + n + 1 + 4096;
        r->data = (char *)realloc(r->data, r->cap);
    }
    memcpy(r->data + r->len, ptr, n);
    r->len += n;
    r->data[r->len] = '\0';
    return n;
}

static size_t _header_cb(char *buf, size_t size, size_t nitems, void *ud) {
    HttpResp *r = (HttpResp *)ud;
    size_t n = size * nitems;
    if (strncasecmp(buf, "X-Correlation-Id:", 17) == 0) {
        const char *val = buf + 17;
        while (*val == ' ') val++;
        size_t vl = strlen(val);
        while (vl > 0 && (val[vl-1] == '\r' || val[vl-1] == '\n')) vl--;
        if (vl < sizeof(r->correlation_id)) {
            memcpy(r->correlation_id, val, vl);
            r->correlation_id[vl] = '\0';
        }
    }
    return n;
}

static void resp_free(HttpResp *r) { free(r->data); memset(r, 0, sizeof(*r)); }

/*
 * Perform an HTTP request.
 * method  — "GET" / "POST" / "DELETE"
 * path    — URL path (e.g. "/api/tables")
 * body    — request body (may be NULL)
 * content_type — e.g. "application/json" (may be NULL)
 * out     — response (caller must call resp_free())
 * Returns CURL error code (0 = success).
 */
static CURLcode http_do(const char *method, const char *path,
                        const char *body, const char *content_type,
                        HttpResp *out) {
    memset(out, 0, sizeof(*out));
    out->cap = 4096;
    out->data = (char *)malloc(out->cap);
    out->data[0] = '\0';

    char url[512];
    snprintf(url, sizeof(url), "http://127.0.0.1:%d%s", _srv_port, path);

    CURL *c = curl_easy_init();
    struct curl_slist *hdrs = NULL;

    if (_srv_jwt[0]) {
        char auth_hdr[600];
        snprintf(auth_hdr, sizeof(auth_hdr), "Authorization: Bearer %s", _srv_jwt);
        hdrs = curl_slist_append(hdrs, auth_hdr);
    }
    if (content_type) {
        char ct_hdr[128];
        snprintf(ct_hdr, sizeof(ct_hdr), "Content-Type: %s", content_type);
        hdrs = curl_slist_append(hdrs, ct_hdr);
    }
    if (hdrs) curl_easy_setopt(c, CURLOPT_HTTPHEADER, hdrs);

    curl_easy_setopt(c, CURLOPT_URL, url);
    curl_easy_setopt(c, CURLOPT_NOSIGNAL, 1L);
    curl_easy_setopt(c, CURLOPT_TIMEOUT, 10L);
    curl_easy_setopt(c, CURLOPT_WRITEFUNCTION, _write_cb);
    curl_easy_setopt(c, CURLOPT_WRITEDATA, out);
    curl_easy_setopt(c, CURLOPT_HEADERFUNCTION, _header_cb);
    curl_easy_setopt(c, CURLOPT_HEADERDATA, out);

    if (strcmp(method, "POST") == 0) {
        curl_easy_setopt(c, CURLOPT_POST, 1L);
        curl_easy_setopt(c, CURLOPT_POSTFIELDS, body ? body : "");
        curl_easy_setopt(c, CURLOPT_POSTFIELDSIZE, (long)(body ? strlen(body) : 0));
    } else if (strcmp(method, "DELETE") == 0) {
        curl_easy_setopt(c, CURLOPT_CUSTOMREQUEST, "DELETE");
    }

    CURLcode rc = curl_easy_perform(c);
    curl_easy_getinfo(c, CURLINFO_RESPONSE_CODE, &out->status);
    curl_easy_cleanup(c);
    if (hdrs) curl_slist_free_all(hdrs);
    return rc;
}

/* Shortcuts */
static CURLcode http_get(const char *path, HttpResp *out) {
    return http_do("GET", path, NULL, NULL, out);
}
static CURLcode http_post_json(const char *path, const char *body, HttpResp *out) {
    return http_do("POST", path, body, "application/json", out);
}
static CURLcode http_post_csv(const char *path, const char *csv, HttpResp *out) {
    return http_do("POST", path, csv, "text/csv", out);
}
static CURLcode http_delete(const char *path, HttpResp *out) {
    return http_do("DELETE", path, NULL, NULL, out);
}

/* ── Auth helper ── */
static int test_login(const char *password) {
    char body[256];
    snprintf(body, sizeof(body),
             "{\"username\":\"admin\",\"password\":\"%s\"}", password);
    HttpResp r;
    http_post_json("/api/auth/token", body, &r);
    if (r.status != 200) { resp_free(&r); return -1; }
    /* extract token field */
    const char *p = strstr(r.data, "\"token\"");
    if (!p) { resp_free(&r); return -1; }
    p = strchr(p, ':');
    if (!p) { resp_free(&r); return -1; }
    while (*p == ':' || *p == ' ' || *p == '"') p++;
    int i = 0;
    while (*p && *p != '"' && i < (int)sizeof(_srv_jwt) - 1)
        _srv_jwt[i++] = *p++;
    _srv_jwt[i] = '\0';
    resp_free(&r);
    return 0;
}

/* ── Simple JSON field extraction ── */

/* Returns pointer to value of first matching key, or NULL. Non-allocating. */
static const char *json_find(const char *json, const char *key) {
    char needle[128];
    snprintf(needle, sizeof(needle), "\"%s\"", key);
    const char *p = strstr(json, needle);
    if (!p) return NULL;
    p += strlen(needle);
    while (*p == ' ' || *p == ':') p++;
    return p;
}

/* Extract a string value (into buf). Returns 0 on success. */
static int json_str(const char *json, const char *key, char *buf, size_t cap) {
    const char *p = json_find(json, key);
    if (!p || *p != '"') return -1;
    p++;
    size_t i = 0;
    while (*p && *p != '"' && i < cap - 1) buf[i++] = *p++;
    buf[i] = '\0';
    return 0;
}

/* Extract an integer value. Returns defval if not found. */
static long json_int(const char *json, const char *key, long defval) {
    const char *p = json_find(json, key);
    if (!p) return defval;
    if (*p == '"') p++;  /* skip quote if value is quoted */
    char *end;
    long v = strtol(p, &end, 10);
    return (end > p) ? v : defval;
}

/* Check if JSON array/object contains a string */
static int json_contains(const char *json, const char *needle) {
    return strstr(json, needle) != NULL;
}
