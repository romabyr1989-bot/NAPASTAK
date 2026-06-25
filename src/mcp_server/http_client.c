/* http_client.c — thin libcurl wrapper for talking to the gateway.
 * Buffers response body into the supplied arena. */
#include "mcp.h"
#include "../../lib/core/log.h"
#include <curl/curl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Растущий буфер ответа, размещаемый в арене (a). */
typedef struct { char *buf; size_t len, cap; Arena *a; } Buf;

/* libcurl-колбэк записи: дописывает очередную порцию тела в Buf,
 * при необходимости удваивая ёмкость, и держит строку null-terminated. */
static size_t write_cb(char *ptr, size_t sz, size_t nm, void *ud) {
    Buf *b = ud;
    size_t add = sz * nm;
    /* +1 — место под завершающий '\0' */
    if (b->len + add + 1 > b->cap) {
        size_t new_cap = b->cap ? b->cap * 2 : 4096;
        while (new_cap < b->len + add + 1) new_cap *= 2;
        char *n = arena_alloc(b->a, new_cap);
        if (b->len) memcpy(n, b->buf, b->len);
        b->buf = n;
        b->cap = new_cap;
    }
    memcpy(b->buf + b->len, ptr, add);
    b->len += add;
    b->buf[b->len] = '\0';
    return add;
}

/* Выполняет один HTTP-запрос через libcurl: ставит Bearer-токен и
 * Content-Type (если заданы), буферизует тело ответа в арену и заполняет
 * out (status/body/err). Возвращает 0 при успехе, -1 при ошибке curl. */
static int do_request(const char *method, const char *url,
                      const char *bearer, const char *body,
                      const char *content_type,
                      McpHttpResp *out, Arena *a) {
    CURL *c = curl_easy_init();
    if (!c) {
        out->status = -1;
        snprintf(out->err, sizeof(out->err), "curl_easy_init failed");
        return -1;
    }

    Buf body_buf = { .a = a };
    char hdr_auth[512]; hdr_auth[0] = '\0';
    char hdr_ct[256];   hdr_ct[0]   = '\0';
    struct curl_slist *headers = NULL;
    if (bearer && bearer[0]) {
        snprintf(hdr_auth, sizeof(hdr_auth), "Authorization: Bearer %s", bearer);
        headers = curl_slist_append(headers, hdr_auth);
    }
    if (content_type) {
        snprintf(hdr_ct, sizeof(hdr_ct), "Content-Type: %s", content_type);
        headers = curl_slist_append(headers, hdr_ct);
    }

    curl_easy_setopt(c, CURLOPT_URL, url);
    curl_easy_setopt(c, CURLOPT_CUSTOMREQUEST, method);
    curl_easy_setopt(c, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(c, CURLOPT_WRITEFUNCTION, write_cb);
    curl_easy_setopt(c, CURLOPT_WRITEDATA, &body_buf);
    curl_easy_setopt(c, CURLOPT_TIMEOUT, 60L);
    curl_easy_setopt(c, CURLOPT_NOSIGNAL, 1L); /* безопасно в многопоточной среде */
    if (body) {
        curl_easy_setopt(c, CURLOPT_POSTFIELDS, body);
        curl_easy_setopt(c, CURLOPT_POSTFIELDSIZE, (long)strlen(body));
    }

    CURLcode rc = curl_easy_perform(c);
    long status = 0;
    curl_easy_getinfo(c, CURLINFO_RESPONSE_CODE, &status);

    if (rc != CURLE_OK) {
        out->status = -1;
        snprintf(out->err, sizeof(out->err), "%s", curl_easy_strerror(rc));
        curl_slist_free_all(headers);
        curl_easy_cleanup(c);
        return -1;
    }

    out->status   = (int)status;
    /* пустой ответ → отдаём неизменяемую "" вместо NULL */
    out->body     = body_buf.buf ? body_buf.buf : (char *)"";
    out->body_len = body_buf.len;
    out->err[0]   = '\0';

    curl_slist_free_all(headers);
    curl_easy_cleanup(c);
    return 0;
}

/* Склеивает base+path в новую строку в арене (без нормализации слэшей). */
static char *build_url(const char *base, const char *path, Arena *a) {
    size_t b = strlen(base), p = strlen(path);
    char *u = arena_alloc(a, b + p + 1);
    memcpy(u, base, b);
    memcpy(u + b, path, p + 1);
    return u;
}

/* GET по url_base+path. */
int mcp_http_get(const char *url_base, const char *path,
                 const char *bearer, McpHttpResp *out, Arena *a) {
    return do_request("GET", build_url(url_base, path, a), bearer, NULL, NULL, out, a);
}

/* POST с телом body; по умолчанию Content-Type application/json. */
int mcp_http_post(const char *url_base, const char *path,
                  const char *bearer, const char *body,
                  const char *content_type, McpHttpResp *out, Arena *a) {
    return do_request("POST", build_url(url_base, path, a), bearer,
                      body, content_type ? content_type : "application/json",
                      out, a);
}

/* DELETE по url_base+path. */
int mcp_http_delete(const char *url_base, const char *path,
                    const char *bearer, McpHttpResp *out, Arena *a) {
    return do_request("DELETE", build_url(url_base, path, a), bearer,
                      NULL, NULL, out, a);
}
