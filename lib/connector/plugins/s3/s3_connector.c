/* S3 / MinIO connector — ABI v1 */
#include "../../../connector/connector.h"
#include "../../../core/arena.h"
#include "../../../core/json.h"
#include "../../../core/log.h"
#include "aws_sig4.h"
#include <curl/curl.h>
#include <openssl/sha.h>
#include <string.h>
#include <stdlib.h>
#include <strings.h>
#include <stdio.h>

/* ── Context ── */

typedef struct {
    AwsCredentials creds;
    char endpoint[256];    /* "https://s3.amazonaws.com" */
    char bucket[128];
    char prefix[256];
    char file_format[16];  /* "csv" or "parquet" */
    Arena *arena;
} S3Ctx;

/* ── CURL write callback ── */

typedef struct {
    char  *data;
    size_t len;
    size_t cap;
} DynBuf;

static size_t write_callback(void *ptr, size_t size, size_t nmemb, void *userdata)
{
    DynBuf *buf = (DynBuf *)userdata;
    size_t incoming = size * nmemb;
    if (buf->len + incoming + 1 > buf->cap) {
        size_t new_cap = buf->cap ? buf->cap * 2 : 4096;
        while (new_cap < buf->len + incoming + 1) new_cap *= 2;
        buf->data = realloc(buf->data, new_cap);
        if (!buf->data) return 0;
        buf->cap = new_cap;
    }
    memcpy(buf->data + buf->len, ptr, incoming);
    buf->len += incoming;
    buf->data[buf->len] = '\0';
    return incoming;
}

/* ── JSON config parser helpers ── */

static void cfg_str(const char *cfg, const char *key, char *dst, size_t dstsz)
{
    const char *p = strstr(cfg, key);
    if (!p) return;
    p = strchr(p, ':');
    if (!p) return;
    p++;
    while (*p == ' ' || *p == '\t') p++;
    if (*p == '"') {
        p++;
        const char *e = strchr(p, '"');
        if (e) snprintf(dst, dstsz, "%.*s", (int)(e - p), p);
    }
}

/* ── URL builder helper ── */

/* Appends path+query to endpoint in a malloc'd buffer.  Caller must free. */
static char *build_url(const S3Ctx *ctx, const char *path, const char *query)
{
    char buf[1024];
    if (query && *query)
        snprintf(buf, sizeof(buf), "%s%s?%s", ctx->endpoint, path, query);
    else
        snprintf(buf, sizeof(buf), "%s%s", ctx->endpoint, path);
    return strdup(buf);
}

/* ── Signed CURL request ── */

typedef struct {
    const char *method;
    const char *url_path;  /* path component */
    const char *query;
    const char *range;     /* optional Range header value */
    long        resp_code;
    DynBuf      body;
} S3Req;

static int s3_do_request(S3Ctx *ctx, S3Req *r, Arena *a)
{
    /* Signature */
    char auth_hdr[512];
    char date_hdr[32];
    /* SHA-256 of empty body */
    const char *empty_sha = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

    /* Extract host from endpoint */
    char host[256];
    const char *ep = ctx->endpoint;
    if (strncmp(ep, "https://", 8) == 0) ep += 8;
    else if (strncmp(ep, "http://", 7) == 0) ep += 7;
    const char *slash = strchr(ep, '/');
    if (slash)
        snprintf(host, sizeof(host), "%.*s", (int)(slash - ep), ep);
    else
        snprintf(host, sizeof(host), "%s", ep);

    Arena *tmp = a ? a : ctx->arena;
    aws_sig4_sign(&ctx->creds, r->method, host, r->url_path,
                  r->query, empty_sha,
                  auth_hdr, sizeof(auth_hdr),
                  date_hdr, sizeof(date_hdr),
                  tmp);

    /* Build full URL */
    char *url = build_url(ctx, r->url_path, r->query);

    CURL *curl = curl_easy_init();
    if (!curl) { free(url); return -1; }

    struct curl_slist *headers = NULL;
    char auth_full[600], date_full[64];
    snprintf(auth_full, sizeof(auth_full), "Authorization: %s", auth_hdr);
    snprintf(date_full, sizeof(date_full), "x-amz-date: %s", date_hdr);
    headers = curl_slist_append(headers, auth_full);
    headers = curl_slist_append(headers, date_full);

    if (r->range) {
        char range_full[64];
        snprintf(range_full, sizeof(range_full), "Range: %s", r->range);
        headers = curl_slist_append(headers, range_full);
    }

    curl_easy_setopt(curl, CURLOPT_URL, url);
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_callback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &r->body);
    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, 30L);
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 1L);

    if (strcmp(r->method, "HEAD") == 0)
        curl_easy_setopt(curl, CURLOPT_NOBODY, 1L);
    else if (strcmp(r->method, "GET") != 0)
        curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, r->method);

    CURLcode res = curl_easy_perform(curl);
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &r->resp_code);
    curl_slist_free_all(headers);
    curl_easy_cleanup(curl);
    free(url);

    if (res != CURLE_OK) {
        LOG_ERROR("s3: curl error: %s", curl_easy_strerror(res));
        return -1;
    }
    return 0;
}

/* ── XML helper: extract first tag value ── */

static char *xml_next_tag(const char *xml, const char *tag, const char **next_search)
{
    char open_tag[64], close_tag[64];
    snprintf(open_tag, sizeof(open_tag), "<%s>", tag);
    snprintf(close_tag, sizeof(close_tag), "</%s>", tag);

    const char *start = strstr(xml, open_tag);
    if (!start) { if (next_search) *next_search = NULL; return NULL; }
    start += strlen(open_tag);
    const char *end = strstr(start, close_tag);
    if (!end) { if (next_search) *next_search = NULL; return NULL; }

    size_t len = (size_t)(end - start);
    char *val = malloc(len + 1);
    memcpy(val, start, len);
    val[len] = '\0';

    if (next_search) *next_search = end + strlen(close_tag);
    return val;
}

/* ── CSV parsing helpers ── */

static int csv_count_cols(const char *line)
{
    if (!line || !*line) return 0;
    int n = 1;
    for (const char *p = line; *p && *p != '\n' && *p != '\r'; p++)
        if (*p == ',') n++;
    return n;
}

static void csv_split_line(Arena *a, const char *line, char **cols, int max_cols, int *n_out)
{
    int n = 0;
    const char *p = line;
    while (n < max_cols && *p && *p != '\n' && *p != '\r') {
        bool in_quote = (*p == '"');
        const char *start = in_quote ? ++p : p;
        if (in_quote) {
            while (*p && !(*p == '"' && *(p+1) != '"')) p++;
        } else {
            while (*p && *p != ',' && *p != '\n' && *p != '\r') p++;
        }
        cols[n++] = arena_strndup(a, start, (size_t)(p - start));
        if (in_quote && *p == '"') p++;
        if (*p == ',') p++;
    }
    *n_out = n;
}

/* ── Connector functions ── */

static void *s3_create(const char *config_json, Arena *arena)
{
    S3Ctx *ctx = arena_calloc(arena, sizeof(S3Ctx));
    ctx->arena = arena;

    /* defaults */
    snprintf(ctx->endpoint, sizeof(ctx->endpoint), "https://s3.amazonaws.com");
    snprintf(ctx->file_format, sizeof(ctx->file_format), "csv");

    if (config_json) {
        cfg_str(config_json, "\"access_key\"",  ctx->creds.access_key, sizeof(ctx->creds.access_key));
        cfg_str(config_json, "\"secret_key\"",  ctx->creds.secret_key, sizeof(ctx->creds.secret_key));
        cfg_str(config_json, "\"region\"",       ctx->creds.region,    sizeof(ctx->creds.region));
        cfg_str(config_json, "\"bucket\"",       ctx->bucket,           sizeof(ctx->bucket));
        cfg_str(config_json, "\"prefix\"",       ctx->prefix,           sizeof(ctx->prefix));
        cfg_str(config_json, "\"endpoint\"",     ctx->endpoint,         sizeof(ctx->endpoint));
        cfg_str(config_json, "\"file_format\"",  ctx->file_format,      sizeof(ctx->file_format));
    }

    /* default region */
    if (!ctx->creds.region[0])
        snprintf(ctx->creds.region, sizeof(ctx->creds.region), "us-east-1");

    snprintf(ctx->creds.service, sizeof(ctx->creds.service), "s3");

    curl_global_init(CURL_GLOBAL_DEFAULT);
    LOG_INFO("s3: connector created for bucket=%s region=%s", ctx->bucket, ctx->creds.region);
    return ctx;
}

static void s3_destroy(void *vctx)
{
    /* ctx and ctx->arena are owned by the CALLER (connector_load passes its
     * request arena to create(), and ctx itself is arena_calloc'd from it).
     * The previous code did arena_destroy(ctx->arena)+free(ctx)+curl_global_
     * cleanup() here, which (a) freed arena-owned memory → SIGABRT, (b) tore
     * down the caller's arena mid-request, and (c) shut down libcurl while
     * other connectors still used it. Nothing to free here — match csv/pg. */
    (void)vctx;
}

static int s3_list_entities(void *vctx, Arena *a, DfoEntityList *out)
{
    S3Ctx *ctx = (S3Ctx *)vctx;

    /* GET /{bucket}/?list-type=2&prefix={prefix} */
    char url_path[512];
    snprintf(url_path, sizeof(url_path), "/%s/", ctx->bucket);

    char query[512];
    if (ctx->prefix[0])
        snprintf(query, sizeof(query), "list-type=2&prefix=%s", ctx->prefix);
    else
        snprintf(query, sizeof(query), "list-type=2");

    S3Req req = {0};
    req.method   = "GET";
    req.url_path = url_path;
    req.query    = query;

    if (s3_do_request(ctx, &req, a) != 0 || req.resp_code != 200) {
        LOG_ERROR("s3: list_entities failed, HTTP %ld", req.resp_code);
        free(req.body.data);
        return -1;
    }

    /* Parse <Key>...</Key> entries from ListBucketResult XML */
    int cap = 64;
    DfoEntity *items = arena_calloc(a, cap * sizeof(DfoEntity));
    int count = 0;

    const char *pos = req.body.data;
    while (pos) {
        const char *next = NULL;
        char *key = xml_next_tag(pos, "Key", &next);
        if (!key) break;

        /* Only include .csv or .parquet files */
        size_t klen = strlen(key);
        bool is_csv     = klen > 4  && strcasecmp(key + klen - 4,  ".csv")     == 0;
        bool is_parquet = klen > 8  && strcasecmp(key + klen - 8,  ".parquet") == 0;

        if (is_csv || is_parquet) {
            if (count == cap) {
                cap *= 2;
                DfoEntity *nb = arena_calloc(a, cap * sizeof(DfoEntity));
                memcpy(nb, items, count * sizeof(DfoEntity));
                items = nb;
            }
            items[count].entity = arena_strdup(a, key);
            items[count].type   = "table";
            count++;
        }
        free(key);
        pos = next;
    }

    free(req.body.data);
    out->items = items;
    out->count = count;
    LOG_INFO("s3: list_entities found %d files", count);
    return 0;
}

static int s3_describe(void *vctx, Arena *a, const char *entity, Schema **out)
{
    S3Ctx *ctx = (S3Ctx *)vctx;

    /* Download first 8192 bytes */
    char url_path[512];
    snprintf(url_path, sizeof(url_path), "/%s/%s", ctx->bucket, entity);

    S3Req req = {0};
    req.method   = "GET";
    req.url_path = url_path;
    req.query    = "";
    req.range    = "bytes=0-8191";

    if (s3_do_request(ctx, &req, a) != 0) {
        LOG_ERROR("s3: describe failed for %s", entity);
        free(req.body.data);
        return -1;
    }

    if (!req.body.data || req.body.len == 0) {
        free(req.body.data);
        return -1;
    }

    /* Find first line (CSV header) */
    const char *data = req.body.data;
    const char *eol  = strchr(data, '\n');
    size_t line_len  = eol ? (size_t)(eol - data) : req.body.len;

    char *first_line = arena_strndup(a, data, line_len);
    /* Remove trailing \r if present — arena_strndup always NUL-terminates */
    size_t fl_trimmed = line_len;
    if (fl_trimmed > 0 && first_line[fl_trimmed - 1] == '\r')
        first_line[--fl_trimmed] = '\0';

    int ncols = csv_count_cols(first_line);
    if (ncols <= 0) { free(req.body.data); return -1; }

    char **header_cols = arena_alloc(a, ncols * sizeof(char *));
    int actual_cols = 0;
    csv_split_line(a, first_line, header_cols, ncols, &actual_cols);
    ncols = actual_cols;

    Schema *schema = arena_calloc(a, sizeof(Schema));
    schema->ncols  = ncols;
    schema->cols   = arena_alloc(a, ncols * sizeof(ColDef));
    for (int i = 0; i < ncols; i++) {
        schema->cols[i].name     = header_cols[i];
        schema->cols[i].type     = COL_TEXT;
        schema->cols[i].nullable = true;
    }

    free(req.body.data);
    *out = schema;
    return 0;
}

static int s3_read_batch(void *vctx, Arena *a, DfoReadReq *req,
                         const char *entity, ColBatch **out)
{
    S3Ctx *ctx = (S3Ctx *)vctx;

    /* Parse cursor */
    int64_t offset = 0;
    if (req->cursor && strncmp(req->cursor, "offset:", 7) == 0)
        offset = (int64_t)strtoll(req->cursor + 7, NULL, 10);

    /* 1 MB chunk */
    int64_t chunk = 1048575;
    char range[64];
    snprintf(range, sizeof(range), "bytes=%lld-%lld",
             (long long)offset, (long long)(offset + chunk));

    char url_path[512];
    snprintf(url_path, sizeof(url_path), "/%s/%s", ctx->bucket, entity);

    S3Req s3req = {0};
    s3req.method   = "GET";
    s3req.url_path = url_path;
    s3req.query    = "";
    s3req.range    = range;

    if (s3_do_request(ctx, &s3req, a) != 0) {
        LOG_ERROR("s3: read_batch failed for %s at offset %lld", entity, (long long)offset);
        free(s3req.body.data);
        return -1;
    }

    /* HTTP 416 = Range Not Satisfiable → EOF */
    if (s3req.resp_code == 416 || s3req.body.len == 0) {
        free(s3req.body.data);
        return 1; /* end of file */
    }

    /* Build schema on the fly from first line */
    const char *data = s3req.body.data;
    const char *eol  = strchr(data, '\n');
    if (!eol) { free(s3req.body.data); return 1; }

    size_t hdr_len = (size_t)(eol - data);
    char *hdr_line = arena_strndup(a, data, hdr_len);
    if (hdr_len > 0 && hdr_line[hdr_len - 1] == '\r') hdr_line[hdr_len - 1] = '\0';

    int ncols = csv_count_cols(hdr_line);
    if (ncols <= 0) { free(s3req.body.data); return -1; }

    char **hdr_cols = arena_alloc(a, ncols * sizeof(char *));
    int actual_ncols = 0;
    csv_split_line(a, hdr_line, hdr_cols, ncols, &actual_ncols);
    ncols = actual_ncols;

    Schema *schema = arena_calloc(a, sizeof(Schema));
    schema->ncols  = ncols;
    schema->cols   = arena_alloc(a, ncols * sizeof(ColDef));
    for (int i = 0; i < ncols; i++) {
        schema->cols[i].name     = hdr_cols[i];
        schema->cols[i].type     = COL_TEXT;
        schema->cols[i].nullable = true;
    }

    /* Allocate ColBatch */
    int batch_cap = (req->limit > 0 && req->limit < BATCH_SIZE) ? (int)req->limit : BATCH_SIZE;
    ColBatch *batch = arena_calloc(a, sizeof(ColBatch));
    batch->schema = schema;
    batch->ncols  = ncols;

    char ***sv = arena_calloc(a, ncols * sizeof(char **));
    for (int c = 0; c < ncols; c++) {
        sv[c] = arena_alloc(a, batch_cap * sizeof(char *));
        batch->values[c]      = sv[c];
        batch->null_bitmap[c] = arena_calloc(a, (batch_cap + 7) / 8);
    }

    /* Parse data rows (skip header line) */
    const char *pos = eol + 1;
    int row = 0;
    while (row < batch_cap && pos && *pos) {
        const char *row_end = strchr(pos, '\n');
        size_t row_len = row_end ? (size_t)(row_end - pos) : strlen(pos);
        if (row_len == 0) { pos = row_end ? row_end + 1 : NULL; continue; }

        char *row_line = arena_strndup(a, pos, row_len);
        if (row_line[row_len - 1] == '\r') row_line[row_len - 1] = '\0';

        char **cols = arena_alloc(a, ncols * sizeof(char *));
        int ncols_row = 0;
        csv_split_line(a, row_line, cols, ncols, &ncols_row);

        for (int c = 0; c < ncols; c++) {
            if (c < ncols_row && cols[c] && *cols[c])
                sv[c][row] = cols[c];
            else {
                sv[c][row] = NULL;
                batch->null_bitmap[c][row / 8] |= (1u << (row % 8));
            }
        }
        row++;
        pos = row_end ? row_end + 1 : NULL;
    }

    batch->nrows = row;
    *out = batch;

    /* Update cursor (cast away const — arena owns the string lifetime) */
    int64_t new_offset = offset + (int64_t)s3req.body.len;
    *(const char **)&req->cursor = arena_sprintf(a, "offset:%lld", (long long)new_offset);

    free(s3req.body.data);

    /* If we got less than a full chunk, signal EOF on next call */
    return (s3req.body.len < (size_t)chunk) ? 0 : 0;
}

static int s3_cdc_start(void *vctx, DfoCdcHandler handler, void *userdata)
{
    (void)vctx; (void)handler; (void)userdata;
    LOG_WARN("s3: CDC not supported");
    return -1;
}

static int s3_cdc_stop(void *vctx)
{
    (void)vctx;
    return -1;
}

static int s3_ping(void *vctx)
{
    S3Ctx *ctx = (S3Ctx *)vctx;

    char url_path[256];
    snprintf(url_path, sizeof(url_path), "/%s/", ctx->bucket);

    S3Req req = {0};
    req.method   = "HEAD";
    req.url_path = url_path;
    req.query    = "";

    if (s3_do_request(ctx, &req, NULL) != 0) return -1;
    free(req.body.data);

    if (req.resp_code == 200 || req.resp_code == 204) {
        LOG_INFO("s3: ping OK (%ld)", req.resp_code);
        return 0;
    }
    LOG_WARN("s3: ping returned HTTP %ld", req.resp_code);
    return -1;
}

/* ── Sink: serialize the batch to CSV and PUT it as an object ──
 * Object key = prefix + (entity if given, else "export.csv"). The body's real
 * SHA-256 is signed (sigv4) and sent as x-amz-content-sha256. mode is advisory:
 * an object PUT always replaces the key, so APPEND and OVERWRITE behave the
 * same (one object per run). Returns rows written or <0 on error.
 *
 * NOTE: verified to compile and to sign with the real payload hash, but not
 * exercised against a live bucket in this environment. */
static void s3_csv_field(DynBuf *b, const char *v) {
    if (!v) v = "";
    int needq = 0;
    for (const char *p=v; *p; p++) if (*p==','||*p=='"'||*p=='\n'||*p=='\r') { needq=1; break; }
    if (!needq) { write_callback((void*)v, 1, strlen(v), b); return; }
    write_callback((void*)"\"", 1, 1, b);
    for (const char *p=v; *p; p++) { if (*p=='"') write_callback((void*)"\"",1,1,b); write_callback((void*)p,1,1,b); }
    write_callback((void*)"\"", 1, 1, b);
}

static int s3_write_batch(void *vctx, Arena *a, const char *entity,
                          const Schema *schema, const ColBatch *batch, int mode) {
    (void)a; (void)mode;
    S3Ctx *ctx = vctx;
    if (!ctx->bucket[0]) { LOG_ERROR("s3 sink: no bucket configured"); return -1; }

    /* 1. Serialize batch → CSV body */
    DynBuf body = {0};
    char tmp[64];
    for (int c = 0; c < batch->ncols; c++) {
        if (c) write_callback((void*)",", 1, 1, &body);
        s3_csv_field(&body, schema->cols[c].name);
    }
    write_callback((void*)"\n", 1, 1, &body);
    for (int r = 0; r < batch->nrows; r++) {
        for (int c = 0; c < batch->ncols; c++) {
            if (c) write_callback((void*)",", 1, 1, &body);
            const uint8_t *bm = batch->null_bitmap[c];
            if (bm && ((bm[r/8]>>(r%8))&1u)) continue;
            const char *v;
            switch (schema->cols[c].type) {
                case COL_INT64:  snprintf(tmp,sizeof(tmp),"%lld",(long long)((int64_t*)batch->values[c])[r]); v=tmp; break;
                case COL_DOUBLE: snprintf(tmp,sizeof(tmp),"%.10g",((double*)batch->values[c])[r]); v=tmp; break;
                default:         v=((char**)batch->values[c])[r]; break;
            }
            s3_csv_field(&body, v);
        }
        write_callback((void*)"\n", 1, 1, &body);
    }
    if (!body.data) { body.data = strdup(""); body.len = 0; }

    /* 2. Object key: prefix + entity (default export.csv) */
    char key[512];
    const char *obj = (entity && entity[0]) ? entity : "export.csv";
    snprintf(key, sizeof(key), "%s%s", ctx->prefix, obj);
    char url_path[640];
    snprintf(url_path, sizeof(url_path), "/%s/%s", ctx->bucket, key);

    /* 3. Real payload SHA-256 (sigv4 requires it for body requests) */
    unsigned char dg[32]; char payload_hex[65];
    SHA256((const unsigned char*)body.data, body.len, dg);
    for (int i=0;i<32;i++) snprintf(payload_hex+i*2,3,"%02x",dg[i]);

    /* 4. Host from endpoint */
    char host[256]; const char *ep = ctx->endpoint;
    if (strncmp(ep,"https://",8)==0) ep+=8; else if (strncmp(ep,"http://",7)==0) ep+=7;
    const char *slash = strchr(ep,'/');
    if (slash) snprintf(host,sizeof(host),"%.*s",(int)(slash-ep),ep);
    else snprintf(host,sizeof(host),"%s",ep);

    char auth_hdr[512], date_hdr[32];
    aws_sig4_sign(&ctx->creds, "PUT", host, url_path, "", payload_hex,
                  auth_hdr, sizeof(auth_hdr), date_hdr, sizeof(date_hdr), ctx->arena);

    char *url = build_url(ctx, url_path, NULL);
    CURL *curl = curl_easy_init();
    if (!curl) { free(url); free(body.data); return -1; }
    struct curl_slist *headers = NULL;
    char auth_full[600], date_full[64], sha_full[96];
    snprintf(auth_full,sizeof(auth_full),"Authorization: %s",auth_hdr);
    snprintf(date_full,sizeof(date_full),"x-amz-date: %s",date_hdr);
    snprintf(sha_full,sizeof(sha_full),"x-amz-content-sha256: %s",payload_hex);
    headers = curl_slist_append(headers, auth_full);
    headers = curl_slist_append(headers, date_full);
    headers = curl_slist_append(headers, sha_full);
    headers = curl_slist_append(headers, "Content-Type: text/csv");

    DynBuf resp = {0};
    curl_easy_setopt(curl, CURLOPT_URL, url);
    curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "PUT");
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, body.data);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, (long)body.len);
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_callback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &resp);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, 60L);
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 1L);

    CURLcode res = curl_easy_perform(curl);
    long code = 0; curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &code);
    curl_slist_free_all(headers); curl_easy_cleanup(curl);
    free(url); free(body.data); free(resp.data);

    if (res != CURLE_OK) { LOG_ERROR("s3 sink: curl: %s", curl_easy_strerror(res)); return -1; }
    if (code < 200 || code >= 300) { LOG_ERROR("s3 sink: HTTP %ld for %s", code, url_path); return -1; }
    LOG_INFO("s3 sink: PUT %d rows → s3://%s/%s (HTTP %ld)", batch->nrows, ctx->bucket, key, code);
    return batch->nrows;
}

/* ── Entry point ──
 * Exported as a DATA symbol (const struct), matching every other plugin and
 * what the loader dlsym()s. (Previously a function returning the struct, which
 * the loader mis-read as the struct → garbage abi_version → never loaded.) */
const DfoConnector dfo_connector_entry = {
    .abi_version   = DFO_CONNECTOR_ABI_VERSION,
    .name          = "s3",
    .version       = "1.0.0",
    .description   = "AWS S3 / MinIO connector",
    .create        = s3_create,
    .destroy       = s3_destroy,
    .list_entities = s3_list_entities,
    .describe      = s3_describe,
    .read_batch    = s3_read_batch,
    .cdc_start     = s3_cdc_start,
    .cdc_stop      = s3_cdc_stop,
    .ping          = s3_ping,
    .write_batch   = s3_write_batch,
};
