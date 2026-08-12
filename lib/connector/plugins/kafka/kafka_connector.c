/* Kafka streaming connector — ABI v1 */
#include "../../../connector/connector.h"
#include "../../../core/arena.h"
#include "../../../core/json.h"
#include "../../../core/log.h"
#include <librdkafka/rdkafka.h>
#include <pthread.h>
#include <string.h>
#include <strings.h>   /* strcasecmp */
#include <stdlib.h>
#include <stdatomic.h>
#include <stdio.h>

/* ── Context ── */

#define KAFKA_BUF_SIZE 256

typedef struct {
    rd_kafka_t                        *rk;
    rd_kafka_topic_partition_list_t   *tplist;
    char  brokers[512];
    char  group_id[128];
    char  topic_name[128];
    char  data_format[16];   /* "json" or "csv" */

    /* SASL / TLS. Empty string = "not configured" → the property is not set on
     * the rdkafka conf at all, so a plaintext broker keeps working as before. */
    char  security_protocol[32];  /* plaintext|ssl|sasl_plaintext|sasl_ssl */
    char  sasl_mechanism[32];     /* SCRAM-SHA-256|SCRAM-SHA-512|PLAIN|... */
    char  sasl_username[128];
    char  sasl_password[256];
    char  ssl_ca_location[512];
    char  ssl_cert_location[512];
    char  ssl_key_location[512];
    char  ssl_key_password[128];
    int   ssl_no_verify;          /* 1 → disable hostname verification */

    DfoCdcHandler   cdc_handler;
    void           *cdc_userdata;
    pthread_t       consumer_thread;
    atomic_int      consumer_running;

    /* Broker-side failures (auth included) arrive asynchronously on rdkafka's
     * own thread via error_cb — stash them here so the synchronous entry points
     * can report *why* they returned no rows instead of looking like an empty
     * topic. */
    pthread_mutex_t err_mu;
    char            last_error[256];
    int             err_repeat;   /* identical errors seen since last report */
    atomic_int      auth_failed;

    ColBatch       *buffer[KAFKA_BUF_SIZE];
    int             buf_head, buf_tail;
    pthread_mutex_t buf_mu;
    Arena          *arena;
} KafkaCtx;

/* Record a broker error. librdkafka re-reports the same failure on every
 * reconnect attempt, so returns 1 only when the message actually changed —
 * that is the one worth a log line. *suppressed receives how many identical
 * ones were swallowed since the last report. */
static int ctx_note_error(KafkaCtx *ctx, const char *msg, int *suppressed)
{
    if (!msg) msg = "";
    pthread_mutex_lock(&ctx->err_mu);
    int is_new = strcmp(ctx->last_error, msg) != 0;
    *suppressed = ctx->err_repeat;
    if (is_new) {
        ctx->err_repeat = 0;
        snprintf(ctx->last_error, sizeof(ctx->last_error), "%s", msg);
    } else {
        ctx->err_repeat++;
    }
    pthread_mutex_unlock(&ctx->err_mu);
    return is_new;
}

static void ctx_set_error(KafkaCtx *ctx, const char *msg)
{
    int ignored = 0;
    ctx_note_error(ctx, msg, &ignored);
}

/* Copy the last broker error into caller-owned storage. Returns 1 if set. */
static int ctx_get_error(KafkaCtx *ctx, char *dst, size_t dstsz)
{
    pthread_mutex_lock(&ctx->err_mu);
    int has = ctx->last_error[0] != '\0';
    snprintf(dst, dstsz, "%s", ctx->last_error);
    pthread_mutex_unlock(&ctx->err_mu);
    return has;
}

/* rdkafka's synchronous calls return a generic code ("Broker transport
 * failure"); the actual cause ("SASL authentication error: …") only ever
 * arrives through error_cb. Join the two for a message that can be acted on. */
static void err_detail(KafkaCtx *ctx, const char *generic, char *dst, size_t dstsz)
{
    char last[256] = "";
    ctx_get_error(ctx, last, sizeof(last));
    if (last[0] && strcmp(last, generic) != 0)
        snprintf(dst, dstsz, "%s — %s", generic, last);
    else
        snprintf(dst, dstsz, "%s", generic);
}

/* ── JSON config parser helpers ── */

/* Legacy scanner, kept only as a fallback for configs that do not parse as
 * JSON. It stops at the first '"' and so mangles any value containing an
 * escape — which is exactly what SASL passwords tend to contain. */
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

/* Read a string field out of the parsed config object. Accepts both the
 * snake_case name used by the UI/YAML ("sasl_username") and librdkafka's own
 * dotted name ("sasl.username"), so a config copied from a Kafka client
 * works unchanged. `dotted` may be NULL. */
static void cfg_get(JVal *root, const char *snake, const char *dotted,
                    char *dst, size_t dstsz)
{
    JVal *v = json_get(root, snake);
    if (!v && dotted) v = json_get(root, dotted);
    if (!v || v->type == JV_NULL) return;

    if (v->type == JV_STRING) {
        if (v->len >= dstsz)
            LOG_WARN("kafka: config \"%s\" is %zu bytes, truncated to %zu",
                     snake, v->len, dstsz - 1);
        snprintf(dst, dstsz, "%.*s", (int)v->len, v->s);
    } else if (v->type == JV_BOOL) {
        snprintf(dst, dstsz, "%s", v->b ? "true" : "false");
    } else if (v->type == JV_NUMBER) {
        snprintf(dst, dstsz, "%lld", (long long)v->n);
    }
}

/* security.protocol is matched case-insensitively by librdkafka, but "SASL-SSL"
 * (dash) and stray whitespace are common in hand-written configs. */
static void normalize_protocol(char *s)
{
    for (char *p = s; *p; p++) {
        if (*p == '-') *p = '_';
        else if (*p >= 'A' && *p <= 'Z') *p = (char)(*p - 'A' + 'a');
    }
}

/* Mechanisms are spelled "SCRAM-SHA-256" by Kafka. Accept scram_sha_256,
 * scram-sha-256, SCRAM_SHA_512, plain, … and normalize to that spelling. */
static void normalize_mechanism(char *s)
{
    for (char *p = s; *p; p++) {
        if (*p == '_') *p = '-';
        else if (*p >= 'a' && *p <= 'z') *p = (char)(*p - 'a' + 'A');
    }
}

/* ── Kafka rdkafka helpers ── */

/* Async broker errors (auth, transport, all-brokers-down). Runs on an rdkafka
 * thread — only touches ctx through the err_mu-guarded helpers. */
static void kafka_error_cb(rd_kafka_t *rk, int err, const char *reason,
                           void *opaque)
{
    (void)rk;
    KafkaCtx *ctx = (KafkaCtx *)opaque;
    rd_kafka_resp_err_t e = (rd_kafka_resp_err_t)err;

    int is_auth = (e == RD_KAFKA_RESP_ERR__AUTHENTICATION ||
                   e == RD_KAFKA_RESP_ERR_SASL_AUTHENTICATION_FAILED);

    char msg[256];
    snprintf(msg, sizeof(msg), "%s: %s", rd_kafka_err2name(e),
             reason ? reason : "");

    if (!ctx) {
        LOG_WARN("kafka: broker error %s", msg);
        return;
    }

    if (is_auth) atomic_store(&ctx->auth_failed, 1);

    /* librdkafka retries on a backoff, so one wrong password produces this
     * callback every few seconds — and a down broker produces a burst of
     * identical _ALL_BROKERS_DOWN. Report each distinct failure once. */
    int suppressed = 0;
    if (!ctx_note_error(ctx, msg, &suppressed)) {
        LOG_DEBUG("kafka: broker error repeats: %s", msg);
        return;
    }

    char tail[64] = "";
    if (suppressed > 0)
        snprintf(tail, sizeof(tail), " (%d identical suppressed)", suppressed);

    if (is_auth)
        LOG_ERROR("kafka: authentication failed — %s%s", msg, tail);
    else
        LOG_WARN("kafka: broker error — %s%s", msg, tail);
}

/* Set one rdkafka property, logging the key (never the value — this is also
 * used for sasl.password) and, for the SASL/SSL keys, the build's feature list,
 * since "No such configuration property: sasl.mechanism" means librdkafka was
 * compiled without SASL support rather than that the config is wrong. */
static int conf_set(rd_kafka_conf_t *conf, const char *key, const char *val,
                    char *errstr, size_t errlen)
{
    if (rd_kafka_conf_set(conf, key, val, errstr, errlen) == RD_KAFKA_CONF_OK)
        return 0;

    char feats[256] = "";
    size_t fsz = sizeof(feats);
    if (rd_kafka_conf_get(conf, "builtin.features", feats, &fsz) != RD_KAFKA_CONF_OK)
        snprintf(feats, sizeof(feats), "<unknown>");

    LOG_ERROR("kafka: cannot set %s: %s (librdkafka builtin.features=%s)",
              key, errstr, feats);
    return -1;
}

static rd_kafka_t *make_consumer(KafkaCtx *ctx, char *errstr, size_t errlen)
{
    rd_kafka_conf_t *conf = rd_kafka_conf_new();

    rd_kafka_conf_set_opaque(conf, ctx);
    rd_kafka_conf_set_error_cb(conf, kafka_error_cb);

    struct { const char *key; const char *val; } base[] = {
        { "bootstrap.servers", ctx->brokers  },
        { "group.id",          ctx->group_id },
        /* Start from earliest unconsumed offset */
        { "auto.offset.reset", "earliest"    },
    };
    for (size_t i = 0; i < sizeof(base) / sizeof(base[0]); i++) {
        if (conf_set(conf, base[i].key, base[i].val, errstr, errlen) != 0) {
            rd_kafka_conf_destroy(conf);
            return NULL;
        }
    }

    /* Auth / transport. Each property is set only when configured, so an
     * unauthenticated broker sees exactly the conf it saw before. */
    struct { const char *key; const char *val; } sec[] = {
        { "security.protocol",        ctx->security_protocol },
        { "sasl.mechanism",           ctx->sasl_mechanism    },
        { "sasl.username",            ctx->sasl_username     },
        { "sasl.password",            ctx->sasl_password     },
        { "ssl.ca.location",          ctx->ssl_ca_location   },
        { "ssl.certificate.location", ctx->ssl_cert_location },
        { "ssl.key.location",         ctx->ssl_key_location  },
        { "ssl.key.password",         ctx->ssl_key_password  },
    };
    for (size_t i = 0; i < sizeof(sec) / sizeof(sec[0]); i++) {
        if (!sec[i].val[0]) continue;
        if (conf_set(conf, sec[i].key, sec[i].val, errstr, errlen) != 0) {
            rd_kafka_conf_destroy(conf);
            return NULL;
        }
    }

    if (ctx->ssl_no_verify &&
        conf_set(conf, "ssl.endpoint.identification.algorithm", "none",
                 errstr, errlen) != 0) {
        rd_kafka_conf_destroy(conf);
        return NULL;
    }

    rd_kafka_t *rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, errlen);
    if (!rk) {
        rd_kafka_conf_destroy(conf);
        return NULL;
    }

    /* conf is consumed by rd_kafka_new on success */
    rd_kafka_poll_set_consumer(rk);
    return rk;
}

/* ── ColBatch builder for a single Kafka message ── */

/* Infer a simple 1-column batch from raw payload when format is unknown */
static ColBatch *batch_from_raw(Arena *a, const char *payload, size_t payload_len,
                                 int64_t offset_val)
{
    /* Schema: two columns — offset (INT64), value (TEXT) */
    Schema *schema = arena_calloc(a, sizeof(Schema));
    schema->ncols  = 2;
    schema->cols   = arena_alloc(a, 2 * sizeof(ColDef));
    schema->cols[0].name     = "kafka_offset";
    schema->cols[0].type     = COL_INT64;
    schema->cols[0].nullable = false;
    schema->cols[1].name     = "value";
    schema->cols[1].type     = COL_TEXT;
    schema->cols[1].nullable = true;

    ColBatch *batch = arena_calloc(a, sizeof(ColBatch));
    batch->schema  = schema;
    batch->ncols   = 2;
    batch->nrows   = 1;

    int64_t *offsets = arena_alloc(a, sizeof(int64_t));
    offsets[0] = offset_val;
    batch->values[0] = offsets;
    batch->null_bitmap[0] = arena_calloc(a, 1);

    char **vals = arena_alloc(a, sizeof(char *));
    vals[0] = arena_strndup(a, payload, payload_len);
    batch->values[1] = vals;
    batch->null_bitmap[1] = arena_calloc(a, 1);

    return batch;
}

/* Build ColBatch from JSON message: {"key1": val1, "key2": val2, ...} */
static ColBatch *batch_from_json(Arena *a, const char *payload, size_t payload_len,
                                  int64_t offset_val)
{
    /* Use arena-backed json_parse */
    JVal *root = json_parse(a, payload, payload_len);
    if (!root || root->type != JV_OBJECT || root->nkeys == 0)
        return batch_from_raw(a, payload, payload_len, offset_val);

    /* Build schema from JSON keys, prepend kafka_offset column */
    size_t ncols = root->nkeys + 1;
    Schema *schema = arena_calloc(a, sizeof(Schema));
    schema->ncols  = (int)ncols;
    schema->cols   = arena_alloc(a, ncols * sizeof(ColDef));

    schema->cols[0].name     = "kafka_offset";
    schema->cols[0].type     = COL_INT64;
    schema->cols[0].nullable = false;

    for (size_t i = 0; i < root->nkeys; i++) {
        schema->cols[i + 1].name     = root->keys[i];
        schema->cols[i + 1].nullable = true;
        JVal *v = root->vals[i];
        if (!v || v->type == JV_NULL)
            schema->cols[i + 1].type = COL_TEXT;
        else if (v->type == JV_NUMBER)
            schema->cols[i + 1].type = COL_DOUBLE;
        else if (v->type == JV_BOOL)
            schema->cols[i + 1].type = COL_BOOL;
        else
            schema->cols[i + 1].type = COL_TEXT;
    }

    ColBatch *batch = arena_calloc(a, sizeof(ColBatch));
    batch->schema = schema;
    batch->ncols  = (int)ncols;
    batch->nrows  = 1;

    /* offset column */
    int64_t *offsets = arena_alloc(a, sizeof(int64_t));
    offsets[0] = offset_val;
    batch->values[0] = offsets;
    batch->null_bitmap[0] = arena_calloc(a, 1);

    /* data columns */
    for (size_t i = 0; i < root->nkeys; i++) {
        JVal *v = root->vals[i];
        ColType ct = schema->cols[i + 1].type;

        batch->null_bitmap[i + 1] = arena_calloc(a, 1);

        if (!v || v->type == JV_NULL) {
            char **sv = arena_alloc(a, sizeof(char *));
            sv[0] = NULL;
            batch->values[i + 1] = sv;
            batch->null_bitmap[i + 1][0] = 1;
        } else if (ct == COL_DOUBLE) {
            double *dv = arena_alloc(a, sizeof(double));
            dv[0] = v->n;
            batch->values[i + 1] = dv;
        } else if (ct == COL_BOOL) {
            int64_t *iv = arena_alloc(a, sizeof(int64_t));
            iv[0] = v->b ? 1 : 0;
            batch->values[i + 1] = iv;
        } else {
            char **sv = arena_alloc(a, sizeof(char *));
            sv[0] = arena_strndup(a, v->s, v->len);
            batch->values[i + 1] = sv;
        }
    }

    return batch;
}

/* ── CDC consumer thread ── */

static void *consumer_thread_fn(void *arg)
{
    KafkaCtx *ctx = (KafkaCtx *)arg;

    while (atomic_load(&ctx->consumer_running)) {
        rd_kafka_message_t *msg = rd_kafka_consumer_poll(ctx->rk, 100 /*ms*/);
        if (!msg) continue;

        if (msg->err) {
            if (msg->err != RD_KAFKA_RESP_ERR__PARTITION_EOF)
                LOG_WARN("kafka: consumer error: %s", rd_kafka_message_errstr(msg));
            rd_kafka_message_destroy(msg);
            continue;
        }

        /* Build event */
        Arena *ev_arena = arena_create(65536);
        ColBatch *batch = batch_from_raw(ev_arena,
                                          (const char *)msg->payload,
                                          msg->len,
                                          (int64_t)msg->offset);

        CdcEvent ev = {0};
        ev.op     = CDC_INSERT;
        ev.entity = ctx->topic_name;
        ev.after  = batch;
        ev.lsn    = (int64_t)msg->offset;

        if (ctx->cdc_handler)
            ctx->cdc_handler(&ev, ctx->cdc_userdata);

        arena_destroy(ev_arena);
        rd_kafka_message_destroy(msg);
    }

    return NULL;
}

/* ── Connector functions ── */

static void *kafka_create(const char *config_json, Arena *arena)
{
    KafkaCtx *ctx = arena_calloc(arena, sizeof(KafkaCtx));
    ctx->arena = arena;
    pthread_mutex_init(&ctx->buf_mu, NULL);
    pthread_mutex_init(&ctx->err_mu, NULL);
    atomic_store(&ctx->consumer_running, 0);
    atomic_store(&ctx->auth_failed, 0);

    /* defaults */
    snprintf(ctx->brokers,     sizeof(ctx->brokers),     "localhost:9092");
    snprintf(ctx->group_id,    sizeof(ctx->group_id),    "dfo-consumer");
    snprintf(ctx->data_format, sizeof(ctx->data_format), "json");

    if (config_json) {
        JVal *cfg = json_parse(arena, config_json, strlen(config_json));
        if (cfg && cfg->type == JV_OBJECT) {
            cfg_get(cfg, "brokers",     "bootstrap.servers", ctx->brokers,     sizeof(ctx->brokers));
            cfg_get(cfg, "group_id",    "group.id",          ctx->group_id,    sizeof(ctx->group_id));
            cfg_get(cfg, "topic",       NULL,                ctx->topic_name,  sizeof(ctx->topic_name));
            cfg_get(cfg, "data_format", NULL,                ctx->data_format, sizeof(ctx->data_format));

            cfg_get(cfg, "security_protocol", "security.protocol", ctx->security_protocol, sizeof(ctx->security_protocol));
            cfg_get(cfg, "sasl_mechanism",    "sasl.mechanism",    ctx->sasl_mechanism,    sizeof(ctx->sasl_mechanism));
            cfg_get(cfg, "sasl_username",     "sasl.username",     ctx->sasl_username,     sizeof(ctx->sasl_username));
            cfg_get(cfg, "sasl_password",     "sasl.password",     ctx->sasl_password,     sizeof(ctx->sasl_password));
            cfg_get(cfg, "ssl_ca_location",   "ssl.ca.location",   ctx->ssl_ca_location,   sizeof(ctx->ssl_ca_location));
            cfg_get(cfg, "ssl_certificate_location", "ssl.certificate.location", ctx->ssl_cert_location, sizeof(ctx->ssl_cert_location));
            cfg_get(cfg, "ssl_key_location",  "ssl.key.location",  ctx->ssl_key_location,  sizeof(ctx->ssl_key_location));
            cfg_get(cfg, "ssl_key_password",  "ssl.key.password",  ctx->ssl_key_password,  sizeof(ctx->ssl_key_password));

            JVal *nv = json_get(cfg, "ssl_no_verify");
            ctx->ssl_no_verify = json_bool(nv, false) ? 1 : 0;
        } else {
            /* Not valid JSON — fall back to the legacy scanner so pre-existing
             * plaintext configs keep working, but say so: SASL fields are not
             * read on this path because the scanner cannot handle escapes. */
            LOG_WARN("kafka: connector_config is not a JSON object, "
                     "falling back to legacy parsing (SASL fields ignored)");
            cfg_str(config_json, "\"brokers\"",     ctx->brokers,     sizeof(ctx->brokers));
            cfg_str(config_json, "\"group_id\"",    ctx->group_id,    sizeof(ctx->group_id));
            cfg_str(config_json, "\"topic\"",       ctx->topic_name,  sizeof(ctx->topic_name));
            cfg_str(config_json, "\"data_format\"", ctx->data_format, sizeof(ctx->data_format));
        }
    }

    normalize_protocol(ctx->security_protocol);
    normalize_mechanism(ctx->sasl_mechanism);

    /* Credentials without a protocol is the most common misconfiguration:
     * librdkafka then talks PLAINTEXT and the broker drops the connection
     * without ever running SASL. Infer the obvious protocol and say so. */
    if (!ctx->security_protocol[0] && ctx->sasl_username[0]) {
        snprintf(ctx->security_protocol, sizeof(ctx->security_protocol), "%s",
                 ctx->ssl_ca_location[0] ? "sasl_ssl" : "sasl_plaintext");
        LOG_WARN("kafka: sasl_username set without security_protocol — "
                 "assuming %s; set it explicitly if the broker expects otherwise",
                 ctx->security_protocol);
    }
    /* SASL selected but no mechanism → Kafka's own default is GSSAPI, which is
     * never what a username/password pair means. */
    if (strncmp(ctx->security_protocol, "sasl", 4) == 0 && !ctx->sasl_mechanism[0]) {
        snprintf(ctx->sasl_mechanism, sizeof(ctx->sasl_mechanism), "SCRAM-SHA-512");
        LOG_WARN("kafka: security_protocol=%s without sasl_mechanism — "
                 "defaulting to SCRAM-SHA-512", ctx->security_protocol);
    }

    char errstr[512];
    ctx->rk = make_consumer(ctx, errstr, sizeof(errstr));
    if (!ctx->rk) {
        LOG_ERROR("kafka: failed to create consumer: %s", errstr);
        ctx_set_error(ctx, errstr);
        return ctx;
    }

    if (ctx->sasl_mechanism[0])
        LOG_INFO("kafka: auth protocol=%s mechanism=%s user=%s",
                 ctx->security_protocol, ctx->sasl_mechanism,
                 ctx->sasl_username[0] ? ctx->sasl_username : "<unset>");

    /* Subscribe to topic */
    ctx->tplist = rd_kafka_topic_partition_list_new(1);
    rd_kafka_topic_partition_list_add(ctx->tplist, ctx->topic_name,
                                      RD_KAFKA_PARTITION_UA);

    rd_kafka_resp_err_t err = rd_kafka_subscribe(ctx->rk, ctx->tplist);
    if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
        LOG_ERROR("kafka: subscribe failed: %s", rd_kafka_err2str(err));
    } else {
        LOG_INFO("kafka: subscribed to topic=%s brokers=%s", ctx->topic_name, ctx->brokers);
    }

    /* rd_kafka_new() only queues the connection: with bad credentials it still
     * returns a handle, and every later poll just times out with no messages —
     * indistinguishable from an empty topic. Force one metadata round-trip so
     * a SASL handshake failure surfaces here, at create() time. */
    const struct rd_kafka_metadata *md = NULL;
    rd_kafka_resp_err_t merr = rd_kafka_metadata(ctx->rk, 0 /*only subscribed*/,
                                                 NULL, &md, 5000 /*ms*/);
    if (merr == RD_KAFKA_RESP_ERR_NO_ERROR) {
        rd_kafka_metadata_destroy(md);
    } else {
        char detail[384];
        err_detail(ctx, rd_kafka_err2str(merr), detail, sizeof(detail));
        LOG_ERROR("kafka: broker handshake failed: %s", detail);
    }

    return ctx;
}

static void kafka_destroy(void *vctx)
{
    KafkaCtx *ctx = (KafkaCtx *)vctx;
    if (!ctx) return;

    if (atomic_load(&ctx->consumer_running)) {
        atomic_store(&ctx->consumer_running, 0);
        pthread_join(ctx->consumer_thread, NULL);
    }

    if (ctx->rk) {
        rd_kafka_consumer_close(ctx->rk);
        rd_kafka_destroy(ctx->rk);
    }
    if (ctx->tplist)
        rd_kafka_topic_partition_list_destroy(ctx->tplist);

    pthread_mutex_destroy(&ctx->buf_mu);
    /* err_mu is deliberately NOT destroyed: error_cb runs on rdkafka's own
     * broker threads and may fire once more as they wind down. The mutex lives
     * in the host arena, so leaving it initialised costs nothing, whereas
     * locking a destroyed mutex is undefined behaviour. */

    /* NB: ctx (and ctx->arena) are owned by the HOST — ctx was allocated with
     * arena_calloc(host_arena, ...). Do NOT arena_destroy() the host's arena or
     * free() arena memory here (that corrupted the host heap and crashed the
     * gateway). Mirror pg_connector: release only the plugin's own resources. */
}

static int kafka_list_entities(void *vctx, Arena *a, DfoEntityList *out)
{
    KafkaCtx *ctx = (KafkaCtx *)vctx;

    out->items = arena_calloc(a, sizeof(DfoEntity));
    out->items[0].entity = arena_strdup(a, ctx->topic_name);
    out->items[0].type   = "stream";
    out->count = 1;
    return 0;
}

static int kafka_describe(void *vctx, Arena *a, const char *entity, Schema **out)
{
    KafkaCtx *ctx = (KafkaCtx *)vctx;
    if (!ctx->rk) return -1;
    (void)entity;

    /* Sample up to 10 messages for schema inference */
    Schema *schema = NULL;
    int sampled = 0;

    /* For CSV format: read first message, treat first line as header */
    /* For JSON format: read first message, extract keys */
    while (sampled < 10) {
        rd_kafka_message_t *msg = rd_kafka_consumer_poll(ctx->rk, 200 /*ms*/);
        if (!msg) {
            sampled++;
            continue;
        }
        if (msg->err) {
            rd_kafka_message_destroy(msg);
            sampled++;
            continue;
        }

        const char *payload = (const char *)msg->payload;
        size_t      plen    = msg->len;
        int64_t     offset  = (int64_t)msg->offset;

        if (strcasecmp(ctx->data_format, "csv") == 0) {
            /* First line = header */
            const char *eol = memchr(payload, '\n', plen);
            size_t hdr_len  = eol ? (size_t)(eol - payload) : plen;
            char *hdr = arena_strndup(a, payload, hdr_len);
            if (hdr[hdr_len - 1] == '\r') hdr[hdr_len - 1] = '\0';

            /* Count columns */
            int ncols = 1;
            for (const char *p = hdr; *p; p++)
                if (*p == ',') ncols++;

            schema = arena_calloc(a, sizeof(Schema));
            schema->ncols = ncols;
            schema->cols  = arena_alloc(a, ncols * sizeof(ColDef));
            const char *p = hdr;
            for (int i = 0; i < ncols; i++) {
                const char *start = p;
                while (*p && *p != ',') p++;
                schema->cols[i].name     = arena_strndup(a, start, (size_t)(p - start));
                schema->cols[i].type     = COL_TEXT;
                schema->cols[i].nullable = true;
                if (*p == ',') p++;
            }
        } else {
            /* JSON: parse and extract keys → schema */
            ColBatch *b = batch_from_json(a, payload, plen, offset);
            if (b) schema = b->schema;
        }

        rd_kafka_message_destroy(msg);
        if (schema) break;
        sampled++;
    }

    if (!schema && atomic_load(&ctx->auth_failed)) {
        char last[256] = "";
        ctx_get_error(ctx, last, sizeof(last));
        LOG_ERROR("kafka: describe aborted, authentication failed: %s", last);
        return -1;
    }

    if (!schema) {
        /* Fallback: two-column schema */
        schema = arena_calloc(a, sizeof(Schema));
        schema->ncols = 2;
        schema->cols  = arena_alloc(a, 2 * sizeof(ColDef));
        schema->cols[0].name = "kafka_offset"; schema->cols[0].type = COL_INT64; schema->cols[0].nullable = false;
        schema->cols[1].name = "value";        schema->cols[1].type = COL_TEXT;  schema->cols[1].nullable = true;
    }

    *out = schema;
    return 0;
}

static int kafka_read_batch(void *vctx, Arena *a, DfoReadReq *req,
                            const char *entity, ColBatch **out)
{
    KafkaCtx *ctx = (KafkaCtx *)vctx;
    if (!ctx->rk) return -1;
    (void)entity;

    int64_t limit = (req->limit > 0 && req->limit < BATCH_SIZE) ? req->limit : BATCH_SIZE;
    int64_t last_offset = -1;

    /* We collect individual batches (1 msg each) and merge into one */
    ColBatch **msgs     = arena_alloc(a, limit * sizeof(ColBatch *));
    int        msg_count = 0;

    int64_t deadline_ms = 1000; /* 1 second total timeout */
    int64_t elapsed_ms  = 0;

    while (msg_count < (int)limit && elapsed_ms < deadline_ms) {
        rd_kafka_message_t *msg = rd_kafka_consumer_poll(ctx->rk, 100 /*ms*/);
        elapsed_ms += 100;

        if (!msg) continue;
        if (msg->err) {
            if (msg->err != RD_KAFKA_RESP_ERR__PARTITION_EOF)
                LOG_WARN("kafka: poll error: %s", rd_kafka_message_errstr(msg));
            rd_kafka_message_destroy(msg);
            continue;
        }

        const char *payload = (const char *)msg->payload;
        size_t      plen    = msg->len;
        last_offset         = (int64_t)msg->offset;

        ColBatch *b;
        if (strcasecmp(ctx->data_format, "json") == 0)
            b = batch_from_json(a, payload, plen, last_offset);
        else
            b = batch_from_raw(a, payload, plen, last_offset);

        msgs[msg_count++] = b;
        rd_kafka_message_destroy(msg);
    }

    if (msg_count == 0) {
        *out = NULL;
        /* Distinguish "topic is idle" from "we never got past the SASL
         * handshake" — both look like zero messages from here. */
        if (atomic_load(&ctx->auth_failed)) {
            char last[256] = "";
            ctx_get_error(ctx, last, sizeof(last));
            LOG_ERROR("kafka: read aborted, authentication failed: %s", last);
            return -1;
        }
        return 0; /* no messages yet, caller may retry */
    }

    /* Use the schema from the first message */
    Schema *schema  = msgs[0]->schema;
    int     ncols   = schema->ncols;
    int     nrows   = msg_count;

    ColBatch *batch = arena_calloc(a, sizeof(ColBatch));
    batch->schema   = schema;
    batch->ncols    = ncols;
    batch->nrows    = nrows;

    /* Allocate merged value arrays */
    for (int c = 0; c < ncols; c++) {
        batch->null_bitmap[c] = arena_calloc(a, (nrows + 7) / 8);
        switch (schema->cols[c].type) {
            case COL_INT64:
            case COL_BOOL: {
                int64_t *iv = arena_alloc(a, nrows * sizeof(int64_t));
                for (int r = 0; r < msg_count; r++) {
                    ColBatch *src = msgs[r];
                    if (c < src->ncols && src->values[c]) {
                        iv[r] = ((int64_t *)src->values[c])[0];
                    } else {
                        iv[r] = 0;
                        batch->null_bitmap[c][r / 8] |= (1u << (r % 8));
                    }
                }
                batch->values[c] = iv;
                break;
            }
            case COL_DOUBLE: {
                double *dv = arena_alloc(a, nrows * sizeof(double));
                for (int r = 0; r < msg_count; r++) {
                    ColBatch *src = msgs[r];
                    if (c < src->ncols && src->values[c]) {
                        dv[r] = ((double *)src->values[c])[0];
                    } else {
                        dv[r] = 0.0;
                        batch->null_bitmap[c][r / 8] |= (1u << (r % 8));
                    }
                }
                batch->values[c] = dv;
                break;
            }
            default: {
                char **sv = arena_alloc(a, nrows * sizeof(char *));
                for (int r = 0; r < msg_count; r++) {
                    ColBatch *src = msgs[r];
                    if (c < src->ncols && src->values[c]) {
                        sv[r] = ((char **)src->values[c])[0];
                    } else {
                        sv[r] = NULL;
                        batch->null_bitmap[c][r / 8] |= (1u << (r % 8));
                    }
                }
                batch->values[c] = sv;
                break;
            }
        }
    }

    *out = batch;

    if (last_offset >= 0)
        *(const char **)&req->cursor = arena_sprintf(a, "offset:%lld", (long long)last_offset);

    return 0;
}

static int kafka_cdc_start(void *vctx, DfoCdcHandler handler, void *userdata)
{
    KafkaCtx *ctx = (KafkaCtx *)vctx;
    if (!ctx->rk) return -1;
    if (atomic_load(&ctx->consumer_running)) return 0; /* already running */

    ctx->cdc_handler  = handler;
    ctx->cdc_userdata = userdata;
    atomic_store(&ctx->consumer_running, 1);

    int rc = pthread_create(&ctx->consumer_thread, NULL, consumer_thread_fn, ctx);
    if (rc != 0) {
        LOG_ERROR("kafka: pthread_create failed: %d", rc);
        atomic_store(&ctx->consumer_running, 0);
        return -1;
    }
    LOG_INFO("kafka: CDC started for topic=%s", ctx->topic_name);
    return 0;
}

static int kafka_cdc_stop(void *vctx)
{
    KafkaCtx *ctx = (KafkaCtx *)vctx;
    if (!atomic_load(&ctx->consumer_running)) return 0;

    atomic_store(&ctx->consumer_running, 0);
    pthread_join(ctx->consumer_thread, NULL);
    LOG_INFO("kafka: CDC stopped");
    return 0;
}

static int kafka_ping(void *vctx)
{
    KafkaCtx *ctx = (KafkaCtx *)vctx;
    if (!ctx->rk) return -1;

    const struct rd_kafka_metadata *meta = NULL;
    rd_kafka_resp_err_t err = rd_kafka_metadata(ctx->rk, 1 /*all_topics*/,
                                                 NULL, &meta, 3000 /*ms*/);
    if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
        char detail[384];
        err_detail(ctx, rd_kafka_err2str(err), detail, sizeof(detail));
        LOG_WARN("kafka: ping failed: %s", detail);
        return -1;
    }
    rd_kafka_metadata_destroy(meta);
    LOG_INFO("kafka: ping OK, brokers=%s", ctx->brokers);
    return 0;
}

/* ── Entry point ── */

/* Exported as a DATA symbol (const struct) — the loader dlsym()s this name and
 * reads it as a DfoConnector*, exactly like every other plugin. (It used to be
 * a function returning the struct, which the loader mis-read as the struct
 * itself → garbage abi_version. Never caught because the .so never built.) */
const DfoConnector dfo_connector_entry = {
    .abi_version   = DFO_CONNECTOR_ABI_VERSION,
    .name          = "kafka",
    .version       = "1.0.0",
    .description   = "Apache Kafka streaming connector",
    .create        = kafka_create,
    .destroy       = kafka_destroy,
    .list_entities = kafka_list_entities,
    .describe      = kafka_describe,
    .read_batch    = kafka_read_batch,
    .cdc_start     = kafka_cdc_start,
    .cdc_stop      = kafka_cdc_stop,
    .ping          = kafka_ping,
};
