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

    DfoCdcHandler   cdc_handler;
    void           *cdc_userdata;
    pthread_t       consumer_thread;
    atomic_int      consumer_running;

    ColBatch       *buffer[KAFKA_BUF_SIZE];
    int             buf_head, buf_tail;
    pthread_mutex_t buf_mu;
    Arena          *arena;
} KafkaCtx;

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

/* ── Kafka rdkafka helpers ── */

static rd_kafka_t *make_consumer(const char *brokers, const char *group_id,
                                  char *errstr, size_t errlen)
{
    rd_kafka_conf_t *conf = rd_kafka_conf_new();

    if (rd_kafka_conf_set(conf, "bootstrap.servers", brokers,
                          errstr, errlen) != RD_KAFKA_CONF_OK) {
        rd_kafka_conf_destroy(conf);
        return NULL;
    }
    if (rd_kafka_conf_set(conf, "group.id", group_id,
                          errstr, errlen) != RD_KAFKA_CONF_OK) {
        rd_kafka_conf_destroy(conf);
        return NULL;
    }
    /* Start from earliest unconsumed offset */
    if (rd_kafka_conf_set(conf, "auto.offset.reset", "earliest",
                          errstr, errlen) != RD_KAFKA_CONF_OK) {
        rd_kafka_conf_destroy(conf);
        return NULL;
    }

    rd_kafka_t *rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, errlen);
    if (!rk) return NULL;

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
    atomic_store(&ctx->consumer_running, 0);

    /* defaults */
    snprintf(ctx->brokers,     sizeof(ctx->brokers),     "localhost:9092");
    snprintf(ctx->group_id,    sizeof(ctx->group_id),    "dfo-consumer");
    snprintf(ctx->data_format, sizeof(ctx->data_format), "json");

    if (config_json) {
        cfg_str(config_json, "\"brokers\"",     ctx->brokers,     sizeof(ctx->brokers));
        cfg_str(config_json, "\"group_id\"",    ctx->group_id,    sizeof(ctx->group_id));
        cfg_str(config_json, "\"topic\"",       ctx->topic_name,  sizeof(ctx->topic_name));
        cfg_str(config_json, "\"data_format\"", ctx->data_format, sizeof(ctx->data_format));
    }

    char errstr[512];
    ctx->rk = make_consumer(ctx->brokers, ctx->group_id, errstr, sizeof(errstr));
    if (!ctx->rk) {
        LOG_ERROR("kafka: failed to create consumer: %s", errstr);
        return ctx;
    }

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
        LOG_WARN("kafka: ping failed: %s", rd_kafka_err2str(err));
        return -1;
    }
    rd_kafka_metadata_destroy(meta);
    LOG_INFO("kafka: ping OK, brokers=%s", ctx->brokers);
    return 0;
}

/* ── Sink: produce one JSON message per row to a topic ──
 * Topic = `entity` if given, else the configured "topic". A fresh producer is
 * created per call, all rows are produced, then flushed. mode is ignored (Kafka
 * is an append-only log). Returns rows produced or <0 on error.
 *
 * NOTE: compiles and follows the standard rdkafka producer flow, but not
 * exercised against a live broker in this environment. */
static void kafka_json_escape(char **buf, size_t *len, size_t *cap, const char *s) {
    for (const char *p = s ? s : ""; *p; p++) {
        char esc[8]; const char *w; int n;
        switch (*p) {
            case '"':  w="\\\""; n=2; break;  case '\\': w="\\\\"; n=2; break;
            case '\n': w="\\n";  n=2; break;  case '\r': w="\\r";  n=2; break;
            case '\t': w="\\t";  n=2; break;
            default:
                if ((unsigned char)*p < 0x20) { snprintf(esc,sizeof(esc),"\\u%04x",*p); w=esc; n=6; }
                else { esc[0]=*p; w=esc; n=1; }
        }
        if (*len + (size_t)n + 1 > *cap) { while (*len+(size_t)n+1 > *cap) *cap*=2; *buf=realloc(*buf,*cap); }
        memcpy(*buf+*len, w, (size_t)n); *len += (size_t)n;
    }
}

static int kafka_write_batch(void *vctx, Arena *a, const char *entity,
                             const Schema *schema, const ColBatch *batch, int mode) {
    (void)a; (void)mode;
    KafkaCtx *ctx = vctx;
    const char *topic = (entity && entity[0]) ? entity : ctx->topic_name;
    if (!topic || !topic[0]) { LOG_ERROR("kafka sink: no topic"); return -1; }
    if (!ctx->brokers[0])    { LOG_ERROR("kafka sink: no brokers"); return -1; }

    char errstr[512];
    rd_kafka_conf_t *conf = rd_kafka_conf_new();
    if (rd_kafka_conf_set(conf, "bootstrap.servers", ctx->brokers, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        LOG_ERROR("kafka sink: conf: %s", errstr); rd_kafka_conf_destroy(conf); return -1;
    }
    rd_kafka_t *rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
    if (!rk) { LOG_ERROR("kafka sink: producer: %s", errstr); return -1; }
    rd_kafka_topic_t *rkt = rd_kafka_topic_new(rk, topic, NULL);
    if (!rkt) { LOG_ERROR("kafka sink: topic_new failed"); rd_kafka_destroy(rk); return -1; }

    int produced = 0;
    char numbuf[64];
    for (int r = 0; r < batch->nrows; r++) {
        size_t cap = 256, len = 0; char *msg = malloc(cap);
        msg[len++] = '{';
        for (int c = 0; c < batch->ncols; c++) {
            if (c) { msg[len++]=','; }
            msg[len++]='"'; kafka_json_escape(&msg,&len,&cap,schema->cols[c].name);
            if (len+2>cap){cap*=2;msg=realloc(msg,cap);} msg[len++]='"'; msg[len++]=':';
            const uint8_t *bm = batch->null_bitmap[c];
            if (bm && ((bm[r/8]>>(r%8))&1u)) {
                if (len+4>cap){cap*=2;msg=realloc(msg,cap);} memcpy(msg+len,"null",4); len+=4; continue;
            }
            const char *v;
            switch (schema->cols[c].type) {
                case COL_INT64:  snprintf(numbuf,sizeof(numbuf),"%lld",(long long)((int64_t*)batch->values[c])[r]); v=numbuf; break;
                case COL_DOUBLE: snprintf(numbuf,sizeof(numbuf),"%.10g",((double*)batch->values[c])[r]); v=numbuf; break;
                default:         v=((char**)batch->values[c])[r]; break;
            }
            if (len+1>cap){cap*=2;msg=realloc(msg,cap);} msg[len++]='"';
            kafka_json_escape(&msg,&len,&cap,v);
            if (len+1>cap){cap*=2;msg=realloc(msg,cap);} msg[len++]='"';
        }
        if (len+1>cap){cap*=2;msg=realloc(msg,cap);} msg[len++]='}';
        int rc = rd_kafka_produce(rkt, RD_KAFKA_PARTITION_UA, RD_KAFKA_MSG_F_COPY,
                                  msg, len, NULL, 0, NULL);
        free(msg);
        if (rc == -1) { LOG_ERROR("kafka sink: produce: %s",
                                  rd_kafka_err2str(rd_kafka_last_error())); }
        else produced++;
        rd_kafka_poll(rk, 0);
    }
    rd_kafka_flush(rk, 10000);
    int unsent = rd_kafka_outq_len(rk);
    rd_kafka_topic_destroy(rkt);
    rd_kafka_destroy(rk);
    if (unsent > 0) { LOG_ERROR("kafka sink: %d messages not delivered", unsent); return -1; }
    LOG_INFO("kafka sink: produced %d rows → topic '%s'", produced, topic);
    return produced;
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
    .write_batch   = kafka_write_batch,
};
