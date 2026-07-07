/* kafka_connector.c — коннектор Kafka (источник + приёмник), ABI v3.
 *
 * Транспорт: librdkafka. Источник — consumer, читающий один топик; приёмник —
 * producer, публикующий по одному JSON-сообщению на строку. Плагин экспортирует
 * таблицу ABI как data-символ dfo_connector_entry (см. конец файла).
 *
 * Форматы payload (config data_format): "json" | "csv" | "avro".
 *   json — сообщение = объект; ключи → колонки.
 *   csv  — первая строка сэмпла = заголовок.
 *   avro — Confluent-обёртка [0x00][schema_id:4 BE][Avro binary]; схема тянется
 *          из Schema Registry по HTTP (libcurl) и кэшируется по id.
 * Разбор Avro-схемы/двоичного слоя — в avro_record.h / avro_decode.h.
 * Ко всем строкам применяется text-always модель: значения всегда char*,
 * тип колонки в схеме — лишь метаданные (native int64/double → сегфолт ядра).
 */
/* Kafka streaming connector — ABI v2 (JSON/CSV/Avro + Schema Registry) */
#include "../../../connector/connector.h"
#include "../../../core/arena.h"
#include "../../../core/json.h"
#include "../../../core/log.h"
#include "avro_record.h"   /* AvroType/AvroField/AvroSchemaNode + parse/decode */
#include <librdkafka/rdkafka.h>
/* RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_ID was added in librdkafka 2.1.0 (KIP-516).
 * On older clients — e.g. RedOS 8 / EL9 ship librdkafka 1.6.1 — the enum does
 * not exist AND is never returned by the broker, so alias it to
 * UNKNOWN_TOPIC_OR_PART: the topic-does-not-exist check keeps compiling and the
 * behaviour is identical (that branch and this one both report "does not exist"). */
#if !defined(RD_KAFKA_VERSION) || RD_KAFKA_VERSION < 0x020100ff
#define RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_ID RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART
#endif
#include <curl/curl.h>
#include <pthread.h>
#include <string.h>
#include <time.h>
#include <strings.h>   /* strcasecmp */
#include <stdlib.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>

/* ── Context ── */

#define KAFKA_BUF_SIZE 256

/* ── Avro / Confluent Schema Registry ──
 * The schema model (AvroType/AvroField/AvroSchemaNode) and the parse/decode
 * logic live in avro_record.h (unit-tested without librdkafka/curl). Only the
 * Registry HTTP client + the id→schema cache are connector-local. */
typedef struct AvroSchemaCache {
    AvroSchemaNode *head;
    Arena          *arena;
    pthread_mutex_t mu;
} AvroSchemaCache;

/* Контекст одного экземпляра коннектора (аллоцируется в host-арене).
 * Хранит настройки из config_json, живой consumer rk с подпиской, кэш Avro-схем
 * и состояние фонового CDC-потока. Все char-поля инициализируются дефолтами в
 * kafka_create и опционально перекрываются из config_json. */
typedef struct {
    rd_kafka_t                        *rk;
    rd_kafka_topic_partition_list_t   *tplist;
    bool  assigned;          /* read_batch: партиции уже назначены (assign) — не переназначаем при пагинации */
    bool  explicit_group;    /* задан явный group_id → инкремент (committed offset + commit), иначе read-all */
    char  brokers[512];
    char  group_id[128];
    char  topic_name[128];
    char  data_format[16];   /* "json" | "csv" | "avro" */
    char  addr_family[8];    /* broker.address.family: v4 | v6 | any (default v4) */

    /* Security (managed Kafka: Confluent Cloud / MSK / Aiven need SASL_SSL) */
    char  security_protocol[32];  /* PLAINTEXT | SSL | SASL_PLAINTEXT | SASL_SSL */
    char  sasl_mechanism[32];     /* PLAIN | SCRAM-SHA-256 | SCRAM-SHA-512 | GSSAPI */
    char  sasl_username[256];
    char  sasl_password[256];
    /* Kerberos (sasl_mechanism = GSSAPI). Логин/пароль не используются —
     * аутентификация по keytab+principal либо по внешнему кэшу билетов. */
    char  sasl_kerberos_service_name[128]; /* имя сервис-принципала брокера, обычно "kafka" */
    char  sasl_kerberos_principal[256];    /* клиентский принципал, напр. user@REALM */
    char  sasl_kerberos_keytab[512];       /* путь к keytab на хосте gateway */
    /* TLS (security_protocol SSL | SASL_SSL). Пути к файлам на хосте gateway. */
    char  ssl_ca_location[512];          /* CA-сертификат для проверки брокера (PEM) */
    char  ssl_certificate_location[512]; /* клиентский сертификат для mTLS (PEM) */
    char  ssl_key_location[512];         /* клиентский приватный ключ для mTLS (PEM) */
    char  ssl_key_password[256];         /* пароль ключа, если зашифрован */
    char  offset_reset[16];       /* earliest | latest (default earliest) */
    char  last_err[512];          /* human-readable reason of the last failure */

    /* Avro / Schema Registry (data_format == "avro") */
    char  schema_registry_url[512];  /* e.g. "http://registry:8081" */
    char  sr_auth[256];              /* optional "user:pass" basic auth */
    AvroSchemaCache *schema_cache;   /* id → parsed Avro schema (lazy) */

    /* Set by rd_kafka error_cb when librdkafka reports that every configured
     * broker is unreachable (RD_KAFKA_RESP_ERR__ALL_BROKERS_DOWN). Lets a read
     * distinguish "broker down" (hard error) from "topic drained" (0 rows OK). */
    atomic_int      all_brokers_down;

    DfoCdcHandler   cdc_handler;
    void           *cdc_userdata;
    pthread_t       consumer_thread;
    atomic_int      consumer_running;

    ColBatch       *buffer[KAFKA_BUF_SIZE];
    int             buf_head, buf_tail;
    pthread_mutex_t buf_mu;
    Arena          *arena;
} KafkaCtx;

/* libcurl global init exactly once across all connector instances. */
static pthread_once_t kafka_curl_once = PTHREAD_ONCE_INIT;
static void kafka_curl_global_init(void) { curl_global_init(CURL_GLOBAL_DEFAULT); }

/* ── JSON config parser helpers ── */

/* Извлекает строковое значение по ключу "key" из плоского JSON-конфига в dst.
 * Наивный парсер (strstr/strchr), достаточный для одноуровневого config_json;
 * при отсутствии ключа dst не трогается (остаётся значение по умолчанию). */
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

/* rd_kafka error callback. librdkafka delivers asynchronous, cluster-level
 * errors here (the per-message ->err only covers consume-time issues). We latch
 * RD_KAFKA_RESP_ERR__ALL_BROKERS_DOWN so a subsequent read_batch can fail loudly
 * instead of silently returning 0 rows when nothing is actually reachable. The
 * opaque is the KafkaCtx (set via rd_kafka_conf_set_opaque in make_consumer). */
static void kafka_error_cb(rd_kafka_t *rk, int err, const char *reason, void *opaque)
{
    (void)rk;
    KafkaCtx *ctx = (KafkaCtx *)opaque;
    if (!ctx) return;
    if ((rd_kafka_resp_err_t)err == RD_KAFKA_RESP_ERR__ALL_BROKERS_DOWN) {
        atomic_store(&ctx->all_brokers_down, 1);
        snprintf(ctx->last_err, sizeof(ctx->last_err),
                 "all brokers down (%s): %s",
                 ctx->brokers, reason ? reason : "no reachable broker");
        LOG_WARN("kafka: all brokers down: %s", reason ? reason : "");
    } else {
        LOG_WARN("kafka: error_cb [%s]: %s",
                 rd_kafka_err2name((rd_kafka_resp_err_t)err), reason ? reason : "");
    }
}

/* Apply SASL/SSL security settings to a conf (consumer or producer). Each key
 * is set only when non-empty, so plaintext clusters keep working unchanged.
 * Managed Kafka (Confluent Cloud / MSK / Aiven) typically needs:
 *   security.protocol=SASL_SSL, sasl.mechanism=PLAIN, sasl.username/password. */
static void kafka_apply_security(rd_kafka_conf_t *conf, const KafkaCtx *ctx,
                                 char *errstr, size_t errlen)
{
    if (ctx->security_protocol[0])
        rd_kafka_conf_set(conf, "security.protocol", ctx->security_protocol, errstr, errlen);
    if (ctx->sasl_mechanism[0])
        rd_kafka_conf_set(conf, "sasl.mechanism", ctx->sasl_mechanism, errstr, errlen);
    else if (strncmp(ctx->security_protocol, "SASL", 4) == 0)
        /* SASL-протокол выбран, но механизм не задан. У librdkafka дефолт
         * sasl.mechanism = GSSAPI (Kerberos) → без keytab она сразу падает с
         * "Invalid sasl.kerberos.kinit.cmd value: Property not available:
         * sasl.kerberos.keytab", и SASL-подключение по логину/паролю не
         * работает вовсе. Разумный дефолт для username/password — PLAIN. */
        rd_kafka_conf_set(conf, "sasl.mechanism", "PLAIN", errstr, errlen);
    /* GSSAPI (Kerberos) — работает «под капотом», без секретов в конфиге
     * конвейера. Всё берётся из окружения хоста gateway, настроенного ops:
     *   - /etc/krb5.conf (realm → KDC) — обязателен;
     *   - keytab+principal из ENV (KAFKA_KERBEROS_KEYTAB / _PRINCIPAL) →
     *     librdkafka сама держит билет свежим через kinit;
     *   - либо, если keytab не задан, внешний кэш билетов (системный kinit /
     *     cron): отключаем внутренний kinit, чтобы librdkafka не падала на
     *     отсутствующем keytab, и используем готовый ccache (KRB5CCNAME).
     * service.name (сервис-принципал брокера) — из ENV или дефолт "kafka".
     * Поля sasl_kerberos_* в конфиге — необязательный override для API/YAML. */
    if (strcmp(ctx->sasl_mechanism, "GSSAPI") == 0) {
        const char *svc = getenv("KAFKA_KERBEROS_SERVICE_NAME");
        if (!svc || !svc[0]) svc = ctx->sasl_kerberos_service_name[0] ? ctx->sasl_kerberos_service_name : "kafka";
        rd_kafka_conf_set(conf, "sasl.kerberos.service.name", svc, errstr, errlen);

        const char *keytab = getenv("KAFKA_KERBEROS_KEYTAB");
        if (!keytab || !keytab[0]) keytab = ctx->sasl_kerberos_keytab;
        const char *princ  = getenv("KAFKA_KERBEROS_PRINCIPAL");
        if (!princ || !princ[0]) princ = ctx->sasl_kerberos_principal;

        if (keytab && keytab[0] && princ && princ[0]) {
            rd_kafka_conf_set(conf, "sasl.kerberos.keytab", keytab, errstr, errlen);
            rd_kafka_conf_set(conf, "sasl.kerberos.principal", princ, errstr, errlen);
        } else {
            /* Внешний кэш билетов (kinit сделан заранее — cron/системно):
             * заменяем дефолтный kinit.cmd на no-op "true". Пустая строка НЕ
             * годится — librdkafka всё равно валидирует дефолт со ссылкой на
             * %{sasl.kerberos.keytab} и падает; "true" убирает эту ссылку и
             * отключает внутренний kinit, а билет берётся из готового ccache
             * (KRB5CCNAME). */
            rd_kafka_conf_set(conf, "sasl.kerberos.kinit.cmd", "true", errstr, errlen);
        }
    } else {
        /* PLAIN/SCRAM — по логину/паролю. */
        if (ctx->sasl_username[0])
            rd_kafka_conf_set(conf, "sasl.username", ctx->sasl_username, errstr, errlen);
        if (ctx->sasl_password[0])
            rd_kafka_conf_set(conf, "sasl.password", ctx->sasl_password, errstr, errlen);
    }
    /* TLS: CA для проверки брокера + клиентский cert/key для mTLS. Задаются
     * только при непустом значении, так что PLAINTEXT/SASL_PLAINTEXT не трогаются.
     * Проверку сертификата брокера НЕ отключаем (безопасность по умолчанию). */
    if (ctx->ssl_ca_location[0])
        rd_kafka_conf_set(conf, "ssl.ca.location", ctx->ssl_ca_location, errstr, errlen);
    if (ctx->ssl_certificate_location[0])
        rd_kafka_conf_set(conf, "ssl.certificate.location", ctx->ssl_certificate_location, errstr, errlen);
    if (ctx->ssl_key_location[0])
        rd_kafka_conf_set(conf, "ssl.key.location", ctx->ssl_key_location, errstr, errlen);
    if (ctx->ssl_key_password[0])
        rd_kafka_conf_set(conf, "ssl.key.password", ctx->ssl_key_password, errstr, errlen);
}

/* Создаёт consumer rd_kafka: задаёт брокеры, group.id, auto.offset.reset и
 * применяет security-настройки. Возвращает NULL при ошибке конфигурации. */
static rd_kafka_t *make_consumer(const KafkaCtx *ctx, char *errstr, size_t errlen)
{
    rd_kafka_conf_t *conf = rd_kafka_conf_new();

    if (rd_kafka_conf_set(conf, "bootstrap.servers", ctx->brokers,
                          errstr, errlen) != RD_KAFKA_CONF_OK) {
        rd_kafka_conf_destroy(conf);
        return NULL;
    }
    if (rd_kafka_conf_set(conf, "group.id", ctx->group_id,
                          errstr, errlen) != RD_KAFKA_CONF_OK) {
        rd_kafka_conf_destroy(conf);
        return NULL;
    }
    /* Where to start when no committed offset: earliest (default) | latest. */
    const char *off = ctx->offset_reset[0] ? ctx->offset_reset : "earliest";
    if (rd_kafka_conf_set(conf, "auto.offset.reset", off,
                          errstr, errlen) != RD_KAFKA_CONF_OK) {
        rd_kafka_conf_destroy(conf);
        return NULL;
    }
    /* Не коммитим оффсеты автоматически: с эфемерной группой (см. kafka_create)
     * это даёт «читать всё каждый раз» — каждый запуск читает топик с начала и
     * ничего не оставляет закоммиченным. Инкрементальность (только новые
     * сообщения) достигается явным устойчивым group_id в конфиге шага. */
    rd_kafka_conf_set(conf, "enable.auto.commit", "false", errstr, errlen);
    /* Force IPv4 by default: "localhost" resolves to ::1 first on many hosts,
     * but most Kafka/Redpanda brokers listen only on IPv4 — librdkafka then
     * reports "all brokers down". Overridable via broker_address_family. */
    rd_kafka_conf_set(conf, "broker.address.family",
                      ctx->addr_family[0] ? ctx->addr_family : "v4", errstr, errlen);

    kafka_apply_security(conf, ctx, errstr, errlen);

    /* Latch cluster-level errors (all-brokers-down) into ctx via the opaque so a
     * read can fail instead of silently returning 0 rows. ctx is the connector's
     * own context; the cast drops const only because the conf API is untyped. */
    rd_kafka_conf_set_opaque(conf, (void *)ctx);
    rd_kafka_conf_set_error_cb(conf, kafka_error_cb);

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
    schema->cols[0].type     = COL_INT64;  /* logical INT64; the value is stored as
                                            * text (text-always model) so there is
                                            * no native read-back crash, while the
                                            * catalog type renders kafka_offset as a
                                            * JSON number on output. */
    schema->cols[0].nullable = false;
    schema->cols[1].name     = "value";
    schema->cols[1].type     = COL_TEXT;
    schema->cols[1].nullable = true;

    ColBatch *batch = arena_calloc(a, sizeof(ColBatch));
    batch->schema  = schema;
    batch->ncols   = 2;
    batch->nrows   = 1;

    char **offsets = arena_alloc(a, sizeof(char *));
    char obuf[24]; snprintf(obuf, sizeof(obuf), "%lld", (long long)offset_val);
    offsets[0] = arena_strdup(a, obuf);
    batch->values[0] = offsets;
    batch->null_bitmap[0] = arena_calloc(a, 1);

    char **vals = arena_alloc(a, sizeof(char *));
    vals[0] = arena_strndup(a, payload, payload_len);
    batch->values[1] = vals;
    batch->null_bitmap[1] = arena_calloc(a, 1);

    return batch;
}

/* Serialize a JVal sub-document back to JSON text. A nested array/object value
 * must be re-serialized — JVal is a union where {s,len} aliases {items,nitems},
 * so copying v->s/v->len for a container yields garbage bytes (J2). */
static void kafka_jval_write(JBuf *jb, JVal *v) {
    if (!v) { jb_null(jb); return; }
    switch (v->type) {
        case JV_BOOL:   jb_bool(jb, v->b); break;
        case JV_NUMBER: jb_double(jb, v->n); break;
        case JV_STRING: jb_strn(jb, v->s, v->len); break;
        case JV_ARRAY:
            jb_arr_begin(jb);
            for (size_t i = 0; i < v->nitems; i++) kafka_jval_write(jb, v->items[i]);
            jb_arr_end(jb);
            break;
        case JV_OBJECT:
            jb_obj_begin(jb);
            for (size_t i = 0; i < v->nkeys; i++) { jb_key(jb, v->keys[i]); kafka_jval_write(jb, v->vals[i]); }
            jb_obj_end(jb);
            break;
        default: jb_null(jb); break;   /* JV_NULL / JV_ERROR */
    }
}

/* True for a string that is a valid bare JSON number literal — used to decide
 * whether a text value may be emitted UNQUOTED for a typed numeric column. */
static bool kafka_is_json_num(const char *s) {
    if (!s || !*s) return false;
    const char *p = s; if (*p == '-') p++;
    bool dig = false;
    while (*p >= '0' && *p <= '9') { p++; dig = true; }
    if (*p == '.') { p++; while (*p >= '0' && *p <= '9') { p++; dig = true; } }
    if (*p == 'e' || *p == 'E') { p++; if (*p=='+'||*p=='-') p++; bool e=false;
        while (*p>='0'&&*p<='9'){p++;e=true;} if(!e) return false; }
    return dig && *p == '\0';
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
    schema->cols[0].type     = COL_INT64;  /* logical INT64; the value is stored as
                                            * text (text-always model) so there is
                                            * no native read-back crash, while the
                                            * catalog type renders kafka_offset as a
                                            * JSON number on output. */
    schema->cols[0].nullable = false;

    for (size_t i = 0; i < root->nkeys; i++) {
        schema->cols[i + 1].name     = root->keys[i];
        schema->cols[i + 1].nullable = true;
        JVal *v = root->vals[i];
        if (!v || v->type == JV_NULL)
            schema->cols[i + 1].type = COL_TEXT;
        else if (v->type == JV_NUMBER) {
            /* Store JSON numbers NATIVELY, matching json_http/siebel and the CSV
             * contract: values[c] holds a native int64 array for INT64 columns and
             * a native double array for DOUBLE columns. Integer-valued numbers give
             * an exact INT64 (account ids, counts fitting in 63 bits); fractional
             * ones give a DOUBLE. The previous code declared COL_DOUBLE but stored a
             * char*, so the merge (kafka_read_batch) and the preview endpoint read
             * that pointer back as a native number and returned garbage (a heap
             * address / denormal double). Values wider than 63 bits or needing exact
             * decimals arrive as JSON strings (COL_TEXT) and stay exact. */
            int64_t iv = (int64_t)v->n;
            schema->cols[i + 1].type = ((double)iv == v->n) ? COL_INT64 : COL_DOUBLE;
        }
        else if (v->type == JV_BOOL)
            /* TEXT-always: bool хранится текстом "true"/"false" (значение — char*).
             * Раньше схема была COL_BOOL, а значение native int64 → ядро читало
             * его как указатель (0/1) → сегфолт (как в json_http). */
            schema->cols[i + 1].type = COL_TEXT;
        else
            schema->cols[i + 1].type = COL_TEXT;
    }

    ColBatch *batch = arena_calloc(a, sizeof(ColBatch));
    batch->schema = schema;
    batch->ncols  = (int)ncols;
    batch->nrows  = 1;

    /* offset column — native INT64 (COL_INT64), matching batch_from_raw and the
     * CSV/json_http contract; the merge + preview read values[0] as int64_t*. */
    int64_t *offsets = arena_alloc(a, sizeof(int64_t));
    offsets[0] = offset_val;
    batch->values[0] = offsets;
    batch->null_bitmap[0] = arena_calloc(a, 1);

    /* data columns */
    for (size_t i = 0; i < root->nkeys; i++) {
        JVal *v = root->vals[i];

        batch->null_bitmap[i + 1] = arena_calloc(a, 1);

        if (!v || v->type == JV_NULL) {
            /* NULL contract: a missing/JSON-null field is materialized as the
             * DFO_NULL_SENTINEL string (not a NULL pointer, not ""). The null
             * bitmap is kept set so any null-aware consumer also sees it. */
            char **sv = arena_alloc(a, sizeof(char *));
            sv[0] = arena_strdup(a, DFO_NULL_SENTINEL);
            batch->values[i + 1] = sv;
            batch->null_bitmap[i + 1][0] = 1;
        } else if (v->type == JV_NUMBER) {
            /* Native storage matching the column type decided above: integer-valued
             * numbers → int64_t*, fractional → double*. This is exactly what the
             * merge and the preview endpoint read back (both dispatch on the
             * declared column type), so no pointer-as-number corruption. */
            if (schema->cols[i + 1].type == COL_INT64) {
                int64_t *nv = arena_alloc(a, sizeof(int64_t));
                nv[0] = (int64_t)v->n;
                batch->values[i + 1] = nv;
            } else {
                double *nv = arena_alloc(a, sizeof(double));
                nv[0] = v->n;
                batch->values[i + 1] = nv;
            }
        } else if (v->type == JV_BOOL) {
            /* TEXT-always: значение — char* "true"/"false" (не native int64). */
            char **sv = arena_alloc(a, sizeof(char *));
            sv[0] = arena_strdup(a, v->b ? "true" : "false");
            batch->values[i + 1] = sv;
        } else {
            char **sv = arena_alloc(a, sizeof(char *));
            if (v->type == JV_STRING) {
                sv[0] = arena_strndup(a, v->s, v->len);
            } else if (v->type == JV_ARRAY || v->type == JV_OBJECT) {
                /* Re-serialize the sub-document to JSON text. arena_strndup(v->s,
                 * v->len) read the union's items/nitems as a string -> N garbage
                 * non-UTF-8 bytes (J2), corrupting e.g. the ows.CLIENT array. */
                JBuf jb; jb_init(&jb, a, 128);
                kafka_jval_write(&jb, v);
                sv[0] = (char *)jb_done(&jb);
            } else {
                sv[0] = arena_strdup(a, "");   /* JV_NULL / unexpected */
            }
            batch->values[i + 1] = sv;
        }
    }

    return batch;
}

/* ── Avro: Confluent Schema Registry client ── */

/* Растущий буфер для накопления HTTP-ответа Schema Registry. */
struct curl_buf { char *data; size_t len, cap; };

/* libcurl write-callback: дописывает полученный кусок в curl_buf, при нехватке
 * места реаллоцирует; возврат 0 сигнализирует curl об ошибке (OOM). */
static size_t sr_write_cb(void *ptr, size_t size, size_t nmemb, void *userdata)
{
    struct curl_buf *buf = (struct curl_buf *)userdata;
    size_t add = size * nmemb;
    if (buf->len + add + 1 > buf->cap) {
        size_t ncap = (buf->len + add + 1) * 2;
        char  *nd   = realloc(buf->data, ncap);
        if (!nd) return 0;   /* signal error to curl */
        buf->data = nd; buf->cap = ncap;
    }
    memcpy(buf->data + buf->len, ptr, add);
    buf->len += add;
    buf->data[buf->len] = '\0';
    return add;
}

/* GET {registry}/schemas/ids/{id} → returns the unescaped Avro schema JSON
 * (malloc'd, caller frees) or NULL on any error. */
static char *sr_fetch_schema(KafkaCtx *ctx, int32_t schema_id)
{
    if (!ctx->schema_registry_url[0]) {
        LOG_ERROR("kafka: data_format=avro but schema_registry_url is unset");
        return NULL;
    }
    CURL *curl = curl_easy_init();
    if (!curl) return NULL;

    char url[768];
    snprintf(url, sizeof(url), "%s/schemas/ids/%d", ctx->schema_registry_url, schema_id);

    struct curl_buf buf = { malloc(4096), 0, 4096 };
    if (!buf.data) { curl_easy_cleanup(curl); return NULL; }
    buf.data[0] = '\0';

    curl_easy_setopt(curl, CURLOPT_URL, url);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, sr_write_cb);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &buf);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, 10L);
    if (ctx->sr_auth[0]) {
        curl_easy_setopt(curl, CURLOPT_HTTPAUTH, (long)CURLAUTH_BASIC);
        curl_easy_setopt(curl, CURLOPT_USERPWD, ctx->sr_auth);
    }

    CURLcode rc = curl_easy_perform(curl);
    long http_code = 0;
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
    curl_easy_cleanup(curl);

    if (rc != CURLE_OK || http_code != 200) {
        LOG_ERROR("kafka: schema fetch failed (id=%d http=%ld): %s",
                  schema_id, http_code, curl_easy_strerror(rc));
        free(buf.data);
        return NULL;
    }

    /* Response: {"schema":"<json-escaped avro schema>"} — the parser unescapes. */
    JVal *root = json_parse(ctx->arena, buf.data, buf.len);
    free(buf.data);
    if (!root || root->type != JV_OBJECT) return NULL;
    JVal *schema_val = json_get(root, "schema");
    if (!schema_val || schema_val->type != JV_STRING) return NULL;

    const char *s = json_str(schema_val, "");
    char *result = malloc(strlen(s) + 1);
    if (result) memcpy(result, s, strlen(s) + 1);
    return result;
}

/* Return the cached node for schema_id, fetching + parsing on a miss. */
static AvroSchemaNode *schema_cache_get(KafkaCtx *ctx, int32_t schema_id)
{
    AvroSchemaCache *cache = ctx->schema_cache;
    pthread_mutex_lock(&cache->mu);
    for (AvroSchemaNode *n = cache->head; n; n = n->next) {
        if (n->schema_id == schema_id) { pthread_mutex_unlock(&cache->mu); return n; }
    }
    pthread_mutex_unlock(&cache->mu);

    char *schema_json = sr_fetch_schema(ctx, schema_id);
    if (!schema_json) return NULL;

    AvroSchemaNode *node = arena_calloc(cache->arena, sizeof(AvroSchemaNode));
    node->schema_id   = schema_id;
    node->schema_json = arena_strdup(cache->arena, schema_json);
    free(schema_json);
    node->nfields = avro_parse_schema(cache->arena, node->schema_json, &node->fields);
    if (node->nfields < 0) {
        LOG_ERROR("kafka: failed to parse avro schema id=%d", schema_id);
        return NULL;
    }

    pthread_mutex_lock(&cache->mu);
    node->next  = cache->head;
    cache->head = node;
    pthread_mutex_unlock(&cache->mu);

    LOG_INFO("kafka: cached avro schema id=%d (%d fields)", schema_id, node->nfields);
    return node;
}

/* Build a ColBatch from a Confluent Avro message:
 *   [0x00][schema_id:4 BE][avro binary payload]
 * On any problem (bad magic, missing schema, decode error) we fall back to the
 * raw two-column batch so a bad message never crashes the read loop. */
static ColBatch *batch_from_avro(KafkaCtx *ctx, Arena *a,
                                 const uint8_t *msg, size_t mlen, int64_t offset_val)
{
    if (mlen < 5 || msg[0] != 0x00) {
        LOG_WARN("kafka: not a Confluent Avro message (bad magic byte)");
        return batch_from_raw(a, (const char *)msg, mlen, offset_val);
    }

    int32_t schema_id = ((int32_t)msg[1] << 24) | ((int32_t)msg[2] << 16) |
                        ((int32_t)msg[3] << 8)  |  (int32_t)msg[4];

    AvroSchemaNode *schema = schema_cache_get(ctx, schema_id);
    if (!schema) {
        LOG_ERROR("kafka: no schema for id=%d, falling back to raw", schema_id);
        return batch_from_raw(a, (const char *)msg, mlen, offset_val);
    }
    if (schema->nfields + 1 > MAX_COLS) {
        LOG_ERROR("kafka: avro schema id=%d has too many fields (%d > %d)",
                  schema_id, schema->nfields, MAX_COLS - 1);
        return batch_from_raw(a, (const char *)msg, mlen, offset_val);
    }

    char **values; uint8_t *nulls;
    if (avro_decode_record(a, schema->fields, schema->nfields,
                           msg + 5, mlen - 5, &values, &nulls) != 0)
        return batch_from_raw(a, (const char *)msg, mlen, offset_val);

    int ncols = schema->nfields + 1;
    Schema *sc = arena_calloc(a, sizeof(Schema));
    sc->ncols  = ncols;
    sc->cols   = arena_alloc(a, (size_t)ncols * sizeof(ColDef));
    sc->cols[0].name = "kafka_offset"; sc->cols[0].type = COL_INT64; sc->cols[0].nullable = false;
    for (int f = 0; f < schema->nfields; f++) {
        sc->cols[f + 1].name     = arena_strdup(a, schema->fields[f].name);
        sc->cols[f + 1].type     = COL_TEXT;   /* every value rendered as text */
        sc->cols[f + 1].nullable = true;
    }

    ColBatch *batch = arena_calloc(a, sizeof(ColBatch));
    batch->schema = sc; batch->ncols = ncols; batch->nrows = 1;

    int64_t *offs = arena_alloc(a, sizeof(int64_t));
    offs[0] = offset_val;
    batch->values[0]      = offs;
    batch->null_bitmap[0] = arena_calloc(a, 1);

    for (int f = 0; f < schema->nfields; f++) {
        char **col = arena_alloc(a, sizeof(char *));
        /* NULL contract: an Avro union-null field becomes DFO_NULL_SENTINEL
         * (not a NULL pointer), mirroring the JSON path; the null bitmap is
         * kept set so null-aware consumers also see it. */
        if (nulls[f] || !values[f])
            col[0] = arena_strdup(a, DFO_NULL_SENTINEL);
        else
            col[0] = values[f];
        batch->values[f + 1]      = col;
        batch->null_bitmap[f + 1] = arena_calloc(a, 1);
        if (nulls[f] || !values[f]) batch->null_bitmap[f + 1][0] = 0x01;  /* row 0 is null */
    }

    return batch;
}

/* ── CDC consumer thread ── */

/* Фоновый поток CDC: опрашивает топик, оборачивает каждое сообщение в CdcEvent
 * (op=INSERT, lsn=offset) и передаёт в cdc_handler, пока выставлен флаг
 * consumer_running. Каждое событие живёт в собственной арене. */
static void *consumer_thread_fn(void *arg)
{
    KafkaCtx *ctx = (KafkaCtx *)arg;
    unsigned since_commit = 0;   /* сколько событий с последнего коммита */

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

        /* Инкремент CDC: коммитим позицию в группу батчами (async, throttled),
         * чтобы после рестарта поток продолжил с обработанного места
         * (at-least-once). assign+commit — БЕЗ subscribe/join → без ребаланса. */
        if (ctx->explicit_group && ++since_commit >= 100) {
            rd_kafka_commit(ctx->rk, NULL, 1 /*async*/);
            since_commit = 0;
        }
    }

    /* Финальный коммит при остановке — не потерять последние обработанные. */
    if (ctx->explicit_group && since_commit > 0)
        rd_kafka_commit(ctx->rk, NULL, 0 /*sync*/);

    return NULL;
}

/* ── Connector functions ── */

/* Создаёт контекст коннектора: разбирает config_json, инициализирует кэш схем,
 * создаёт consumer и подписывается на топик. Возвращает ctx всегда (даже при
 * ошибке инициализации rk — причина пишется в last_err). */
static void *kafka_create(const char *config_json, Arena *arena)
{
    pthread_once(&kafka_curl_once, kafka_curl_global_init);

    KafkaCtx *ctx = arena_calloc(arena, sizeof(KafkaCtx));
    ctx->arena = arena;
    pthread_mutex_init(&ctx->buf_mu, NULL);
    atomic_store(&ctx->consumer_running, 0);

    /* Schema cache for avro (shares the host arena's lifetime). */
    ctx->schema_cache = arena_calloc(arena, sizeof(AvroSchemaCache));
    ctx->schema_cache->arena = arena;
    pthread_mutex_init(&ctx->schema_cache->mu, NULL);

    /* defaults */
    snprintf(ctx->brokers,     sizeof(ctx->brokers),     "localhost:9092");
    snprintf(ctx->group_id,    sizeof(ctx->group_id),    "dfo-consumer");
    snprintf(ctx->data_format, sizeof(ctx->data_format), "json");
    snprintf(ctx->addr_family, sizeof(ctx->addr_family), "v4");
    ctx->schema_registry_url[0] = '\0';
    ctx->sr_auth[0]             = '\0';

    if (config_json) {
        cfg_str(config_json, "\"brokers\"",     ctx->brokers,     sizeof(ctx->brokers));
        cfg_str(config_json, "\"group_id\"",    ctx->group_id,    sizeof(ctx->group_id));
        cfg_str(config_json, "\"topic\"",       ctx->topic_name,  sizeof(ctx->topic_name));
        cfg_str(config_json, "\"data_format\"", ctx->data_format, sizeof(ctx->data_format));
        cfg_str(config_json, "\"broker_address_family\"", ctx->addr_family, sizeof(ctx->addr_family));
        cfg_str(config_json, "\"schema_registry_url\"",  ctx->schema_registry_url, sizeof(ctx->schema_registry_url));
        cfg_str(config_json, "\"schema_registry_auth\"", ctx->sr_auth,             sizeof(ctx->sr_auth));
        /* security (optional — empty keeps plaintext) */
        cfg_str(config_json, "\"security_protocol\"", ctx->security_protocol, sizeof(ctx->security_protocol));
        cfg_str(config_json, "\"sasl_mechanism\"",    ctx->sasl_mechanism,    sizeof(ctx->sasl_mechanism));
        cfg_str(config_json, "\"sasl_username\"",     ctx->sasl_username,     sizeof(ctx->sasl_username));
        cfg_str(config_json, "\"sasl_password\"",     ctx->sasl_password,     sizeof(ctx->sasl_password));
        cfg_str(config_json, "\"sasl_kerberos_service_name\"", ctx->sasl_kerberos_service_name, sizeof(ctx->sasl_kerberos_service_name));
        cfg_str(config_json, "\"sasl_kerberos_principal\"",    ctx->sasl_kerberos_principal,    sizeof(ctx->sasl_kerberos_principal));
        cfg_str(config_json, "\"sasl_kerberos_keytab\"",       ctx->sasl_kerberos_keytab,       sizeof(ctx->sasl_kerberos_keytab));
        /* TLS/mTLS (для security_protocol SSL | SASL_SSL) */
        cfg_str(config_json, "\"ssl_ca_location\"",          ctx->ssl_ca_location,          sizeof(ctx->ssl_ca_location));
        cfg_str(config_json, "\"ssl_certificate_location\"", ctx->ssl_certificate_location, sizeof(ctx->ssl_certificate_location));
        cfg_str(config_json, "\"ssl_key_location\"",         ctx->ssl_key_location,         sizeof(ctx->ssl_key_location));
        cfg_str(config_json, "\"ssl_key_password\"",         ctx->ssl_key_password,         sizeof(ctx->ssl_key_password));
        cfg_str(config_json, "\"offset_reset\"",      ctx->offset_reset,      sizeof(ctx->offset_reset));
    }

    /* «Читать всё каждый раз»: если явный group_id не задан (осталось значение по
     * умолчанию "dfo-consumer"), делаем группу УНИКАЛЬНОЙ на каждый запуск. Свежая
     * группа не имеет закоммиченных оффсетов, поэтому auto.offset.reset=earliest
     * читает весь топик заново, а enable.auto.commit=false ничего не оставляет.
     * Ранее группа была стабильной (dfo-<topic>) → повторный прогон читал 0. Явный
     * group_id в конфиге шага сохраняет инкрементальную семантику (только новые). */
    /* Явный ли group_id? (не дефолт) → инкрементальная семантика: читаем с
     * закоммиченного оффсета и коммитим после чтения (паттерн simple consumer:
     * assign + committed/commit, БЕЗ subscribe → без ребаланса). Иначе —
     * уникальная эфемерная группа и «читать всё каждый раз». */
    ctx->explicit_group = (strcmp(ctx->group_id, "dfo-consumer") != 0);
    if (!ctx->explicit_group) {
        static _Atomic unsigned kafka_group_seq = 0;
        unsigned n = atomic_fetch_add(&kafka_group_seq, 1);
        const char *t = ctx->topic_name[0] ? ctx->topic_name : "topic";
        snprintf(ctx->group_id, sizeof(ctx->group_id),
                 "dfo-%s-%ld-%u", t, (long)time(NULL), n);
    }

    if (strcasecmp(ctx->data_format, "avro") == 0 && !ctx->schema_registry_url[0])
        LOG_ERROR("kafka: data_format=avro requires schema_registry_url "
                  "(messages will fall back to raw)");

    char errstr[512];
    ctx->rk = make_consumer(ctx, errstr, sizeof(errstr));
    if (!ctx->rk) {
        LOG_ERROR("kafka: failed to create consumer: %s", errstr);
        snprintf(ctx->last_err, sizeof(ctx->last_err), "%s", errstr[0] ? errstr : "consumer init failed");
        return ctx;
    }

    /* НЕ подписываемся на consumer-группу при создании: разовое чтение
     * (read_batch/describe) идёт через assign партиций и в группу не вступает,
     * поэтому подключение НЕ вызывает ребаланс чужих консюмеров на боевом
     * кластере. Групповую подписку делает только kafka_cdc_start (непрерывный
     * поток). tplist готовим здесь для этого случая. */
    ctx->tplist = rd_kafka_topic_partition_list_new(1);
    rd_kafka_topic_partition_list_add(ctx->tplist, ctx->topic_name,
                                      RD_KAFKA_PARTITION_UA);

    return ctx;
}

/* Останавливает CDC-поток, закрывает consumer и освобождает только собственные
 * ресурсы плагина (память ctx/арены принадлежит хосту — см. примечание ниже). */
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

    if (ctx->schema_cache)
        pthread_mutex_destroy(&ctx->schema_cache->mu);
    pthread_mutex_destroy(&ctx->buf_mu);
    /* NB: ctx (and ctx->arena) are owned by the HOST — ctx was allocated with
     * arena_calloc(host_arena, ...). Do NOT arena_destroy() the host's arena or
     * free() arena memory here (that corrupted the host heap and crashed the
     * gateway). Mirror pg_connector: release only the plugin's own resources. */
}

/* Единственная сущность коннектора — сконфигурированный топик (type="stream"). */
static int kafka_list_entities(void *vctx, Arena *a, DfoEntityList *out)
{
    KafkaCtx *ctx = (KafkaCtx *)vctx;

    out->items = arena_calloc(a, sizeof(DfoEntity));
    out->items[0].entity = arena_strdup(a, ctx->topic_name);
    out->items[0].type   = "stream";
    out->count = 1;
    return 0;
}

/* Разовое чтение: назначаем все партиции топика ВРУЧНУЮ (assign), НЕ вступая в
 * consumer-группу. Критично для боевого кластера: subscribe в общую группу
 * триггерит ребаланс ЧУЖИХ консюмеров банка при каждом подключении. Assign —
 * без координатора, без rebalance: коннектор читает партиции напрямую и не
 * влияет на другие приложения (какой бы group_id ни задали). Идемпотентно
 * (ctx->assigned). earliest→BEGINNING, latest→END. */
static int kafka_assign_all(KafkaCtx *ctx, const char *topic)
{
    if (ctx->assigned) return 0;
    if (!topic || !topic[0]) topic = ctx->topic_name;
    rd_kafka_topic_t *rkt = rd_kafka_topic_new(ctx->rk, topic, NULL);
    const struct rd_kafka_metadata *meta = NULL;
    rd_kafka_resp_err_t merr = rkt
        ? rd_kafka_metadata(ctx->rk, 0, rkt, &meta, 5000) : RD_KAFKA_RESP_ERR__FAIL;
    if (merr != RD_KAFKA_RESP_ERR_NO_ERROR || !meta) {
        snprintf(ctx->last_err, sizeof(ctx->last_err),
                 "kafka: metadata for topic '%s' failed: %s", topic, rd_kafka_err2str(merr));
        LOG_ERROR("%s", ctx->last_err);
        if (rkt) rd_kafka_topic_destroy(rkt);
        return -1;
    }
    int64_t start_off = (strcasecmp(ctx->offset_reset, "latest") == 0)
                      ? RD_KAFKA_OFFSET_END : RD_KAFKA_OFFSET_BEGINNING;
    rd_kafka_topic_partition_list_t *parts = rd_kafka_topic_partition_list_new(4);
    for (int i = 0; i < meta->topic_cnt; i++) {
        if (strcmp(meta->topics[i].topic, topic) != 0) continue;
        for (int p = 0; p < meta->topics[i].partition_cnt; p++) {
            rd_kafka_topic_partition_t *tp =
                rd_kafka_topic_partition_list_add(parts, topic, meta->topics[i].partitions[p].id);
            tp->offset = start_off;
        }
    }
    rd_kafka_metadata_destroy(meta);
    rd_kafka_topic_destroy(rkt);

    /* Явный group_id → инкремент: РАЗРЕШАЕМ оффсеты в конкретные числа ДО
     * assign через rd_kafka_committed (OffsetFetch, БЕЗ join/ребаланса; заодно
     * прогревает координатор). Валидный коммит → стартуем оттуда; нет коммита
     * или ошибка → start_off (offset_reset). Так assign получает готовые
     * позиции и чтение НЕ гонится с ленивым OffsetFetch координатора (под
     * GSSAPI это давало 0 строк на первом прогоне). Таймаут щедрый — GSSAPI-
     * хендшейк к координатору небыстрый. */
    if (ctx->explicit_group) {
        /* С assign (без subscribe) координатор группы обнаруживается лениво,
         * поэтому первый OffsetFetch может вернуть NOT_COORDINATOR — повторяем,
         * дав librdkafka обновить координатора между попытками. */
        rd_kafka_resp_err_t ce = RD_KAFKA_RESP_ERR__TIMED_OUT;
        for (int attempt = 0; attempt < 4; attempt++) {
            ce = rd_kafka_committed(ctx->rk, parts, 7000);
            if (ce == RD_KAFKA_RESP_ERR_NO_ERROR) break;
            rd_kafka_poll(ctx->rk, 400);
        }
        for (int i = 0; i < parts->cnt; i++)
            if (ce != RD_KAFKA_RESP_ERR_NO_ERROR || parts->elems[i].offset < 0)
                parts->elems[i].offset = start_off;
    }

    rd_kafka_resp_err_t aerr = rd_kafka_assign(ctx->rk, parts);
    rd_kafka_topic_partition_list_destroy(parts);
    if (aerr != RD_KAFKA_RESP_ERR_NO_ERROR) {
        snprintf(ctx->last_err, sizeof(ctx->last_err),
                 "kafka: assign failed: %s", rd_kafka_err2str(aerr));
        LOG_ERROR("%s", ctx->last_err);
        return -1;
    }
    ctx->assigned = true;
    LOG_INFO("kafka: assigned partitions of topic=%s (%s, без consumer-группы/ребаланса)",
             topic, ctx->explicit_group ? "инкремент по committed offset group_id"
                                        : (start_off == RD_KAFKA_OFFSET_END ? "latest" : "earliest"));
    return 0;
}

/* Выводит схему топика: сэмплирует до 10 сообщений и строит Schema по формату
 * (CSV-заголовок / Avro-схема / ключи JSON); при неудаче — fallback из 2 колонок. */
static int kafka_describe(void *vctx, Arena *a, const char *entity, Schema **out)
{
    KafkaCtx *ctx = (KafkaCtx *)vctx;
    if (!ctx->rk) return -1;

    /* Схему тоже сэмплируем через assign, не вступая в группу (см. выше). */
    if (kafka_assign_all(ctx, (entity && entity[0]) ? entity : ctx->topic_name) != 0)
        return -1;

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
        } else if (strcasecmp(ctx->data_format, "avro") == 0) {
            /* Avro: decode one message via its Registry schema → schema */
            ColBatch *b = batch_from_avro(ctx, a, (const uint8_t *)payload, plen, offset);
            if (b) schema = b->schema;
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

/* Preflight check before polling: confirms the cluster is reachable and that the
 * topic actually exists. Without this a typo'd topic or a wholly-down cluster
 * just yields 0 rows and a green run. Returns 0 when it's safe to consume; <0 and
 * fills ctx->last_err otherwise:
 *   - metadata RPC fails / all brokers down  → broker connectivity error
 *   - topic absent from metadata, or its .err is UNKNOWN_TOPIC_OR_PART → topic
 *     does not exist (distinct from an existing-but-empty topic, which is OK). */
static int kafka_preflight(KafkaCtx *ctx, const char *topic)
{
    /* The metadata round-trip below (with its own timeout) is the authority on
     * reachability — we do NOT short-circuit on the error_cb's all_brokers_down
     * latch here, because librdkafka emits ALL_BROKERS_DOWN transiently during
     * normal bootstrap before the first successful connect. Trusting that latch
     * pre-metadata falsely fails a healthy cluster on the first run. */

    /* Topic-scoped metadata: cheaper than all_topics and gives us the topic .err
     * the broker reports for THIS topic. */
    rd_kafka_topic_t *rkt = rd_kafka_topic_new(ctx->rk, topic, NULL);
    if (!rkt) {
        snprintf(ctx->last_err, sizeof(ctx->last_err),
                 "kafka: failed to create topic handle for '%s'", topic);
        LOG_ERROR("%s", ctx->last_err);
        return -1;
    }

    const struct rd_kafka_metadata *meta = NULL;
    rd_kafka_resp_err_t err =
        rd_kafka_metadata(ctx->rk, 0 /*only this topic*/, rkt, &meta, 5000 /*ms*/);

    if (err != RD_KAFKA_RESP_ERR_NO_ERROR || !meta) {
        snprintf(ctx->last_err, sizeof(ctx->last_err),
                 "kafka: cannot reach brokers (%s): %s",
                 ctx->brokers, rd_kafka_err2str(err));
        LOG_ERROR("%s", ctx->last_err);
        rd_kafka_topic_destroy(rkt);
        return -1;
    }

    /* Metadata round-trip succeeded → brokers ARE reachable. Clear any transient
     * ALL_BROKERS_DOWN the error_cb latched during bootstrap, so a healthy
     * cluster is not falsely reported down. A genuine outage still fails above
     * (rd_kafka_metadata returns an error / times out). */
    atomic_store(&ctx->all_brokers_down, 0);

    /* Locate our topic in the response and inspect its broker-reported error. */
    int found = 0, rc = 0;
    for (int i = 0; i < meta->topic_cnt; i++) {
        if (strcmp(meta->topics[i].topic, topic) != 0) continue;
        found = 1;
        rd_kafka_resp_err_t terr = meta->topics[i].err;
        if (terr == RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART ||
            terr == RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_ID ||
            meta->topics[i].partition_cnt == 0) {
            snprintf(ctx->last_err, sizeof(ctx->last_err),
                     "kafka: topic '%s' does not exist (%s)",
                     topic, rd_kafka_err2str(terr));
            LOG_ERROR("%s", ctx->last_err);
            rc = -1;
        } else if (terr != RD_KAFKA_RESP_ERR_NO_ERROR) {
            snprintf(ctx->last_err, sizeof(ctx->last_err),
                     "kafka: topic '%s' metadata error: %s",
                     topic, rd_kafka_err2str(terr));
            LOG_ERROR("%s", ctx->last_err);
            rc = -1;
        }
        break;
    }
    if (!found) {
        snprintf(ctx->last_err, sizeof(ctx->last_err),
                 "kafka: topic '%s' does not exist (not in cluster metadata)", topic);
        LOG_ERROR("%s", ctx->last_err);
        rc = -1;
    }

    rd_kafka_metadata_destroy(meta);
    rd_kafka_topic_destroy(rkt);
    return rc;
}

/* Читает до `limit` сообщений, разбирает каждое по формату и сливает в один
 * ColBatch (схема — от первого сообщения). Курсор req->cursor = последний offset.
 * Возвращает 0 и *out=NULL, если сообщений пока нет. */
static int kafka_read_batch(void *vctx, Arena *a, DfoReadReq *req,
                            const char *entity, ColBatch **out)
{
    KafkaCtx *ctx = (KafkaCtx *)vctx;
    if (!ctx->rk) return -1;

    /* S18/S19: detect a down cluster or a non-existent (e.g. mistyped) topic up
     * front so the run fails (status!=0) instead of reporting a green 0 rows. An
     * existing-but-empty topic passes this and drains to 0 rows as before. */
    const char *check_topic = (entity && entity[0]) ? entity : ctx->topic_name;
    if (check_topic && check_topic[0]) {
        if (kafka_preflight(ctx, check_topic) != 0) {
            *out = NULL;
            return -1;
        }
    }

    /* Разовое чтение топика: НЕ подписываемся на consumer-группу, а вручную
     * назначаем все партиции топика (assign). Группа требует join+назначения
     * (~3с даже на тёплом брокере, а на холодном старте/медленном кластере не
     * укладывается в дедлайн → «топик не пришёл» на первом же «Показать
     * данные»). Assign обходит координацию: первый poll сразу тянет данные.
     * Назначаем один раз на инстанс; при пагинации позиция консюмера сама
     * продвигается между вызовами read_batch. earliest → OFFSET_BEGINNING
     * («читать всё каждый раз»), latest → OFFSET_END (только новые). */
    if (kafka_assign_all(ctx, check_topic) != 0) { *out = NULL; return -1; }

    int64_t limit = (req->limit > 0 && req->limit < BATCH_SIZE) ? req->limit : BATCH_SIZE;
    int64_t last_offset = -1;

    /* We collect individual batches (1 msg each) and merge into one */
    ColBatch **msgs     = arena_alloc(a, limit * sizeof(ColBatch *));
    int        msg_count = 0;

    /* Measure REAL wall-clock time, not a poll counter: rd_kafka_consumer_poll()
     * returns immediately while the consumer-group is still being assigned its
     * partitions (cold-start rebalance), so a "+100ms per poll" counter races to
     * the deadline in milliseconds and the read returns 0 rows before any message
     * arrives. We give the coordinator up to `deadline_ms` of real time, but once
     * we've received at least one message we stop as soon as the topic drains. */
    const int64_t deadline_ms = 10000;
    const int64_t brokers_down_grace_ms = 6000;  /* сколько ждём переподключения */
    struct timespec t0; clock_gettime(CLOCK_MONOTONIC, &t0);
    bool got_any = false;
    int64_t brokers_down_since = -1;             /* -1 = кластер не помечен down */

    while (msg_count < (int)limit) {
        struct timespec tn; clock_gettime(CLOCK_MONOTONIC, &tn);
        int64_t elapsed_ms = (tn.tv_sec - t0.tv_sec) * 1000
                           + (tn.tv_nsec - t0.tv_nsec) / 1000000;
        if (elapsed_ms >= deadline_ms) break;

        /* Транзиентный реконнект: librdkafka переподключается сам за секунды, а
         * preflight в начале read_batch уже подтвердил доступность кластера.
         * Поэтому НЕ падаем на первом же ALL_BROKERS_DOWN (это ронял чтение при
         * кратковременном обрыве — «отваливается»); даём grace на восстановление и
         * жёстко падаем, только если брокеры недоступны непрерывно дольше grace и
         * ни одного сообщения так и не пришло. */
        if (!got_any && atomic_load(&ctx->all_brokers_down)) {
            if (brokers_down_since < 0) brokers_down_since = elapsed_ms;
            else if (elapsed_ms - brokers_down_since >= brokers_down_grace_ms) {
                if (!ctx->last_err[0])
                    snprintf(ctx->last_err, sizeof(ctx->last_err),
                             "all brokers down (%s)", ctx->brokers);
                LOG_ERROR("kafka: %s", ctx->last_err);
                *out = NULL;
                return -1;
            }
        } else {
            brokers_down_since = -1;  /* пришло сообщение или флаг снят — сбрасываем */
        }

        rd_kafka_message_t *msg = rd_kafka_consumer_poll(ctx->rk, 500 /*ms*/);
        if (!msg) { if (got_any) break; else continue; }  /* drained after first msg */
        if (msg->err) {
            if (msg->err != RD_KAFKA_RESP_ERR__PARTITION_EOF)
                LOG_WARN("kafka: poll error: %s", rd_kafka_message_errstr(msg));
            rd_kafka_message_destroy(msg);
            if (got_any) break;   /* EOF after reading messages → done */
            continue;
        }
        got_any = true;

        const char *payload = (const char *)msg->payload;
        size_t      plen    = msg->len;
        last_offset         = (int64_t)msg->offset;

        /* Guard against NULL-value messages (Kafka tombstones / log compaction):
         * a successful message can carry payload==NULL with len==0. The format
         * parsers dereference the payload (memchr / msg[0] / json_parse), so feed
         * them an empty buffer instead of NULL to avoid a NULL-deref crash. */
        const char *safe_payload = payload ? payload : "";
        size_t      safe_plen    = payload ? plen : 0;

        ColBatch *b;
        if (strcasecmp(ctx->data_format, "avro") == 0)
            b = batch_from_avro(ctx, a, (const uint8_t *)safe_payload, safe_plen, last_offset);
        else if (strcasecmp(ctx->data_format, "json") == 0)
            b = batch_from_json(a, safe_payload, safe_plen, last_offset);
        else
            b = batch_from_raw(a, safe_payload, safe_plen, last_offset);

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

    /* Сшиваем per-message батчи (по 1 строке) в колоночные массивы: строка r
     * колонки c берётся из msgs[r]->values[c][0]. Раскладка значения зависит от
     * типа колонки (INT64/BOOL — native int64, DOUBLE — native double, иначе —
     * char*), чтобы совпадать с тем, как батч заполнял batch_from_* . */
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

    /* Инкремент: коммитим позицию в группу (OffsetCommit, синхронно). С assign
     * (не subscribe) это коммит «простого консюмера» — координатор сохраняет
     * оффсет, но rebalance-протокол НЕ запускается, чужие консюмеры не
     * трогаются. Следующий прогон с тем же group_id продолжит с этой точки. */
    if (ctx->explicit_group && msg_count > 0) {
        /* NOT_COORDINATOR транзиентен при ленивом обнаружении координатора
         * (assign без subscribe) — повторяем с обновлением между попытками. */
        rd_kafka_resp_err_t cerr = RD_KAFKA_RESP_ERR__TIMED_OUT;
        for (int attempt = 0; attempt < 4; attempt++) {
            cerr = rd_kafka_commit(ctx->rk, NULL, 0 /*sync*/);
            if (cerr == RD_KAFKA_RESP_ERR_NO_ERROR || cerr == RD_KAFKA_RESP_ERR__NO_OFFSET) break;
            rd_kafka_poll(ctx->rk, 400);
        }
        if (cerr != RD_KAFKA_RESP_ERR_NO_ERROR && cerr != RD_KAFKA_RESP_ERR__NO_OFFSET)
            LOG_WARN("kafka: commit offset failed: %s", rd_kafka_err2str(cerr));
    }

    /* Курсор для пагинации — последний прочитанный offset ("offset:<N>"). Пишем
     * поверх const-поля req->cursor через каст: вызывающий владеет структурой и
     * читает обновлённый курсор после возврата. */
    if (last_offset >= 0)
        *(const char **)&req->cursor = arena_sprintf(a, "offset:%lld", (long long)last_offset);

    return 0;
}

/* Запускает фоновый CDC-поток (consumer_thread_fn), доставляющий каждое
 * сообщение как CdcEvent(INSERT) в handler. Идемпотентно: повторный вызов при
 * уже работающем потоке — no-op (0). */
static int kafka_cdc_start(void *vctx, DfoCdcHandler handler, void *userdata)
{
    KafkaCtx *ctx = (KafkaCtx *)vctx;
    if (!ctx->rk) return -1;
    if (atomic_load(&ctx->consumer_running)) return 0; /* already running */

    /* CDC тоже через assign, а НЕ subscribe: непрерывное чтение назначенных
     * партиций без вступления в consumer-группу → без rebalance чужих
     * консюмеров на боевом кластере. Инкремент (resume после рестарта)
     * обеспечивают committed offset при assign + периодический commit в
     * потоке (см. consumer_thread_fn) при явном group_id. */
    if (kafka_assign_all(ctx, ctx->topic_name) != 0)
        return -1;   /* last_err уже выставлен */
    LOG_INFO("kafka: CDC assigned topic=%s (group=%s, без subscribe/ребаланса)",
             ctx->topic_name, ctx->group_id);

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

/* Сбрасывает флаг consumer_running и дожидается завершения CDC-потока. */
static int kafka_cdc_stop(void *vctx)
{
    KafkaCtx *ctx = (KafkaCtx *)vctx;
    if (!atomic_load(&ctx->consumer_running)) return 0;

    atomic_store(&ctx->consumer_running, 0);
    pthread_join(ctx->consumer_thread, NULL);
    LOG_INFO("kafka: CDC stopped");
    return 0;
}

/* Проверка связи: запрашивает метаданные брокеров (таймаут 3с). 0 — ОК. */
static int kafka_ping(void *vctx)
{
    KafkaCtx *ctx = (KafkaCtx *)vctx;
    if (!ctx->rk) return -1;

    const struct rd_kafka_metadata *meta = NULL;
    rd_kafka_resp_err_t err = rd_kafka_metadata(ctx->rk, 1 /*all_topics*/,
                                                 NULL, &meta, 3000 /*ms*/);
    if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
        LOG_WARN("kafka: ping failed: %s", rd_kafka_err2str(err));
        snprintf(ctx->last_err, sizeof(ctx->last_err), "%s", rd_kafka_err2str(err));
        return -1;
    }
    rd_kafka_metadata_destroy(meta);
    LOG_INFO("kafka: ping OK, brokers=%s", ctx->brokers);
    return 0;
}

/* ── last_error(): human-readable reason of the last failure (ABI v3) ── */
static const char *kafka_last_error(void *vctx)
{
    KafkaCtx *ctx = (KafkaCtx *)vctx;
    return (ctx && ctx->last_err[0]) ? ctx->last_err : NULL;
}

/* ── Sink: produce one JSON message per row to a topic ──
 * Topic = `entity` if given, else the configured "topic". A fresh producer is
 * created per call, all rows are produced, then flushed. mode is ignored (Kafka
 * is an append-only log). Returns rows produced or <0 on error.
 *
 * NOTE: compiles and follows the standard rdkafka producer flow, but not
 * exercised against a live broker in this environment. */
/* Дописывает s в растущий буфер *buf с JSON-экранированием (кавычки, слэши,
 * управляющие символы < 0x20 → \uXXXX); при нехватке места *buf реаллоцируется. */
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
    kafka_apply_security(conf, ctx, errstr, sizeof(errstr));
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
            /* Values are always char* text (text-always model); the schema type
             * is metadata only. Reading the cell as a native int64/double (the old
             * switch) reinterpreted the char* POINTER as the value and shipped a
             * heap address / garbage double; numbers were also wrapped in quotes,
             * emitting a JSON string instead of a number (K6). Now: typed numeric/
             * bool columns emit the text UNQUOTED as a real JSON number/bool;
             * everything else is a quoted, escaped JSON string. */
            const char *v = ((char**)batch->values[c])[r];
            if (!v || strcmp(v, DFO_NULL_SENTINEL)==0) {
                if (len+4>cap){cap*=2;msg=realloc(msg,cap);} memcpy(msg+len,"null",4); len+=4; continue;
            }
            ColType _ct = schema->cols[c].type;
            bool bare = ((_ct==COL_INT64||_ct==COL_DOUBLE) && kafka_is_json_num(v)) ||
                        (_ct==COL_BOOL && (!strcmp(v,"true")||!strcmp(v,"false")));
            if (bare) {
                size_t vl=strlen(v); if(len+vl>cap){while(len+vl>cap)cap*=2;msg=realloc(msg,cap);}
                memcpy(msg+len,v,vl); len+=vl;
            } else {
                if (len+1>cap){cap*=2;msg=realloc(msg,cap);} msg[len++]='"';
                kafka_json_escape(&msg,&len,&cap,v);
                if (len+1>cap){cap*=2;msg=realloc(msg,cap);} msg[len++]='"';
            }
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
    .version       = "1.1.0",
    .description   = "Kafka connector (JSON/CSV/Avro+SchemaRegistry)",
    .create        = kafka_create,
    .destroy       = kafka_destroy,
    .list_entities = kafka_list_entities,
    .describe      = kafka_describe,
    .read_batch    = kafka_read_batch,
    .cdc_start     = kafka_cdc_start,
    .cdc_stop      = kafka_cdc_stop,
    .ping          = kafka_ping,
    .write_batch   = kafka_write_batch,
    .last_error    = kafka_last_error,
};
